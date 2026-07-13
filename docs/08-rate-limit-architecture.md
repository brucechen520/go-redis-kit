# Stage 8：限流與異步架構（Rate Limiting & Async Architecture）

> 模組：`github.com/twteam/go-redis-kit`　Go 1.26.1　Client：`github.com/redis/go-redis/v9` + Kafka（`segmentio/kafka-go` 或 `confluent-kafka-go`）
>
> 本章把 docs/06 §3 的限流演算法「拉高到架構層」：Redis 與 Kafka 在一條完整請求路徑上各自的定位、讀寫兩條路的生命週期、以及穩定性四件套的分工。演算法細節（token bucket / 漏桶 Lua）請回 docs/06 §3；分散式鎖與冪等請回 docs/05 §5。

---

## 目錄

1. [本章目標](#1-本章目標)
2. [完整架構圖（含 infra 裝置）](#2-完整架構圖含-infra-裝置)
3. [Request 生命週期：讀寫兩條路](#3-request-生命週期讀寫兩條路)
4. [寫請求經過 Redis 幾次](#4-寫請求經過-redis-幾次)
5. [穩定性四件套：誰實作、靠不靠 Redis](#5-穩定性四件套誰實作靠不靠-redis)
6. [Redis 的套 vs Kafka 的套](#6-redis-的套-vs-kafka-的套)
7. [限流 vs MQ 削峰：同步拒絕 vs 異步緩衝](#7-限流-vs-mq-削峰同步拒絕-vs-異步緩衝)
8. [infra 分層與高可用](#8-infra-分層與高可用)
9. [坑](#9-坑)

---

## 1. 本章目標

docs/06 §3 教「限流算法怎麼寫」，但實務會被問的是**架構問題**：

- 限流放在哪一層？為什麼越外層越好？
- Redis 和 Kafka 在同一條請求路徑上怎麼分工？
- 讀請求和寫請求的生命週期為什麼不一樣？
- 「寫請求走 Kafka」是不是就不碰 Redis 了？（不是）
- 穩定性四件套（限流/熔斷/降級/快取）哪些靠 Redis、哪些是應用層？
- 漏桶在真實系統裡是什麼？（Kafka 削峰的實體實現）

學完能畫出完整架構圖、講清楚一個請求從進來到落地碰過哪些裝置、每個裝置的職責。

---

## 2. 完整架構圖（含 infra 裝置）

```
                              ┌─────────┐
                     Client ─▶│ DNS/CDN │
                              └────┬────┘
                                   ▼
                          ┌─────────────────┐
                          │  L4 LB          │  雲 LB / LVS（四層，分流）
                          └────────┬────────┘
                                   ▼
                          ┌────────────────────┐
                          │ L7 Gateway/Ingress │ nginx / Envoy / Kong
                          │  ① 限流(第一道) ────┼──────┐ 讀+寫都要過
                          └────────┬───────────┘      │
                                   ▼                  │  查/扣桶
                          ┌─────────────────┐         │
                          │ API Service     │         │
                          │ (N pods, 無狀態) │         ▼
                          └───┬─────────┬───┘   ╔══════════════════╗
              讀請求(同步)     │         │  寫請求 ║  Redis Cluster   ║ ← 共享狀態
                 ┌────────────┘         └──────╫▶ · 限流桶(全域/user)║   (Sentinel /
                 ▼                              ║ · 快取 cache      ║    Cluster HA)
          ╔═════════════╗ hit→回                ║ · 冪等去重 SETNX  ║
          ║ Redis 快取   ║────────────┐         ║ · 分散式鎖        ║
          ╚══════╤══════╝            │         ╚═════╤═══════╤═════╝
                 │ miss              │               │       │
                 ▼                   │        寫請求 ▼(限流過後)│
          ┌─────────────┐           │        ┌──────────────┐│
          │ 熔斷/降級     │ 壞→降級   │        │ Producer     ││
          │ (應用層邏輯)  │           │        │ 寫 Kafka→回202││
          └──────┬──────┘           │        └──────┬───────┘│
                 ▼                   │               ▼        │
          ╔═════════════╗           │        ╔══════════════╗│
          ║  DB Primary ║◀──回填快取─┘        ║ Kafka Cluster║│ N brokers
          ║  + Replica  ║                    ║ (KRaft/ZK)   ║│ (topic 削峰=漏桶)
          ╚═════════════╝                    ╚══════╤═══════╝│
                 ▲                                   ▼        │
                 │  ①冪等去重(SETNX)②落DB     ┌──────────────┐│
                 │  ③刪快取(cache aside)      │ Consumer     ││
                 └────────────────────────────┤ Group        │┘
                    寫路徑也回頭碰 Redis 3 次    │ (N 實例,勻速) │
                                              └──────────────┘
```

**三條主軸**：
- **Gateway 限流**（Redis）：擋量，同步拒絕，全站入口，讀寫都過。
- **讀路徑**（Redis 快取）：同步、可拒、秒級即時。
- **寫路徑**（Kafka 削峰）：異步、不可丟，但前後仍碰 Redis（見第 4 節）。

---

## 3. Request 生命週期：讀寫兩條路

讀和寫的語意不同（可拒 vs 不可丟、同步 vs 異步），生命週期分兩條路。

### 3.1 讀請求（同步、可拒）── Redis 主場

```
1. Gateway 限流: 扣全域桶 → 扣 per-user 桶 → 任一超量 → 回 429（結束）
2. 過閘 → 查 Redis 快取
     hit  → 直接回（不打 DB）✓ 結束
     miss → 往下
3. 熔斷檢查: 下游最近大量失敗？ → 是 → 降級回舊值/預設（結束）
4. 否 → 查 DB → 回填快取(TTL 抖動) → 回應
```

**同步返回**：client 一路等到結果（毫秒級）。超量當場被拒，由 client 退避重試。

### 3.2 寫請求（不可丟、可異步）── Kafka 主場

```
1. Gateway 限流: 擋惡意爆量 → 過閘
2. Producer: 寫進 Kafka topic → 立刻回 202 Accepted（client 不等處理完）
3. Consumer: 勻速拉取(這就是漏桶) → 冪等處理 → 落 DB
     處理失敗 → 進 retry topic → 幾次還敗 → 進死信隊列(DLQ)
```

**異步**：client 收 202 就走，實際處理慢慢做。尖峰被 Kafka 吸收，consumer 恆定速率消費。

> **對照工單系統**：建單 / 狀態變更走 Kafka（不可丟）；查詢走 Redis 快取（可拒）。SLA timer 掃描也可以 consumer 勻速跑，避免瞬間掃全表打爆 DB。

---

## 4. 寫請求經過 Redis 幾次

**「寫請求走 Kafka 就不碰 Redis」是常見誤解——錯。** 寫路徑至少碰 Redis 3 次：

| 階段 | 碰 Redis 做什麼 | 對應章節 |
|---|---|---|
| **① Gateway 限流** | 扣全域桶 + per-user 桶（token bucket）| docs/06 §3 |
| **② Consumer 冪等去重** | `SETNX dedup:<eventID>` 防重複消費（或用 DB 唯一約束）| docs/05 §5 |
| **③ 落 DB 後刪快取** | `DEL cache:<key>`（cache aside 寫路徑：先更 DB 後刪快取）| docs/06 §2.4 |
| （選配）**分散式鎖** | 同一資源互斥處理時 `SET NX` + fencing token | docs/05 §5 |

**所以寫請求**：
1. 入口先被 Redis **限流**擋一次
2. Consumer 消費時用 Redis **去重**防重複
3. 更完 DB 用 Redis **刪快取**保一致性（否則讀路徑會讀到舊值）

Kafka 只負責**中間那段削峰緩衝**；前後的守門、去重、快取一致性都還是 Redis 的活。兩者**不是二選一**，是**同一條寫路徑上的分工**。

---

## 5. 穩定性四件套：誰實作、靠不靠 Redis

先分清兩個容易混的「四件套」。

**(a) 穩定性四件套**是**概念**（限流 / 熔斷 / 降級 / 快取），**不是 Redis 專屬**：

| 機制 | 擋什麼 | 一句話 | 誰實作 | 靠 Redis？ |
|---|---|---|---|---|
| **限流** rate limit | 擋「量」 | 超量直接拒（429）| Redis token bucket | ✅ |
| **快取** cache | 減「負載」 | 先擋掉重複讀，不打後端 | Redis cache aside | ✅ |
| **熔斷** circuit break | 擋「故障傳播」| 下游連續失敗就快速失敗 | 應用層（gobreaker / hystrix-go）| ❌ 純記憶體邏輯 |
| **降級** fallback | 給「替代品」| 壞了回舊值/預設/友善錯誤 | 應用層 | ❌ |

→ Redis 只扛其中**兩件**（限流 + 快取）；熔斷 / 降級是**應用層**的事，不進 Redis。

**四者串起來看**（一個讀請求的完整防線）：

```
請求 →[限流] 超量？→ 拒 429
       ↓ 過
     [快取] 命中？→ 直接回（不打 DB）
       ↓ miss
     [熔斷] 下游正常？→ 否 →[降級] 回舊值/預設
       ↓ 是
     打 DB → 回填快取 → 回應
```

- **限流**在最前面：先把超量擋在門外，後面三個才不會被沖垮。
- **快取**減少真正打到後端的量。
- **熔斷**在下游已出問題時止血，避免「越慢越重試、越重試越慢」的死亡螺旋。
- **降級**是熔斷 / 失敗後的兜底，保證有東西可回，不讓使用者看到 500。

---

## 6. Redis 的套 vs Kafka 的套

**(b) Redis 自己的常見用途**（Redis 的「套」）：

| 用途 | 對應本 repo |
|---|---|
| 快取 cache aside | docs/06 §2 |
| 限流 token bucket | docs/06 §3 |
| 分散式鎖 + fencing | docs/05 §5 |
| 計數 / 去重（INCR / Set / HLL / Bitmap）| docs/01 |
| 延遲隊列 / 排行榜（ZSet）| labs demo 15-16 |
| Stream（輕量 MQ）| 待補 |

**Kafka 的套**（patterns）：

| 套 | 做什麼 | 對應概念 |
|---|---|---|
| **削峰填谷** peak shaving | 突發塞 queue、consumer 勻速消費 | **= 漏桶落地** |
| **異步解耦** decouple | producer/consumer 分離，producer 立刻回 | 202 Accepted |
| **消費端限速** consumer throttle | consumer 端用 `rate.Limiter` 控拉取速度 | 漏桶「勻速流出」由誰保證 |
| **事件驅動** event-driven / CDC | DB 變更 → 發事件 → 下游反應 | 知識庫增量 re-index |
| **重試 + 死信** retry topic + DLQ | 失敗進重試 topic，多次敗進 DLQ 人工查 | 保證「不可丟」|
| **冪等消費** idempotent consume | 消費者去重（唯一約束 / dedup key），防重複處理 | 呼應 docs/05 冪等 |
| **分區並行** partition parallelism | 同 key 進同 partition，跨 partition 並行 | 吞吐 vs 順序取捨 |
| **反壓** backpressure | 監控 consumer lag，滿了讓 producer 慢下來 | 保護 consumer |

**漏桶三要素在 Kafka 的對應**：
- 漏桶的「桶」= Kafka topic（buffer）
- 漏桶的「勻速流出」= consumer 消費速率（用 `rate.Limiter` 或 poll 批量大小控制）
- 漏桶的「溢出」= topic retention 到期 / 磁碟滿（要設好 retention + 監控 lag）

---

## 7. 限流 vs MQ 削峰：同步拒絕 vs 異步緩衝

兩者常被拿來比，但**語意根本不同**，核心分野是**同步/異步**與**丟不丟**：

| | token bucket（限流） | Kafka / MQ（削峰填谷）|
|---|---|---|
| 超量怎麼辦 | **拒絕**（丟棄 / 429） | **收下排隊**，consumer 晚點勻速處理 |
| 同步/異步 | 同步（當場回：過或拒） | 異步（先收下，client 不等結果）|
| 資料留不留 | 不留，拒了就沒了 | 留，一定會被處理 |
| 本質 | 閘門 gatekeeper | 緩衝區 buffer |
| 對應演算法 | token bucket | **其實是漏桶**（consumer 勻速流出，docs/06 §3.4）|

**選型**：
- 請求要**即時回應**、且**可以拒**（讀查詢、API 呼叫）→ **rate limit**，拒了 client 自己重試。
- 任務**不可丟**、**可延遲**、**可異步**（下單、發信、事件處理）→ **MQ 削峰**，必須做只是晚點做。

**常一起用**：gateway 先 rate limit 擋惡意爆量 → 合法寫入任務進 Kafka 削峰 → consumer 勻速落 DB。前者擋壞人，後者保護下游不被合法尖峰打垮。

> 一句話：**rate limit 問「現在能不能做」（不能就拒）；MQ 問「這件事一定要做、何時做」（先收下、慢慢做）。**

---

## 8. infra 分層與高可用

| 層 | 裝置 | 職責 | 狀態 |
|---|---|---|---|
| 邊緣 | DNS / CDN | 靜態資源、就近接入 | — |
| 四層 | L4 LB（雲 LB / LVS）| 分流到 gateway | 無狀態 |
| 七層 | Gateway / Ingress（nginx / Envoy / Kong）| **限流第一道**、路由、認證 | 無狀態 |
| 應用 | API Service（N pods）| 業務邏輯 | **無狀態** |
| 共享狀態 | **Redis Cluster**（Sentinel / Cluster HA）| 限流 / 快取 / 去重 / 鎖 | **有狀態** |
| 異步 | **Kafka Cluster**（N brokers，KRaft）| 削峰 / 解耦 / 事件 | **有狀態** |
| 消費 | Consumer Group（N 實例）| 勻速消費 = 漏桶落地 | 無狀態（offset 存 Kafka）|
| 儲存 | DB Primary + Replica | 真相來源、讀寫分離 | **有狀態** |

**關鍵原則**：
- **無狀態的（API / Consumer / Gateway）才能隨便加 pod 水平擴。**
- **狀態集中在 Redis / Kafka / DB**，這三個都必須高可用（Redis 用 Sentinel / Cluster、Kafka 多 broker + 副本、DB 主從）。
- 這就是「無狀態服務 + 有狀態中介軟體」的經典切分：把難處理的狀態壓縮到少數幾個有 HA 保障的元件，其餘全部做成可拋棄、可重建的無狀態節點。

---

## 9. 坑

- **以為寫請求不碰 Redis**：如第 4 節，寫路徑碰 Redis 至少 3 次（限流 / 去重 / 刪快取）。Kafka 只是中間的緩衝。
- **限流放太內層**：放到 business logic 才擋，超量流量已經吃掉連線 / 記憶體了。越外層越省。
- **只有 per-user 桶沒有全域桶**：一萬用戶各自沒超標，加總把 DB 打爆。要疊全域桶（見 docs/06 §3.0）。
- **consumer 不限速**：Kafka 削了峰，consumer 卻全速消費把 DB 打爆——漏桶的「勻速流出」沒實作，等於白削。consumer 端要用 `rate.Limiter` 或控 poll 批量。
- **consumer 不冪等**：Kafka 至少一次（at-least-once）投遞，同一訊息可能重複消費。必須用 DB 唯一約束或 Redis dedup key 去重。
- **把熔斷 / 降級當成 Redis 功能**：它們是應用層邏輯（gobreaker 之類），跟 Redis 無關。別到處找「Redis 熔斷指令」。
- **忘了監控 consumer lag**：lag 持續上升 = 消費跟不上生產 = 漏桶要溢出。要告警 + 反壓 + 擴 consumer。
- **retention 設太短**：Kafka 訊息還沒被消費完就過期刪掉 = 資料遺失。retention 要 > 最壞情況的消費延遲。
- **Redis / Kafka 沒 HA**：有狀態元件單點掛掉 = 全站癱瘓。Redis 上 Sentinel/Cluster、Kafka 多副本，非做不可。

---

## 延伸閱讀

- 限流四演算法細節（token bucket / 漏桶 / 滑動窗口 / 固定窗口）→ docs/06 §3
- 快取三大問題 + 一致性（cache aside / 延遲雙刪）→ docs/06 §2
- 分散式鎖 + fencing token + 冪等 → docs/05 §5
- 令牌桶可跑範例 → `labs/01-string-counter` demo 12-14；fencing token → demo 17
