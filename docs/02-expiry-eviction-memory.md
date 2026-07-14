# Stage 2：過期 (Expiry)、淘汰 (Eviction) 與記憶體 (Memory)

> 模組：`github.com/twteam/go-redis-kit` ｜ Go 1.26.1 ｜ Client：`github.com/redis/go-redis/v9`
>
> 本文承接 Stage 1，聚焦 Redis 如何「讓 key 消失」與「在記憶體不足時做取捨」。這兩件事看起來是兩個獨立主題，實際上緊密相連：TTL 決定 key「應該」何時消失，過期機制決定 key「實際」何時被清掉，而淘汰策略決定「記憶體真的滿了、又沒東西自然過期」時該犧牲誰。理解這條鏈路，是把 Redis 從「會用」變成「敢在正式環境用」的關鍵。

---

## 1. 本階段目標

讀完並做完本文的實驗後，你應該能夠：

1. 說清楚 Redis 的**兩種過期刪除機制**（惰性刪除、定期抽樣刪除），並解釋「為什麼一個 key TTL 到了、記憶體卻沒有立刻降下來」。
2. 熟練使用 TTL 相關指令：`EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT` / `TTL` / `PTTL` / `PERSIST` / `EXPIRETIME`，並理解 `SET ... EX/PX/EXAT/KEEPTTL` 的語意。
3. 完整背出並選用 8 種 `maxmemory-policy`：`noeviction`、`allkeys-lru`、`allkeys-lfu`、`allkeys-random`、`volatile-lru`、`volatile-lfu`、`volatile-ttl`、`volatile-random`。
4. 解釋**近似 LRU** 與 **LFU 計數器 + 衰減**的原理，並知道 `maxmemory-samples`、`lfu-log-factor`、`lfu-decay-time` 各調什麼。
5. 使用記憶體分析工具鏈：`redis-cli --bigkeys` / `--memkeys`、`MEMORY USAGE`、`MEMORY DOCTOR`、`MEMORY STATS`、`OBJECT ENCODING`，並會調各資料型別的 listpack 編碼門檻。
6. 辨識並處理**大 key / 熱 key** 問題。
7. 避開常見地雷：集中過期造成的 TTL 抖動、沒設 `maxmemory` 造成 OOM、把近似 LRU 當精確 LRU、fork 時記憶體翻倍。
8. 用 go-redis 寫出**帶抖動 (jitter) 的分級 TTL 快取層**，並監聽過期事件。

---

## 2. 過期機制

### 2.1 兩種刪除策略：惰性 + 定期抽樣

Redis 的 key 過期後**不會**立刻從記憶體中消失。Redis 用兩種互補的機制來清理過期 key：

#### (a) 惰性刪除 (Lazy / Passive expiration)

當**有人存取**某個 key 時（`GET`、`HGET`、`EXPIRE` 等），Redis 會先檢查它是否過期。如果過期了，就在這一刻刪掉它，並對呼叫端回傳「不存在」。

- 優點：CPU 成本極低，只在真的碰到 key 時才做檢查。
- 缺點：如果一個過期 key **再也沒有人存取**，它會永遠佔著記憶體，直到被定期刪除或淘汰掃到。

#### (b) 定期抽樣刪除 (Active / Proactive expiration)

Redis 有一個背景任務（serverCron，預設每秒跑約 10 次，由 `hz` 參數控制），會**主動抽樣**帶有 TTL 的 key 來清理。核心演算法（Redis 的 `activeExpireCycle`）大致是：

1. 從「設有過期時間的 key」集合中隨機抽取一批（預設 `ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP` ≈ 20 個）。
2. 刪掉其中已過期的 key。
3. 如果這批裡**已過期的比例超過 25%**，代表過期 key 還很多，立刻再抽一批重複，直到低於門檻或超過時間預算（避免卡住主執行緒）。

這個「抽樣 + 25% 門檻」的設計，讓 Redis 用可控的 CPU 成本，把過期 key 的整體佔比壓在一個上限附近，而不是保證「TTL 一到就馬上刪」。

> 補充：Redis 6 之後預設 `lazyfree` 相關選項可讓刪除大 key 時改由背景執行緒釋放記憶體（`lazyfree-lazy-expire yes`），避免主執行緒因釋放大物件而卡頓。

### 2.2 為何過期 key 仍可能佔記憶體

把上面兩點合起來看，就能解釋這個常見困惑：

> 「我明明設了 `EXPIRE key 10`，10 秒後 `TTL` 回 -2（不存在），但 `INFO memory` 的 `used_memory` 沒有馬上降下來。」

原因：

1. **惰性刪除只在存取時觸發**——沒人碰的過期 key 不會被清。
2. **定期刪除是抽樣、非全掃**——它只保證過期 key 的比例不會失控，不保證每一個都即時刪除。
3. **記憶體釋放 ≠ 還給作業系統**——即使 Redis 內部釋放了物件，jemalloc 可能仍持有這塊記憶體以備後用（記憶體碎片，`mem_fragmentation_ratio`），`used_memory_rss` 不一定跟著降。

實務上這代表：**你不能靠 TTL 來精準控制記憶體上限**。要控制上限，必須設 `maxmemory` + 淘汰策略（見第 3 節）。TTL 是「資料正確性/時效性」工具，`maxmemory` 才是「記憶體安全」工具。

### 2.3 TTL 相關指令

| 指令 | 作用 | 時間單位 | 備註 |
|------|------|----------|------|
| `EXPIRE key seconds` | 設定相對過期（幾秒後） | 秒 | Redis 7.0+ 支援 `NX/XX/GT/LT` 旗標 |
| `PEXPIRE key ms` | 設定相對過期 | 毫秒 | |
| `EXPIREAT key unix-seconds` | 設定絕對過期（到某個 Unix 時間） | 秒 (Unix timestamp) | 適合「所有 key 同一時刻過期」的情境（也易造成雪崩） |
| `PEXPIREAT key unix-ms` | 設定絕對過期 | 毫秒 (Unix timestamp) | |
| `TTL key` | 查剩餘存活秒數 | 秒 | 回 `-1`＝存在但無過期；回 `-2`＝key 不存在 |
| `PTTL key` | 查剩餘存活毫秒數 | 毫秒 | 同上語意 |
| `PERSIST key` | 移除 key 的過期時間，變永久 | — | 成功回 1，本來就沒 TTL 或 key 不存在回 0 |
| `EXPIRETIME key` | 查絕對過期時間點 | 秒 (Unix timestamp) | Redis 7.0+ |
| `PEXPIRETIME key` | 查絕對過期時間點 | 毫秒 (Unix timestamp) | Redis 7.0+ |

`EXPIRE` 的旗標（Redis 7.0+）在避免「意外延長 TTL」時很有用：

```bash
EXPIRE key 100 NX   # 只有在 key 目前沒有 TTL 時才設定
EXPIRE key 100 XX   # 只有在 key 目前已有 TTL 時才設定
EXPIRE key 100 GT   # 只有在新 TTL 大於現有 TTL 時才設定（延長，不縮短）
EXPIRE key 100 LT   # 只有在新 TTL 小於現有 TTL 時才設定（縮短，不延長）
```

`SET` 也可以在寫入的同時設定 TTL，這是最常用、也最不容易出錯的方式（一步完成，避免「SET 成功但 EXPIRE 失敗」造成永久 key）：

```bash
SET session:abc "..." EX 3600          # 1 小時後過期
SET session:abc "..." PX 500           # 500ms 後過期
SET session:abc "..." EXAT 1751900000  # 絕對時間過期
SET session:abc "..." EX 3600 NX       # 只在不存在時寫入 + 設 TTL（分散式鎖常用）
SET session:abc "..." KEEPTTL          # 更新 value 但保留原本的 TTL（否則 SET 會清掉 TTL！）
```

> 陷阱：**不帶 `KEEPTTL` 的 `SET` 會清掉既有 TTL**，讓 key 變永久。如果你用 `SET` 更新一個原本有 TTL 的 key，卻忘了 `KEEPTTL`，就會製造出永不過期的殭屍 key，長期累積成記憶體洩漏。

### 2.4 過期事件通知 (Keyspace Notifications)

Redis 可以在 key 過期、被改、被刪時，透過 **Pub/Sub** 發出事件。這讓應用程式能「在 key 過期的當下」做反應（例如清本地快取、觸發補償流程）。

啟用方式（設定 `notify-keyspace-events` 參數）：

```bash
# 動態開啟：K=keyspace 事件、E=keyevent 事件、x=過期事件、g=一般指令、$=string、l=list...
CONFIG SET notify-keyspace-events Ex
# 或在 redis.conf：
# notify-keyspace-events "Ex"
```

參數字母組合（常用）：

| 字母 | 含義 |
|------|------|
| `K` | Keyspace 事件，頻道格式 `__keyspace@<db>__:<key>` |
| `E` | Keyevent 事件，頻道格式 `__keyevent@<db>__:<event>` |
| `x` | 過期事件（key expired） |
| `e` | 淘汰事件（key evicted，因 maxmemory） |
| `g` | 一般指令（DEL、EXPIRE、RENAME...） |
| `$` `l` `s` `h` `z` `t` | 各資料型別的事件（string/list/set/hash/zset/stream） |
| `A` | `g$lshzxet` 全部的別名（除了 `m`/`n`） |

訂閱過期事件（`E` + `x`，所以設 `Ex`）：

```bash
# 訂閱 db 0 的所有過期事件
redis-cli --csv PSUBSCRIBE '__keyevent@0__:expired'
```

> 重要限制：**過期事件是在 key 被「實際刪除」時才發出，不是 TTL 到期的瞬間**。因為刪除依賴惰性 + 定期抽樣，一個沒人存取的過期 key，它的 `expired` 事件可能會延遲數秒（等定期刪除掃到）。所以過期事件**不適合**用來做「精準到毫秒的排程器」。
>
> 另一個限制：Keyspace notifications 走 Pub/Sub，是 **fire-and-forget**，沒有持久化、沒有 ACK。訂閱者當機期間錯過的事件不會補送。需要可靠事件流時，改用 Redis Stream。

#### Go 範例：訂閱過期事件

前提：redis 已 `CONFIG SET notify-keyspace-events Ex`（預設關閉，不開不會發事件）。

```go
// WatchExpired 訂閱 db 0 的過期事件；key 過期當下呼叫 onExpire。
func WatchExpired(ctx context.Context, rdb *redis.Client, onExpire func(key string)) {
	pubsub := rdb.PSubscribe(ctx, "__keyevent@0__:expired")
	go func() {
		defer pubsub.Close()
		for msg := range pubsub.Channel() { // Channel() 內部自動重連 + buffer
			onExpire(msg.Payload) // msg.Payload = 過期的 key 名
		}
	}()
}

// 用法：過期就清本地快取
WatchExpired(ctx, rdb, func(key string) {
	log.Printf("過期：%s", key)
	localCache.Delete(key)
})
```

`__keyevent@0__:expired` 的 `@0` 是 db 編號，`msg.Payload` 是過期的 key 名。若要盯特定 key 的所有事件，改訂 keyspace 頻道 `__keyspace@0__:<key>`（payload 變成事件名 `expired`）。

#### 過期事件不可靠 → 三種補救

過期事件**雙重不可靠**：(a) 發得不準（實際刪除才發，延遲數秒）(b) 收得會漏（fire-and-forget，訂閱者離線期間全丟）。所以**不能拿它當關鍵動作的唯一觸發**。要可靠有三條路線：

| 路線 | 做法 | 適用 |
|---|---|---|
| **① 事件 + 兜底掃描** | 過期事件當「即時性優化」（收到就快點反應），另配一個**定期 worker 掃描**（掃 DB / ZSet 找「該處理但沒處理」的），漏掉的靠掃描補 | 想保留即時性、又不能漏 |
| **② ZSet 延遲隊列**（不靠過期事件） | `ZADD delay:queue <到期時間戳> <任務>`，worker 定期 `ZRANGEBYSCORE 0 <now>` 撈到期項處理。精準（自控掃描頻率）、可靠（沒處理就一直在 ZSet）、可重放 | **要精準排程**（SLA 到期、定時任務）|
| **③ Stream 可靠事件流** | 「該過期時」自己 `XADD` 寫進 Stream，consumer 用 group + `XACK` 消費。有持久化、ACK、重放、崩潰回收 | **要可靠事件**（訂單、金流）|

一句話：keyspace 過期事件 = best-effort 通知，只當**優化**不當**保證**；要精準用 **ZSet 延遲隊列**，要可靠用 **Stream**。

> **實例**：本 repo 對照的 SLA timer 系統走 ZSet（`sla:due_timers`，score=到期時間），**沒**用 keyspace 過期事件——因為 SLA 到期處理不能漏，這是路線 ②，選對了。

---

### 2.5 一般 Pub/Sub（發布訂閱）

過期事件其實只是「Redis 幫你 PUBLISH 的一種 Pub/Sub」。一般 Pub/Sub 則是你自己發、自己訂。

**本質**：即時廣播。發了就送給**當下在線**的訂閱者，沒在線就漏、無持久化、無 ACK、無回溯。

#### 設定：不用開任何 config

keyspace notification 才要 `notify-keyspace-events`；一般 Pub/Sub **直接發直接訂**：

```bash
# 訂閱端
redis-cli SUBSCRIBE cache:invalidate
# 另一個終端 發布端
redis-cli PUBLISH cache:invalidate "user:100"
```

```go
// 訂閱端（每個 pod 起一個）
pubsub := rdb.Subscribe(ctx, "cache:invalidate")
defer pubsub.Close()
for msg := range pubsub.Channel() {
	invalidateLocalCache(msg.Payload) // "user:100" → 清本地快取
}

// 發布端（更新 DB 後廣播）
rdb.Publish(ctx, "cache:invalidate", "user:100")
```

- `SUBSCRIBE ch` 訂精確頻道；`PSUBSCRIBE ch:*` 訂 pattern（萬用字元）。
- go-redis 的 `pubsub.Channel()` 回一個 Go channel，內部自動重連 + buffer。

#### 什麼情境用（即時性 > 可靠性）

| 情境 | 說明 |
|---|---|
| **多 pod 本地快取失效** | 一處更新 DB → `PUBLISH cache:invalidate <key>` → 所有 pod 收到清自己本地快取（呼應 docs/06 §2.4「本地快取靠 Pub/Sub 廣播失效」）|
| **即時訊息推送 / 線上狀態** | 聊天、通知、presence |
| **配置熱更新** | 配置變更廣播給所有實例 reload，不用重啟 |
| **跨實例輕量信號** | 服務間即時通知「某事發生了」，不在乎歷史 |

#### Pub/Sub vs Stream

| | Pub/Sub | Stream |
|---|---|---|
| 交付 | fire-and-forget，在線才收 | 持久化，離線回來可補 |
| ACK | 無 | `XACK` |
| 重放 | 不行 | 可（`XRANGE` / 從任意 ID 重讀）|
| 消費者崩潰 | 訊息永久漏 | 留 PEL，`XAUTOCLAIM` 回收 |
| 成本 | 極省 | 較貴（ID / PEL / group 狀態）|

**一句話**：Pub/Sub 即時但會漏（在線才收）；Stream 可靠可重放（離線回來還能補）。要可靠交付 / 重放 / ACK（訂單、金流、任務隊列）→ 一律用 Stream（docs/01 §6）或 Kafka（docs/08），別用 Pub/Sub。

---

## 3. 淘汰策略 `maxmemory-policy` 全解

當 Redis 的記憶體用量達到 `maxmemory` 時，寫入新資料前會依 `maxmemory-policy` 決定「要不要淘汰、淘汰誰」。

先設定上限（沒設 `maxmemory`（=0）代表無上限，最終會吃光機器記憶體被 OS OOM killer 幹掉）：

```bash
CONFIG SET maxmemory 512mb
CONFIG SET maxmemory-policy allkeys-lru
```

8 種策略分兩個維度：**候選集合**（所有 key `allkeys-*` vs 只有帶 TTL 的 key `volatile-*`）× **淘汰依據**（LRU / LFU / random / TTL）。

| 策略 | 候選集合 | 淘汰依據 | 一句話說明 |
|------|----------|----------|------------|
| `noeviction` | — | 不淘汰 | 記憶體滿時，寫入指令直接回錯誤 `OOM command not allowed`，讀取仍可。**預設值** |
| `allkeys-lru` | 全部 key | 最久沒用 | 淘汰最近最少使用的 key（近似 LRU） |
| `allkeys-lfu` | 全部 key | 最少被用 | 淘汰使用頻率最低的 key（近似 LFU，Redis 4.0+） |
| `allkeys-random` | 全部 key | 隨機 | 隨機挑 key 淘汰 |
| `volatile-lru` | 只有帶 TTL 的 key | 最久沒用 | 只在「有過期時間」的 key 中選 LRU 淘汰 |
| `volatile-lfu` | 只有帶 TTL 的 key | 最少被用 | 只在帶 TTL 的 key 中選 LFU 淘汰 |
| `volatile-ttl` | 只有帶 TTL 的 key | 最接近過期 | 優先淘汰「剩餘 TTL 最短」的 key |
| `volatile-random` | 只有帶 TTL 的 key | 隨機 | 在帶 TTL 的 key 中隨機淘汰 |

### 3.1 何時用哪個？

- **純快取（cache-aside，資料在 DB 有備份，Redis 只是加速層）→ `allkeys-lfu`（首選）或 `allkeys-lru`。**
  快取的 key 不一定都有 TTL，且我們希望把冷資料淘汰、留住熱資料。LFU 在「有少數超熱 key + 大量偶爾存取 key」的實際流量下，通常比 LRU 命中率更高，因為它看「頻率」而非「最近一次」，不會被一次掃描表 (scan) 汙染。

- **Session / 短期 token（每個 key 天生都有 TTL）→ `volatile-ttl` 或 `volatile-lru`。**
  `volatile-ttl` 讓「快過期的先走」，符合 session「反正快到期了」的直覺，記憶體壓力下優先犧牲即將無用的資料。

- **混合負載：Redis 同時放「有 TTL 的快取」與「無 TTL 的重要資料（如設定、計數器）」→ `volatile-*`。**
  這樣淘汰只會發生在帶 TTL 的快取上，保護沒設 TTL 的重要資料不被誤刪。**但前提是你有把握重要資料一定沒設 TTL**；否則會漏保護。

- **`allkeys-random` / `volatile-random`**：極少用。適合「所有 key 存取機率均勻、算 LRU/LFU 反而浪費 CPU」的特殊場景，或壓測基準線。

- **`noeviction`**：當「寧可寫入失敗、也不能弄丟任何資料」時用（例如把 Redis 當唯一資料源的佇列）。**代價是**記憶體滿時所有寫入報錯，應用必須能處理 OOM error。

> 決策速記：**能重建的快取 → allkeys；不能亂丟的資料 → volatile 保護無 TTL 的那批。頻率導向選 lfu，時效導向選 ttl。**

### 3.2 一個常見誤解

`volatile-*` 系列在**沒有任何帶 TTL 的 key 可淘汰**時，行為等同 `noeviction`——也就是寫入會直接報 OOM。所以如果你選了 `volatile-lru`，卻忘了給快取 key 設 TTL，記憶體滿時 Redis 會「找不到能淘汰的對象」而拒絕寫入。這是正式環境常見事故。

---

## 4. LRU vs LFU 原理

### 4.1 近似 LRU（抽樣，不是精確 LRU）

「精確 LRU」需要維護一個雙向鏈結串列，每次存取都把 key 移到頭部——這對每個 key 都要額外指標，記憶體與 CPU 成本高。Redis 為了省記憶體，用的是**近似 LRU**：

- 每個物件的 header 存一個 24-bit 的 **LRU 時鐘戳記**（最後存取時間，解析度到秒）。
- 淘汰時**不掃全部 key**，而是**隨機抽樣 `maxmemory-samples` 個**（預設 5），從樣本中挑最舊的那個淘汰。
- Redis 3.0+ 還維護一個「淘汰候選池 (eviction pool)」，跨多輪抽樣累積較舊的 key，讓近似結果更接近真正的 LRU。

`maxmemory-samples` 是精度 vs CPU 的權衡：

```bash
CONFIG SET maxmemory-samples 10   # 樣本越多越接近精確 LRU，但每次淘汰更耗 CPU
```

- 5（預設）：省 CPU，近似度足夠多數場景。
- 10：明顯更接近精確 LRU，CPU 成本上升有限，命中率敏感的快取可考慮。
- 越高邊際效益遞減。

> 關鍵認知：**近似 LRU 不保證淘汰的一定是「全域最久沒用」的 key**，只是「抽到的樣本中最舊的」。所以不要假設它精確——見第 7 節地雷。

### 4.2 LFU（計數器 + 衰減）

LFU（Least Frequently Used，Redis 4.0+）淘汰「使用頻率最低」的 key。它同樣復用物件 header 那 24 bits，但拆成兩段：

- **16 bits：上次存取時間**（分鐘級，供衰減用）。
- **8 bits：存取計數器**，但它**不是線性計數**，而是**對數近似計數 (logarithmic counter)**，範圍 0–255。

兩個關鍵參數：

```bash
CONFIG SET lfu-log-factor 10   # 計數器增長的對數因子，越大→計數器越難增長→能區分超高頻 key
CONFIG SET lfu-decay-time 1    # 計數器衰減時間（分鐘），閒置這麼久後計數器每過一段就 -1
```

- **`lfu-log-factor`（對數因子）**：8 bits 最多數到 255，若用線性計數，熱 key 幾秒就爆表、大家都 255 無法區分。對數計數讓「存取次數」以遞減機率增加計數器，`log-factor` 越大，計數器越「慢熱」，就能在有限的 0–255 範圍內區分「被存取 100 次」與「被存取 10 萬次」的 key。
- **`lfu-decay-time`（衰減時間）**：如果只增不減，曾經很熱、後來變冷的 key 會永遠保有高計數，賴著不走。衰減機制讓計數器隨閒置時間下降（預設每閒置 1 分鐘、計數器減 1 的量級），使 LFU 能反映「近期頻率」而非「歷史總量」。

新 key 建立時計數器初始為 5（`LFU_INIT_VAL`），避免剛進來就因計數 0 被立刻淘汰。

可用 `OBJECT FREQ key`（需在 LFU 模式下）查某個 key 目前的計數器值，`OBJECT IDLETIME key`（LRU 模式下）查閒置秒數：

```bash
CONFIG SET maxmemory-policy allkeys-lfu
OBJECT FREQ mykey       # 回傳 0-255 的 LFU 計數
# 切回 LRU 模式才能用 IDLETIME
CONFIG SET maxmemory-policy allkeys-lru
OBJECT IDLETIME mykey   # 回傳閒置秒數
```

### 4.3 LRU vs LFU 該選誰

| 情境 | 建議 | 理由 |
|------|------|------|
| 有明顯熱點、長尾偶發存取 | LFU | 頻率導向，抗「一次性掃描」汙染 |
| 存取有時間局部性（剛用的很可能再用） | LRU | 近期性導向，實作/直覺簡單 |
| 週期性全表掃描（報表、備份 job） | LFU | LRU 會被掃描「洗掉」熱資料，LFU 因掃描只 +1 計數不受影響 |

---

## 5. 記憶體分析工具

### 5.1 `redis-cli --bigkeys` / `--memkeys`

`--bigkeys` 用 `SCAN` 遍歷整個 keyspace，找出**每種型別中元素最多 / 佔用最大**的 key。它是抽樣統計、對線上相對安全（非阻塞）：

```bash
redis-cli --bigkeys
# 輸出範例：
# [00.00%] Biggest string found so far '"user:1"' with 3 bytes
# [12.34%] Biggest hash   found so far '"cart:42"' with 12000 fields
# ...
# Biggest hash found '"cart:42"' has 12000 fields
```

`--memkeys` 類似，但用 `MEMORY USAGE` 直接以「實際記憶體位元組數」排序（更準，但更耗資源）：

```bash
redis-cli --memkeys
redis-cli --memkeys-samples 0   # 0 = 不抽樣，精確但慢（容器/元素多時謹慎）
```

> 線上使用建議：`--bigkeys` 會發出大量 `SCAN`；在高負載節點跑，最好加 `-i 0.1`（每批間隔）降低壓力，或在 replica 上跑。

### 5.2 `MEMORY USAGE key`

回傳「單一 key 連同其 value、內部結構、overhead」估算佔用的位元組數：

```bash
MEMORY USAGE cart:42
MEMORY USAGE cart:42 SAMPLES 0   # 對集合型別，0=掃全部元素精算；預設抽樣 5 個估算
```

### 5.3 `MEMORY DOCTOR`

Redis 內建的「健檢醫生」，用人話回報記憶體是否健康、碎片是否過高、是否有異常：

```bash
MEMORY DOCTOR
# "Sam, I detected a few issues in this Redis instance memory implants:
#  * High fragmentation: ..."
# 或 "Sam, I can't find any memory problem..."（一切正常）
```

### 5.4 `MEMORY STATS`

回傳一大包詳細記憶體指標（比 `INFO memory` 更細），重點欄位：

| 欄位 | 含義 |
|------|------|
| `peak.allocated` | 歷史記憶體用量峰值 |
| `total.allocated` | 目前配置器（jemalloc）配出的總量 |
| `startup.allocated` | 啟動時的基礎用量 |
| `dataset.bytes` / `dataset.percentage` | 真正資料（扣掉 overhead）佔用 |
| `overhead.total` | 管理結構、client buffer、複寫 backlog 等額外開銷 |
| `keys.count` | key 總數 |
| `keys.bytes-per-key` | 平均每 key 佔用 |
| `allocator-frag-ratio` | 配置器層碎片比 |

```bash
MEMORY STATS
# 對照 INFO memory 的 used_memory / used_memory_rss / mem_fragmentation_ratio
INFO memory
```

> `mem_fragmentation_ratio` = `used_memory_rss / used_memory`。>1.5 通常代表碎片偏高；<1 代表 Redis 記憶體被 swap 到磁碟（很糟，延遲會爆）。

### 5.5 `OBJECT ENCODING` 與編碼門檻調參

Redis 每種資料型別，底層依「大小」自動切換記憶體佈局：小的用**緊湊編碼**（省記憶體、順序存取），超過門檻後轉成**通用編碼**（多耗記憶體、換取 O(1)/O(logN) 操作）。

```bash
OBJECT ENCODING mykey
```

各型別的編碼與門檻參數（Redis 7.x）：

| 型別 | 小 → 緊湊編碼 | 大 → 通用編碼 | 控制門檻的參數 |
|------|--------------|--------------|----------------|
| String | `int`（純整數）/ `embstr`（≤44 bytes） | `raw`（>44 bytes） | 固定，不可調 |
| Hash | `listpack` | `hashtable` | `hash-max-listpack-entries`（預設 128）、`hash-max-listpack-value`（預設 64 bytes） |
| List | `listpack`（小）；大的用 `quicklist`（listpack 節點串成的鏈） | `quicklist` | `list-max-listpack-size`（預設 128，正數=元素數；負數=每節點大小如 -2=8KB） |
| Set | 全整數且少 → `intset`；小字串集合 → `listpack`；否則 → `hashtable` | `hashtable` | `set-max-intset-entries`（預設 512）、`set-max-listpack-entries`（128）、`set-max-listpack-value`（64） |
| ZSet | `listpack` | `skiplist` | `zset-max-listpack-entries`（預設 128）、`zset-max-listpack-value`（64） |

> 注意：Redis 7.0 把舊名 `ziplist` 改稱 `listpack`（`hash-max-ziplist-entries` 等舊參數名仍相容）。

調門檻範例（記憶體吃緊時，把門檻調低讓更多 key 轉通用編碼；反之調高留在緊湊編碼省記憶體）：

```bash
CONFIG SET hash-max-listpack-entries 256
CONFIG SET hash-max-listpack-value 128
CONFIG SET zset-max-listpack-entries 256
```

> 權衡：緊湊編碼（listpack）省記憶體，但操作是 O(N)（要線性掃描 listpack）。若把門檻設太高，一個「用緊湊編碼但塞了幾千個元素」的 hash，單次 `HGET` 會退化成掃幾千個元素——這就變成「隱形的大 key + 慢操作」。門檻是**記憶體 vs 操作延遲**的權衡，不是越大越好。

---

## 6. 大 key / 熱 key 問題與解法

### 6.1 大 key (Big Key)

**定義**：單一 key 佔用過大記憶體，或集合型別元素數過多（例如一個 hash 有數十萬 field、一個 string 幾 MB）。

**危害**：
- **阻塞**：`DEL` 一個大 key、或對大集合做 `HGETALL` / `SMEMBERS` / `LRANGE 0 -1`，會長時間佔住單執行緒主迴圈，拖慢所有其他請求。
- **記憶體不均**：在 Cluster 模式下，一個大 key 卡在單一 slot，造成節點間記憶體嚴重不均，無法透過分片分散。
- **網路 / 序列化**：一次讀出大 key 產生巨大回應，撐爆 client buffer、拉高延遲。
- **淘汰放大**：淘汰或過期一個大 key 時，一次釋放大量記憶體可能造成延遲尖刺。

**解法**：
- **拆分**：把一個巨大 hash 依 field hash 拆成多個小 hash（`bigkey:{shard}`）；把巨大 list/set 分頁。
- **避免全量操作**：改用 `HSCAN` / `SSCAN` / `ZSCAN` 分批遊走，取代 `HGETALL` / `SMEMBERS`。
- **非阻塞刪除**：用 `UNLINK`（背景釋放）取代 `DEL`；並開 `lazyfree-lazy-expire` / `lazyfree-lazy-eviction`。
- **定期偵測**：排程跑 `redis-cli --bigkeys` / `--memkeys` 監控。

### 6.2 熱 key (Hot Key)

**定義**：極少數 key 承載了不成比例的高存取量（例如秒殺商品、首頁 banner 快取），造成單一 key / 單一分片 CPU 打滿。

**偵測**：

```bash
# 需先設 maxmemory-policy 為 lfu 模式，hotkeys 才能運作
CONFIG SET maxmemory-policy allkeys-lfu
redis-cli --hotkeys
# 或用 MONITOR（僅偵錯，勿長跑，會拖慢 server）取樣觀察高頻 key
```

**解法**：
- **本地快取 (client-side cache)**：在應用端加一層短 TTL 的 process 內快取（如 Go 的 in-memory cache 或 go-redis 的 client-side caching），把熱 key 讀取擋在應用層。
- **多副本分散讀**：把熱 key 複製成 `hotkey:0`..`hotkey:N`，讀取時隨機挑一份，分散到不同 slot/節點。
- **讀寫分離**：熱點讀導向 replica。
- **限流 / 合併**：對同一 key 的並發回源做 singleflight（合併重複請求，見第 10 節防擊穿）。

---

## 7. 踩坑清單

1. **集中過期 → TTL 抖動 / 快取雪崩。**
   若一批 key 用同一個絕對時間（`EXPIREAT`）或同一相對 TTL 同時寫入，它們會在同一秒集體過期。瞬間：(a) 定期刪除要清一大批 key 造成 CPU 尖刺；(b) 大量請求同時 miss、同時回源 DB，把 DB 打垮（雪崩）。
   **解法**：TTL 加隨機抖動（jitter），把過期時間打散（見第 9、10 節的 helper）。

2. **沒設 `maxmemory` → OOM。**
   `maxmemory 0`（預設）代表無上限。資料一直長，最後被 OS 的 OOM killer 殺掉整個 Redis 程序——不是優雅淘汰，是直接掛。**正式環境必設 `maxmemory` + 對應 policy**，且留 ~25% 記憶體給 fork/buffer/碎片（見第 4 點）。

3. **把近似 LRU 當精確 LRU。**
   `allkeys-lru` / `volatile-lru` 是抽樣近似，**不保證**淘汰的是全域最久沒用的 key。如果你的邏輯依賴「一定會先淘汰某個特定舊 key」，會出錯。要更準就調高 `maxmemory-samples`，但仍非精確。

4. **fork 時記憶體翻倍（連結持久化章）。**
   Redis 做 RDB 快照（`BGSAVE`）或 AOF 重寫時會 `fork()` 子程序。父子透過 **copy-on-write** 共享記憶體，理論上不複製；但只要**父程序在 fork 後持續寫入**，被寫到的頁面就會被複製一份。寫入越頻繁，複製越多，最壞情況記憶體用量接近翻倍。若機器記憶體剛好卡在 `maxmemory` 上限、沒留 buffer，fork 就可能觸發 OOM。
   **解法**：`maxmemory` 設為實體記憶體的 ~50–60%（單機同時跑持久化時），留足 CoW 空間；細節見持久化章 (Stage 4)。

5. **`volatile-*` 但 key 沒 TTL → 記憶體滿時等同 noeviction。**（見 3.2）

6. **`SET` 覆寫清掉 TTL。**（見 2.3，忘了 `KEEPTTL`）

7. **過期事件當精準排程器用。**（見 2.4，事件有延遲、不可靠）

---

## 8. redis-cli 實驗：`maxmemory 10mb` + `allkeys-lru`，灌資料看淘汰

完整步驟（假設 docker-compose 已起好一個 Redis，或本機 `redis-server`）：

```bash
# 0) 進入 redis-cli
redis-cli

# 1) 清空 + 設定 10mb 上限與 LRU 淘汰
127.0.0.1:6379> FLUSHALL
127.0.0.1:6379> CONFIG SET maxmemory 10mb
127.0.0.1:6379> CONFIG SET maxmemory-policy allkeys-lru
127.0.0.1:6379> CONFIG GET maxmemory maxmemory-policy

# 2) 記下初始狀態
127.0.0.1:6379> INFO memory
#   關注 used_memory_human
127.0.0.1:6379> INFO stats
#   關注 evicted_keys（目前應為 0）
```

用 shell 迴圈灌大量資料（跳出 cli，在 bash 執行）：

```bash
# 3) 灌 20 萬個 ~100 bytes 的 key，遠超 10mb，逼出淘汰
for i in $(seq 1 200000); do
  redis-cli SET "k:$i" "$(head -c 100 </dev/zero | tr '\0' 'x')" >/dev/null
done
# （更快的做法：用 pipe mode）
# seq 1 200000 | awk '{print "SET k:"$1" xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}' | redis-cli --pipe
```

觀察淘汰效果：

```bash
# 4) 灌完後回 cli 看結果
redis-cli INFO memory | grep -E 'used_memory_human|maxmemory_human'
#   used_memory_human 應該貼著 ~10M，不會超過太多
redis-cli INFO stats | grep evicted_keys
#   evicted_keys 應該是一個很大的數字（大量 key 被 LRU 淘汰）
redis-cli DBSIZE
#   實際留存的 key 數遠小於 200000

# 5) 驗證「最近存取的 key 較容易留下」
redis-cli SET hotkey "I am hot"
redis-cli GET hotkey        # 一直存取它，更新其 LRU 時鐘
# 再灌一批資料後，hotkey 比冷 key 更可能存活（近似，不保證）

# 6) 對照實驗：改用 allkeys-random 或 volatile-lru，重跑觀察 evicted_keys 差異
redis-cli CONFIG SET maxmemory-policy noeviction
redis-cli SET willfail "x"   # 若已滿，回 (error) OOM command not allowed...
```

**預期學到**：
- `used_memory` 被 `maxmemory` 卡住，不再無限增長 → 這就是 `maxmemory` 的價值。
- `evicted_keys` 隨灌資料飆升 → 淘汰真的在發生。
- 切 `noeviction` 後寫入報 `OOM` 錯誤 → 理解 noeviction 的行為。

---

## 9. Go 範例：TTL + 抖動 helper + 監聽過期事件

以下片段假設 `go.mod` 已有 `github.com/redis/go-redis/v9`。

### 9.1 帶抖動的 TTL helper

```go
package rediskit

import (
	"context"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

// JitterTTL 在 base 上下加入 ±jitterRatio 的隨機抖動，避免大量 key 同時過期。
// 例：base=10m, jitterRatio=0.1 => 回傳落在 [9m, 11m] 的隨機 TTL。
func JitterTTL(base time.Duration, jitterRatio float64) time.Duration {
	if jitterRatio <= 0 {
		return base
	}
	// delta 落在 [-jitterRatio, +jitterRatio] * base
	delta := (rand.Float64()*2 - 1) * jitterRatio
	ttl := time.Duration(float64(base) * (1 + delta))
	if ttl < 0 {
		ttl = base
	}
	return ttl
}

// SetWithJitter 寫入 value 並套用帶抖動的 TTL。
func SetWithJitter(ctx context.Context, rdb *redis.Client, key string, val any, base time.Duration, jitterRatio float64) error {
	return rdb.Set(ctx, key, val, JitterTTL(base, jitterRatio)).Err()
}
```

小示範 `main`：

```go
func demoTTL() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	// 寫入帶抖動 TTL 的快取
	_ = SetWithJitter(ctx, rdb, "cache:user:1", "payload", 10*time.Minute, 0.1)

	// 查 TTL
	ttl, _ := rdb.TTL(ctx, "cache:user:1").Result()
	fmt.Println("ttl:", ttl) // 大約落在 9m~11m

	// NX + TTL 一步到位（分散式鎖常見寫法）
	ok, _ := rdb.SetNX(ctx, "lock:job", "1", 30*time.Second).Result()
	fmt.Println("acquired lock:", ok)

	// 更新 value 但保留原 TTL
	_ = rdb.Set(ctx, "cache:user:1", "new-payload", redis.KeepTTL).Err()
}
```

### 9.2 監聽過期事件

```go
// ListenExpired 訂閱 db 0 的過期事件並回呼處理。
// 前置：CONFIG SET notify-keyspace-events Ex
func ListenExpired(ctx context.Context, rdb *redis.Client, onExpired func(key string)) error {
	// 確保通知開啟（也可由 redis.conf 靜態設定）
	if err := rdb.ConfigSet(ctx, "notify-keyspace-events", "Ex").Err(); err != nil {
		return err
	}

	pubsub := rdb.PSubscribe(ctx, "__keyevent@0__:expired")
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			// msg.Payload 就是過期的 key 名稱
			onExpired(msg.Payload)
		}
	}
}
```

用法：

```go
go func() {
	_ = ListenExpired(ctx, rdb, func(key string) {
		log.Printf("key expired: %s -> 清本地快取 / 觸發補償", key)
	})
}()
```

> 提醒（呼應 2.4）：過期事件有延遲、且是 fire-and-forget。這段適合做「盡力而為」的快取同步，不要拿它當唯一的正確性保證。

---

## 10. 專案流程說明：設計快取層的 TTL 策略

在 `go-redis-kit` 的快取層裡，我們把 TTL 策略拆成兩個設計決策：**分級 TTL** 與 **抖動函式**。

### 10.1 分級 TTL（按資料特性分層）

不同資料的「可容忍陳舊度」不同，用同一個 TTL 是懶惰且危險的。建議分級：

| 分級 | 典型資料 | 建議 base TTL | 抖動 | 理由 |
|------|----------|--------------|------|------|
| Hot / 短時效 | 首頁列表、庫存、排行榜 | 30s ~ 5m | ±10% | 變動頻繁，要新鮮 |
| Warm / 中時效 | 使用者 profile、商品詳情 | 10m ~ 1h | ±10% | 偶爾變動 |
| Cold / 長時效 | 設定檔、字典表、地區清單 | 6h ~ 24h | ±20% | 幾乎不變，重在減少回源 |
| Negative（防穿透） | 「查無此資料」的空值標記 | 30s ~ 2m | ±20% | 短 TTL 防惡意打不存在 key |

在專案中把這些做成常數 / 設定，而非散落 magic number：

```go
type TTLTier struct {
	Base   time.Duration
	Jitter float64
}

var (
	TTLHot      = TTLTier{Base: 2 * time.Minute, Jitter: 0.10}
	TTLWarm     = TTLTier{Base: 30 * time.Minute, Jitter: 0.10}
	TTLCold     = TTLTier{Base: 12 * time.Hour, Jitter: 0.20}
	TTLNegative = TTLTier{Base: time.Minute, Jitter: 0.20}
)

func (t TTLTier) TTL() time.Duration { return JitterTTL(t.Base, t.Jitter) }
```

### 10.2 抖動函式的角色

抖動不是「錦上添花」，而是**快取雪崩的第一道防線**。沒有抖動時，一次冷啟動（例如部署後快取全空，同一秒把一批資料寫進去且 TTL 相同）會讓這批資料在未來同一秒集體過期，週期性地製造 DB 尖峰。加入 ±10~20% 抖動後，過期時間被均勻打散，DB 回源壓力被攤平。

### 10.3 與淘汰策略的搭配

- 這個快取層的所有 key **都帶 TTL** → 因此可安全採用 `volatile-*`（若 Redis 同時放其他無 TTL 的重要資料），或若 Redis 專用於快取，直接 `allkeys-lfu`。
- 若走 `volatile-*`，務必確保**每條寫入路徑都設了 TTL**（用上面的 helper 統一入口，禁止裸 `SET` 不帶 TTL），否則會退化成 noeviction（見 3.2）。

### 10.4 完整快取讀取流程（cache-aside + 防雪崩/擊穿/穿透）

```
讀取 key
  ├─ 命中           → 回傳
  └─ miss
       ├─ singleflight 合併同 key 並發回源（防擊穿：熱 key 過期瞬間只放一個請求打 DB）
       ├─ 查 DB
       │    ├─ 有資料 → SET(key, val, TTLTier.TTL())         # 帶抖動，防雪崩
       │    └─ 無資料 → SET(key, "", TTLNegative.TTL())      # 空值短快取，防穿透
       └─ 回傳
```

---

## 11. 練習題 + 檢查點 + 延伸閱讀

### 11.1 練習題

1. 用 `EXPIRE key 100 GT` 與 `LT`，各設計一個「只延長、不縮短 TTL」與「只縮短、不延長」的情境，並用 `TTL` 驗證行為。
2. 開 `notify-keyspace-events Exe`（過期 + 淘汰事件），寫一支訂閱程式，分別觀察「TTL 到期」與「因 maxmemory 淘汰」兩種事件，確認 payload 差異（頻道 `expired` vs `evicted`）。
3. 在第 8 節的實驗中，分別用 `allkeys-lru`、`allkeys-lfu`、`allkeys-random` 各跑一次，記錄 `evicted_keys` 與「hotkey 是否存活」，比較三者差異。
4. 造一個有 5000 個 field 的 hash，用 `OBJECT ENCODING` 看它是 `hashtable`；把 `hash-max-listpack-entries` 調到 8000 後重建，觀察編碼變化與 `MEMORY USAGE` 差異，並測 `HGET` 延遲。
5. 用 `JitterTTL(10*time.Minute, 0.1)` 連續產生 1000 個 TTL，畫出分佈（或印 min/max/平均），確認落在 9m~11m。

### 11.2 檢查點（能回答就算過關）

- [ ] 能解釋「TTL 到了但 `used_memory` 沒降」的三個原因。
- [ ] 能不看表背出 8 種 `maxmemory-policy`，並說出快取用哪個、session 用哪個。
- [ ] 能說明近似 LRU 為何「近似」，以及 `maxmemory-samples` 調高的代價。
- [ ] 能說出 LFU 的計數器為何是「對數 + 衰減」、`lfu-log-factor` 與 `lfu-decay-time` 各管什麼。
- [ ] 能列出四種找大 key 的方式，並說出為何線上要用 `--bigkeys` 而非直接 `KEYS *`。
- [ ] 能解釋 `volatile-lru` 在「沒有帶 TTL 的 key」時等同 `noeviction`。
- [ ] 能說明抖動 TTL 如何防雪崩、negative caching 如何防穿透、singleflight 如何防擊穿。
- [ ] 能說出為何 fork 可能讓記憶體逼近翻倍，以及 `maxmemory` 該保守設定的理由。

### 11.3 延伸閱讀

- Redis 官方文件：Key eviction — <https://redis.io/docs/latest/develop/reference/eviction/>
- Redis 官方文件：Keyspace notifications — <https://redis.io/docs/latest/develop/use/keyspace-notifications/>
- Redis 官方文件：Memory optimization — <https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/memory-optimization/>
- Redis 官方文件：EXPIRE / SET 指令頁（各旗標語意）— <https://redis.io/commands/expire/> 、 <https://redis.io/commands/set/>
- Salvatore Sanfilippo（antirez）部落格：Random notes on improving the Redis LRU algorithm（近似 LRU 與 LFU 的設計動機）
- go-redis 文件：<https://redis.uptrace.dev/>
- 下一站：**Stage 3（資料結構深入）** 與 **Stage 4（持久化 RDB/AOF 與 fork/CoW 細節）**——本文第 7 節第 4 點的 fork 記憶體議題會在 Stage 4 展開。

---

> 小結：TTL 管「時效」，`maxmemory` + policy 管「安全上限」，兩者缺一不可。過期是抽樣近似、淘汰是抽樣近似、記憶體釋放未必還給 OS——把這三個「不精確」記在心裡，你對 Redis 記憶體行為的預期就會正確。
