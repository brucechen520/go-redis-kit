# Stage 4：高可用與擴展（HA & Scaling）

> 模組：`github.com/twteam/go-redis-kit`｜Go 1.26.1｜client：`github.com/redis/go-redis/v9`
> 對應 compose profiles：`sentinel`、`cluster`（`make sentinel-up` / `make cluster-up` / `make cluster-init`）

---

## 1. 本階段目標

前面幾個階段我們都跑在「單機 Redis」上。單機的問題很直接：

- **單點故障（SPOF）**：這台 Redis 掛了，整個服務的快取／鎖／計數器全部消失，讀寫直接 error。
- **容量天花板**：所有資料都在一台機器的記憶體裡，記憶體上限 = 資料上限。
- **吞吐天花板**：Redis 主執行緒是單執行緒處理命令，一台機器的 QPS 有物理上限。

本階段要能回答並動手驗證下面這些問題：

1. **主從複製（replication）** 到底怎麼把資料從 master 同步到 replica？全量與增量差在哪？
2. **Sentinel** 如何做到 master 掛掉時自動選出新 master，客戶端又怎麼「無感」切過去？
3. **Cluster** 如何把資料水平切分到多台機器、突破單機記憶體與吞吐上限？分片規則是什麼？
4. 三種方案（單機 / 主從+Sentinel / Cluster）各自的**取捨**與**適用場景**。
5. 用本 repo 的 `docker-compose.yml` 親手把 **Sentinel 故障轉移** 和 **Cluster slot 重定向** 跑一遍。
6. 用 go-redis 寫出 `NewFailoverClient` 與 `NewClusterClient` 的正確連線設定。

完成後你應該具備「幫一個服務挑 HA 方案」的判斷力，而不是背答案。

---

## 2. 主從複製（Master–Replica Replication）

主從複製是所有 HA 方案的基石：Sentinel 是在主從之上加自動故障轉移，Cluster 每個分片內部也是一組主從。

### 2.1 角色

- **master**：接收寫入（`SET`、`INCR`…），並把寫入「傳播」給所有 replica。
- **replica**（舊稱 slave）：只複製 master 的資料。預設 `replica-read-only yes`，**不接受寫入**（對 replica 寫會回 `READONLY You can't write against a read only replica.`）。

在本 repo 的 sentinel profile 裡：

- `master`：`redis-server --appendonly yes`（對外 `6380:6379`）
- `replica1` / `replica2`：`redis-server --replicaof master 6379`

`--replicaof master 6379` 就是啟動即宣告「我要複製名為 `master` 的節點的 6379」。

### 2.2 全量同步（Full Resync，RDB transfer）

replica 第一次連上 master（或斷線太久無法增量續傳）時，走**全量同步**：

```
1. replica 送 PSYNC ? -1  （我不知道 master 是誰、也沒有 offset）
2. master 回 +FULLRESYNC <replid> <offset>
3. master 執行 BGSAVE，fork 出子行程產生 RDB 快照
4. RDB 檔透過 socket 傳給 replica
5. 產生 RDB 這段期間 master 新收到的寫入，先暫存到「replication buffer」
6. replica 清空自己現有資料 → 載入 RDB
7. master 再把 buffer 裡累積的增量命令補送給 replica
```

重點與坑：

- 全量同步對 master 是**有成本**的：`BGSAVE` fork（copy-on-write，寫多時吃記憶體）＋磁碟 IO ＋網路傳輸整份資料。
- 大 key / 大資料集的全量同步可能耗時數秒到數十秒，期間 replication buffer 若被寫爆（`client-output-buffer-limit replica` 超限），master 會**斷開這個 replica**，replica 重連又觸發全量 → 陷入死循環。**別讓 replica 反覆全量。**
- Redis 支援 **diskless replication**（`repl-diskless-sync yes`）：RDB 不落地，直接從 fork 的子行程 socket 串到 replica，省磁碟。

### 2.3 增量同步（Partial Resync，replication backlog）

一旦全量完成，master 之後每個寫命令都會**即時**傳播給 replica（asynchronous replication，非同步，master 不等 replica ack 就回覆客戶端）。

如果 replica 只是**短暫**斷線（網路抖動、重啟幾秒），重連時不需要重來全量，走**增量同步**：

- master 維護一個環狀緩衝區 **replication backlog**（`repl-backlog-size`，預設 1MB），持續記錄最近傳播出去的命令 stream，並用 **replication offset** 標記進度。
- replica 重連時送 `PSYNC <replid> <offset>`（我上次收到 offset 這裡）。
- 若 replica 要的 offset **還在** backlog 範圍內 → master 回 `+CONTINUE`，只補送缺的那段。
- 若 offset 已經**被環狀覆蓋掉**（斷太久、backlog 太小）→ 只能退回 `+FULLRESYNC` 全量。

實務調參：斷線可能較久、寫入量大時，把 `repl-backlog-size` 調大（例如 64MB）能顯著減少不必要的全量。

### 2.4 PSYNC 一句話總結

`PSYNC` = Redis 2.8+ 的複製握手協議，一個命令同時支援兩種結果：

- `PSYNC ? -1` 或 offset 過期 → `FULLRESYNC`（全量）
- `PSYNC <replid> <offset>` 且 offset 命中 backlog → `CONTINUE`（增量）

`<replid>` 是 master 的 40 字元複製 ID，master 故障轉移換人後 replid 會變，用來判斷「你複製的還是不是同一條歷史」。

### 2.5 複製延遲（Replication Lag）成因

因為是**非同步**複製，replica 的資料永遠「慢 master 一點點」。延遲來源：

1. **網路 RTT / 頻寬**：跨可用區、跨機房更明顯。
2. **master 寫入尖峰**：傳播的命令 stream 變大。
3. **replica 端載入慢**：replica 也是單執行緒，若同時在跑重活（大 `KEYS`、`SORT`）追不上。
4. **大命令**：一個 `DEL bigkey` / `FLUSHALL` 在 replica 端也要花同樣時間執行。

觀測方式：master 上 `INFO replication` 看每個 replica 的 `offset`，和 master 的 `master_repl_offset` 相減就是落後的位元組數；或看 replica 的 `master_last_io_seconds_ago`。

### 2.6 讀寫分離的取捨

很自然的念頭：**寫走 master、讀分攤到 replica**，來擴展讀吞吐。但要清楚代價：

- ✅ 讀 QPS 可以水平擴展（多加 replica）。
- ❌ **讀到舊資料**：因為複製延遲，「剛寫完馬上讀」很可能讀不到（read-your-writes 不成立）。
- ❌ replica 掛了如果客戶端沒處理，讀會失敗。

判準：

- **能容忍最終一致** 的讀（首頁列表、推薦、統計面板）→ 讀 replica 沒問題。
- **要求 read-your-writes** 的讀（下單後查訂單、改完資料立刻回顯）→ **讀 master**，或這條路徑強制走 master。

> 記住這句：「讀 replica 有延遲，剛寫的可能讀不到。」這是後面「踩坑清單」和分散式鎖問題的根源。

---

## 3. Sentinel（哨兵）

主從複製解決了「資料有副本」，但**沒解決「master 掛了誰來頂」**——手動改 `replicaof`、改客戶端連線太慢。Sentinel 就是自動化這件事的控制平面。

### 3.1 四大職責

1. **監控（Monitoring）**：持續 ping master、replica、其他 sentinel，判斷是否存活。
2. **通知（Notification）**：節點狀態變化可透過 API / pub-sub 通知外部。
3. **自動故障轉移（Automatic Failover）**：master 判定客觀下線後，從 replica 中選一個提升為新 master，並讓其他 replica 改複製新 master。
4. **配置提供（Configuration Provider）**：客戶端**不直連 Redis**，而是先問 Sentinel「`mymaster` 現在的 master 位址是誰」。這是客戶端能無感切換的關鍵。

本 repo 起 **3 個 sentinel**（`sentinel1/2/3`），每個設定都是：

```
sentinel monitor mymaster master 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 10000
```

- `mymaster`：這組主從的邏輯名字（客戶端用它來查詢）。
- `master 6379`：初始 master 位址。
- 結尾的 `2`：**quorum**（見下）。

### 3.2 主觀下線 vs 客觀下線

- **SDOWN（Subjectively Down，主觀下線）**：**某一個** sentinel 在 `down-after-milliseconds`（這裡 5000ms）內 ping 不到 master，它自己認為 master 掛了。
- **ODOWN（Objectively Down，客觀下線）**：**至少 quorum 個** sentinel 都主觀認為 master 掛了。只有到 ODOWN 才會觸發故障轉移。

`quorum = 2` 表示要 3 個 sentinel 裡有 2 個都覺得 master 掛了，才算數。這能避免「單一 sentinel 自己網路有問題就誤判」。

### 3.3 選主流程（Failover）

1. 達到 ODOWN。
2. sentinel 之間用 **Raft-like** 演算法選出一個 **leader sentinel**（要拿到多數 sentinel 的授權，所以 sentinel 數量建議是**奇數且 ≥3**）來主導這次轉移。
3. leader 從存活的 replica 中**挑選新 master**，排序依據大致是：
   - 排除斷線 / 標記不健康的；
   - `replica-priority` 較低者優先（值越小越優先，0 表示永不當選）；
   - **複製 offset 最大**者優先（資料最新，丟最少）；
   - 前面都相同時比 runid（字典序）打破平手。
4. leader 對選中的 replica 送 `REPLICAOF NO ONE`（升為 master）。
5. 讓其餘 replica `REPLICAOF <新 master>`。
6. 更新配置，透過 pub-sub 頻道 `+switch-master` 廣播新 master 位址。

### 3.4 客戶端如何發現新 master

**客戶端連的是 Sentinel，不是 Redis。** go-redis 用 `NewFailoverClient`：

```go
rdb := redis.NewFailoverClient(&redis.FailoverOptions{
    MasterName:    "mymaster",
    SentinelAddrs: []string{"localhost:26379"}, // 可多個 sentinel
})
```

流程：client 先問 sentinel `SENTINEL get-master-addr-by-name mymaster` 拿到目前 master 位址再連上；同時**訂閱** sentinel 的 `+switch-master` 頻道，一旦收到切主通知，就把內部連線指到新 master。所以應用程式碼**不需要重啟、不需要改 IP**。

### 3.5 切主瞬間的寫入丟失窗口（務必理解）

因為複製是**非同步**的，故障轉移**一定有丟寫入的風險**：

```
t0  client 寫 SET x=1 到 master，master 回 OK（此時還沒複製到 replica）
t1  master 突然崩潰
t2  sentinel 選了「還沒收到 x=1」的 replica 當新 master
→   x=1 永遠消失了，即使 client 收到過 OK
```

這不是 bug，是非同步複製 + 自動轉移的本質。緩解手段：

- `min-replicas-to-write` / `min-replicas-max-lag`：master 在「存活且不落後太多的 replica 數 < N」時**拒絕寫入**，用可用性換一致性。
- 業務層對關鍵寫入做冪等 / 對帳補償，不要假設「回了 OK 就一定不丟」。

---

## 4. Cluster（叢集分片）

Sentinel 解決可用性，但**所有資料仍在一台 master 上**——記憶體與寫入吞吐沒被擴展。Cluster 用**水平分片（sharding）**解決容量與吞吐問題，同時每個分片自帶主從＋自動故障轉移。

### 4.1 16384 個 slot

Cluster 把 keyspace 切成固定的 **16384 個 hash slot**，每個 master 節點負責其中一段（連續或不連續的 slot 集合）。本 repo 6 節點、`--cluster-replicas 1`，`cluster-init` 後大致是 **3 master + 3 replica**，16384 個 slot 平均分給 3 個 master（約各 5461 個）。

key 落到哪個 slot 的計算：

```
slot = CRC16(key) mod 16384
```

（實務上 `redis-cli` 提供 `CLUSTER KEYSLOT <key>` 直接算給你看。）

為什麼是 16384 而不是更大？節點間 gossip 要交換 slot bitmap，16384 bit = 2KB，夠用又省頻寬；Redis 作者認為 cluster 節點數不會大到需要更多 slot。

### 4.2 MOVED / ASK 重定向

客戶端可能連到「不負責這個 key 的節點」，Cluster 用重定向告訴它該去哪：

- **MOVED**：slot **已經穩定**歸屬別的節點。回應形如 `MOVED 3999 127.0.0.1:7002`。聰明的 client（go-redis `ClusterClient`）收到後會**更新本地 slot→node 路由表**並重試，之後直接命中，不用每次被重定向。
- **ASK**：slot **正在遷移中**（reshard 進行到一半，部分 key 已搬到目標節點）。回應 `ASK 3999 127.0.0.1:7003`，client 這次**臨時**去目標節點，並先送一個 `ASKING` 命令。ASK 是**一次性**的，不更新路由表（因為遷移還沒完成）。

一句話：**MOVED = 永久搬好了（更新路由表）；ASK = 搬遷中臨時借道（不更新路由表）。**

### 4.3 Hash Tag：`{...}` 強制同 slot

跨 slot 的多 key 操作在 Cluster 是被禁止的（見 4.4）。若你**需要**多個 key 在同一節點（例如同一使用者的多個 key 要一起 `MGET` 或跑 Lua/事務），用 **hash tag**：

> 若 key 含有 `{...}`，Cluster **只用大括號裡的內容**去算 slot。

```
user:{42}:profile   → 只用 "42" 算 slot
user:{42}:orders    → 只用 "42" 算 slot  ← 兩者必落同一 slot
```

所以 `{42}` 相同的 key 一定同 slot、同節點，就能一起做多 key 操作。**代價**：同一個 tag 的資料全擠一個節點，tag 設計不當會造成**資料傾斜（hot slot）**。

### 4.4 跨 slot 多 key 的限制

Cluster 下，一條命令若牽涉**多個 key**，這些 key 必須**同屬一個 slot**，否則直接報錯：

```
(error) CROSSSLOT Keys in request don't hash to the same slot
```

受影響的常見操作：

- `MGET` / `MSET` / `DEL k1 k2 k3`（多 key）
- `SINTERSTORE`、`SUNIONSTORE` 等多 key 集合運算
- **`MULTI/EXEC` 事務**、**Lua 腳本**：裡面所有 key 也必須同 slot。

解法：要嘛保證這些 key 用同一 hash tag，要嘛把單一多 key 命令**拆成多個單 key 命令**（go-redis `ClusterClient` 對 `MGET` 之類會嘗試幫你按節點分組 pipeline，但事務/Lua 不會自動拆）。

### 4.5 擴縮容 reshard 概念

擴容 = 加節點 + 把一部分 slot（及其 key）搬過去；縮容相反：

```bash
# 加一個空節點進 cluster
redis-cli --cluster add-node <new_host:port> <existing_host:port>
# 把 N 個 slot 從某來源搬到目標節點（互動式或用參數）
redis-cli --cluster reshard <host:port>
# 縮容：先把節點的 slot 搬空，再 del-node
redis-cli --cluster del-node <host:port> <node-id>
```

reshard 是**逐 slot、逐 key 線上遷移**：遷移中的 slot 對讀寫用 ASK 重定向處理，所以**不停機**。要注意遷移期間路由抖動與額外負載。

### 4.6 Gossip 協議

Cluster **沒有中央協調者**，節點之間用 **gossip**（走 `cluster-bus`，通常是資料埠 +10000，例如 6379→16379）互相交換狀態：誰活著、誰負責哪些 slot、誰是誰的 master/replica。

- 節點週期性互 ping/pong，夾帶部分節點的健康資訊。
- 某節點若被**足夠多** master 標記為 `PFAIL`（可能故障）→ 升級為 `FAIL`（確認故障）並全網廣播。
- 該故障 master 的 replica 發起選舉（需拿到多數 master 投票）提升為新 master，完成**分片內**的自動故障轉移——這點和 Sentinel 類似，但**內建**在 cluster、不需要額外 sentinel 行程。

---

## 5. 三方案對比

| 面向 | 單機 Single | 主從 + Sentinel | Cluster |
|---|---|---|---|
| 容量上限 | 單機記憶體 | 單機記憶體（replica 只是副本，不加容量） | **多節點總和**（可水平擴展） |
| 寫吞吐 | 單機 | 單機（只有一個 master 收寫） | **多 master 分攤** |
| 讀吞吐 | 單機 | 可加 replica 分攤讀（有延遲） | 多節點分攤 |
| 可用性 | 無（SPOF） | 高（自動故障轉移） | 高（分片內自動故障轉移） |
| 資料一致性 | 強（就一份） | 最終一致（複製延遲、切主可能丟寫） | 最終一致（同上，且跨 slot 無事務） |
| 客戶端複雜度 | 最低 | 中（要用 FailoverClient） | 高（要用 ClusterClient，注意 CROSSSLOT） |
| 運維複雜度 | 最低 | 中（管 sentinel 群） | 最高（slot、reshard、gossip） |
| 多 key / 事務 / Lua | 無限制 | 無限制（都在一個 master） | **受限**（需同 slot / hash tag） |
| 適用場景 | 本地開發、快取可重建、非關鍵 | 需要高可用但**單機容量夠**（絕大多數線上服務） | 資料量或吞吐**超過單機**上限 |

**選型直覺**：先問「單機記憶體/吞吐夠不夠？」

- 夠 → **主從 + Sentinel**（複雜度低、可用性高，覆蓋 90% 場景）。
- 不夠 → **Cluster**，並接受多 key 操作的限制。
- 別為了「看起來很潮」直接上 Cluster，它的運維與程式限制是實打實的成本。

---

## 6. 踩坑清單

1. **Cluster 下不能隨意 `MGET` / `MSET` / 多 key `DEL`**
   跨 slot 會 `CROSSSLOT` 報錯。要多 key 一起操作，必須用 **hash tag** `{...}` 把它們綁到同 slot；但要小心 tag 造成熱點傾斜。

2. **Sentinel 切主會丟寫入 → 分散式鎖的致命坑**
   非同步複製 + 故障轉移的丟寫窗口，對**分散式鎖**特別危險：client A 在舊 master 拿到鎖（`SET lock ... NX`），還沒複製到 replica，master 崩潰、replica 升主，鎖「不見了」，client B 也拿到同一把鎖 → **兩個 client 同時持鎖**。這正是 Redlock 想解決、卻又被 Martin Kleppmann 質疑的問題。**單一 Redis（哪怕有 Sentinel）的鎖不是絕對安全的**，關鍵資源要靠 fencing token / DB 唯一約束兜底。（對照 `labs/05-distributed-lock`。）

3. **複製延遲導致讀不一致**
   讀 replica 時「剛寫的讀不到」。凡是 read-your-writes 的路徑，讀 master 或加版本/戳記校驗，別默默讀 replica。

4. **客戶端寫死 master IP**
   用 Sentinel 卻直接連 master 的 IP，等於白搭——切主後你連的還是舊死掉的 master。**一定用 `NewFailoverClient`（連 sentinel）或 `NewClusterClient`（連任一節點自動發現）**，讓 client 自己追蹤拓撲變化。

5. **replica 反覆全量同步**
   `repl-backlog-size` 太小或 `client-output-buffer-limit replica` 太緊，導致 replica 一斷線就退回全量、又把 master 拖累。調大 backlog、監控全量次數。

6. **sentinel 數量用偶數 / 只有 1 個**
   leader 選舉要多數，**奇數且 ≥3** 才有意義；只有 1 個 sentinel 本身就是 SPOF。

7. **以為 replica 能寫**
   `replica-read-only yes`（預設）下對 replica 寫會 `READONLY` 報錯。要寫就得走 master。

---

## 7. 動手實驗（用本 repo 的 compose）

> 前置：Docker Desktop 已啟動。所有指令在 repo 根目錄執行。

### 7.1 Sentinel：殺掉 master 看自動選主

**Step 1｜起環境**（1 master + 2 replica + 3 sentinel）：

```bash
make sentinel-up
docker compose --profile sentinel ps
```

**Step 2｜確認初始主從關係**：

```bash
# master 上看到 2 個 replica、role:master
docker compose --profile sentinel exec master redis-cli INFO replication

# 問 sentinel：現在 mymaster 的 master 是誰
docker compose --profile sentinel exec sentinel1 \
  redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster
```

**Step 3｜寫一筆資料，確認 replica 也有**：

```bash
docker compose --profile sentinel exec master   redis-cli SET hello world
docker compose --profile sentinel exec replica1 redis-cli GET hello   # → "world"
```

**Step 4｜殺掉 master 容器**，模擬 master 崩潰：

```bash
docker compose --profile sentinel kill master
```

**Step 5｜等約 5~15 秒**（`down-after-milliseconds=5000` + 選舉時間），觀察故障轉移：

```bash
# 再問 sentinel，master 位址應該已經變成 replica1 或 replica2
docker compose --profile sentinel exec sentinel1 \
  redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster

# 看 sentinel 日誌裡的 +sdown / +odown / +switch-master 事件
docker compose --profile sentinel logs sentinel1 | grep -E '\+sdown|\+odown|\+switch-master|+failover'

# 新 master 上 role 應為 master，並帶著剩下的 replica
docker compose --profile sentinel exec replica1 redis-cli INFO replication
```

**觀察重點**：`+switch-master mymaster <old_ip> 6379 <new_ip> 6379` 這行就是切主的證據，客戶端（若用 FailoverClient）此刻會自動改連新 master。

**收尾**：

```bash
make sentinel-down
```

### 7.2 Cluster：看 MOVED 重定向與 hash tag

**Step 1｜起 6 節點並初始化 slot**：

```bash
make cluster-up
make cluster-init     # redis-cli --cluster create ... --cluster-replicas 1 --cluster-yes
```

**Step 2｜看叢集拓撲與 slot 分配**：

```bash
docker compose --profile cluster exec node1 redis-cli CLUSTER NODES
docker compose --profile cluster exec node1 redis-cli CLUSTER SLOTS
```

**Step 3｜不加 `-c`，故意 SET 到「不歸這個節點」的 key，看 MOVED**：

```bash
# 先算幾個 key 各自的 slot
docker compose --profile cluster exec node1 redis-cli CLUSTER KEYSLOT foo
docker compose --profile cluster exec node1 redis-cli CLUSTER KEYSLOT bar

# 不用 cluster 模式連線，SET 一個不屬於 node1 的 key → 會回 MOVED
docker compose --profile cluster exec node1 redis-cli SET foo 1
# (error) MOVED 12182 <ip>:6379   ← 告訴你該去哪個節點
```

**Step 4｜加 `-c`（cluster 模式）自動跟隨重定向**：

```bash
docker compose --profile cluster exec node1 redis-cli -c SET foo 1   # OK，client 自動轉到正確節點
docker compose --profile cluster exec node1 redis-cli -c SET bar 2   # OK
docker compose --profile cluster exec node1 redis-cli -c GET foo
```

**Step 5｜體驗 CROSSSLOT 與 hash tag**：

```bash
# foo 與 bar 很可能不同 slot → 多 key MGET 直接報錯
docker compose --profile cluster exec node1 redis-cli -c MGET foo bar
# (error) CROSSSLOT Keys in request don't hash to the same slot

# 用 hash tag 讓兩個 key 綁到同一 slot
docker compose --profile cluster exec node1 redis-cli CLUSTER KEYSLOT 'user:{42}:a'
docker compose --profile cluster exec node1 redis-cli CLUSTER KEYSLOT 'user:{42}:b'
# ↑ 兩個 slot 相同

docker compose --profile cluster exec node1 redis-cli -c MSET 'user:{42}:a' 1 'user:{42}:b' 2
docker compose --profile cluster exec node1 redis-cli -c MGET 'user:{42}:a' 'user:{42}:b'   # OK！
```

**收尾**：

```bash
make cluster-down
```

---

## 8. Go 範例

> client：`github.com/redis/go-redis/v9`。以下片段可放進 `labs/` 底下跑（連線位址對應 compose 對外埠：sentinel 的 sentinel 埠 `26379`、cluster 的 `7001~7006`）。

### 8.1 Sentinel：`NewFailoverClient`

```go
package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName: "mymaster", // 必須和 sentinel monitor 的名字一致
		SentinelAddrs: []string{ // 列出所有 sentinel，任一存活即可
			"localhost:26379",
			// "localhost:26380",
			// "localhost:26381",
		},
		// 讀寫都走 master（預設）。想讀寫分離看下面 8.3。
		DialTimeout:  0,
		PoolSize:     10,
		// SentinelUsername / SentinelPassword / Password 視需要設定
	})
	defer rdb.Close()

	if err := rdb.Set(ctx, "hello", "world", 0).Err(); err != nil {
		panic(err)
	}
	val, err := rdb.Get(ctx, "hello").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("hello =", val)
	// 這期間就算 master 故障轉移，client 會自動追到新 master，程式不用改。
}
```

**讀寫分離版**（讓讀可以打到 replica，接受複製延遲）：

```go
rdb := redis.NewFailoverClient(&redis.FailoverOptions{
	MasterName:    "mymaster",
	SentinelAddrs: []string{"localhost:26379"},

	// 只讀命令路由到 replica（若無可用 replica 會退回 master）
	ReplicaOnly: false, // true 則所有命令都只走 replica（少用）
	RouteByLatency: true, // 從 master+replica 中挑「延遲最低」的做只讀
	// 或 RouteRandomly: true —— 只讀命令在節點間隨機分攤
})
```

> 注意：go-redis 的 `FailoverOptions` 也支援直接回傳一個會自動路由讀寫的 client（`NewFailoverClusterClient`），底層把 master 當寫節點、replica 當讀節點。是否啟用讀分離，取決於你的路徑能不能容忍 stale read（見 §2.6）。

### 8.2 Cluster：`NewClusterClient`

```go
package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{ // 給幾個「種子」節點即可，client 會自己發現整個拓撲
			"localhost:7001",
			"localhost:7002",
			"localhost:7003",
		},
		// 只讀命令允許打到 replica（預設只打 master）
		ReadOnly:       true,
		RouteByLatency: true, // 只讀時挑延遲最低的節點（自動含 ReadOnly 語意）
		// RouteRandomly: true, // 只讀時隨機分攤到 master/replica

		PoolSize:     10,
		MaxRedirects: 3, // 收到 MOVED/ASK 時最多重試幾次
	})
	defer rdb.Close()

	// 單 key：client 依 CRC16 自動路由到正確節點，MOVED 也會自動處理
	if err := rdb.Set(ctx, "foo", "1", 0).Err(); err != nil {
		panic(err)
	}
	fmt.Println(rdb.Get(ctx, "foo").Val())

	// 多 key：務必同 slot，否則 CROSSSLOT。用 hash tag 綁定：
	if err := rdb.MSet(ctx,
		"user:{42}:a", "1",
		"user:{42}:b", "2",
	).Err(); err != nil {
		panic(err)
	}
	vals, err := rdb.MGet(ctx, "user:{42}:a", "user:{42}:b").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("mget =", vals)

	// 對每個 master 節點跑一遍（例如清 key、統計）
	_ = rdb.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
		return m.Ping(ctx).Err()
	})
}
```

### 8.3 `RouteByLatency` / `ReadOnly` 說明

- **`ReadOnly`（ClusterClient）**：預設 `false`，所有命令（含讀）都打到負責該 slot 的 **master**。設 `true` 後，**只讀命令**可以打到該 slot 的 **replica**，用來分攤讀吞吐——代價是可能讀到 stale 資料。
- **`RouteByLatency`**：在候選節點（master + 其 replica）中，優先選 **RTT 最低** 的那個處理只讀命令；隱含開啟 ReadOnly 行為。適合多可用區、想就近讀。
- **`RouteRandomly`**：只讀命令在候選節點間**隨機**挑，做最簡單的負載打散。
- 三者只影響**只讀命令**的路由；**寫命令永遠走 master**（Cluster 保證）。
- FailoverClient 的 `RouteByLatency` / `RouteRandomly` / `ReplicaOnly` 概念一致，只是候選是「master + sentinel 已知的 replica」。

> 一致性提醒：一旦開了讀分離（`ReadOnly` / `RouteByLatency` / `RouteRandomly`），你就明確接受了「剛寫可能讀不到」。read-your-writes 的路徑不要開。

---

## 9. 專案流程：為服務挑 HA 方案的決策步驟

按順序回答，就能收斂到方案：

1. **這份資料掉了會怎樣？**
   - 純快取、可從 DB 重建、掉了只是慢一點 → 可用性要求低，**單機**都行（本地開發也用單機）。
   - 掉了會影響業務（session、鎖、計數）→ 需要 HA，往下走。

2. **資料量與吞吐能不能塞進單機？**
   估算峰值資料量（含成長）與峰值 QPS，對照單機記憶體與單執行緒吞吐。
   - 塞得下 → **主從 + Sentinel**（首選，複雜度可控）。
   - 塞不下（記憶體或寫吞吐超上限） → **Cluster**。

3. **程式碼受得了 Cluster 的限制嗎？**
   盤點是否大量用 `MGET`/`MSET`/多 key 事務/Lua 跨 key。
   - 用很多且難改 → 重新評估：能否分 key、用 hash tag，或先垂直擴容拖延上 Cluster。
   - 可接受 → Cluster + 規範 hash tag 設計，避免熱點。

4. **一致性要求？**
   - 需要 read-your-writes → 讀寫都走 master，別開讀分離。
   - 能容忍最終一致 → 開 `ReadOnly` / `RouteByLatency` 擴讀。
   - 關鍵互斥（鎖）→ 不能只靠 Redis 保正確性，加 fencing token / DB 唯一約束（見踩坑 #2）。

5. **落地與驗證**
   - 客戶端一律用 `NewFailoverClient` / `NewClusterClient`，**禁止寫死節點 IP**。
   - 上線前用本階段的實驗手法**真的殺一次 master**，確認故障轉移時間與應用行為。
   - 監控：複製延遲（offset 差）、故障轉移次數、全量同步次數、Cluster slot 覆蓋率。

---

## 10. 練習題 + 檢查點 + 延伸閱讀

### 練習題

1. 用 `INFO replication` 觀察本 repo sentinel 環境中 master 與 replica 的 `master_repl_offset` / replica `offset`，寫一段話解釋你看到的 lag。
2. 手動觸發一次故障轉移（`docker compose --profile sentinel kill master`），量測從 kill 到 sentinel 回報新 master 大約幾秒，並解釋這個時間和 `down-after-milliseconds` / `failover-timeout` 的關係。
3. 在 Cluster 找出兩個**不同 slot** 的 key，證明 `MGET` 會 `CROSSSLOT`；再用 hash tag 讓它們同 slot 並成功 `MGET`。用 `CLUSTER KEYSLOT` 佐證。
4. 寫一支 Go 小程式：用 `NewFailoverClient` 每秒寫一次遞增計數，過程中 kill master，觀察是否有寫入丟失或短暫報錯，並記錄恢復耗時。
5. 說明為什麼「Sentinel + 分散式鎖」在切主時可能讓兩個 client 同時持鎖，並提出一種兜底設計。

### 檢查點（能講清楚就算過關）

- [ ] 能說明全量同步（RDB transfer）與增量同步（backlog）觸發條件與差異，並解釋 `PSYNC` 的兩種結果。
- [ ] 能說明複製為何是非同步、以及它如何造成讀延遲與切主丟寫。
- [ ] 能畫出 Sentinel 的 SDOWN → ODOWN → 選 leader → 選新 master → switch-master 流程，並解釋 quorum 與奇數 sentinel。
- [ ] 能算出一個 key 的 slot 概念（CRC16 % 16384），並解釋 MOVED 與 ASK 的差別。
- [ ] 能說出 Cluster 跨 slot 多 key 的限制與 hash tag 解法及其熱點風險。
- [ ] 能寫出 `NewFailoverClient` 與 `NewClusterClient` 的正確設定，並解釋 `ReadOnly` / `RouteByLatency` 的作用與代價。
- [ ] 能用「資料重要性 → 容量/吞吐 → 程式限制 → 一致性」的順序為一個服務選型。

### 延伸閱讀

- Redis 官方文件：Replication、Sentinel、Cluster Specification、Cluster Tutorial（redis.io/docs）。
- go-redis 文件：`FailoverOptions`、`ClusterOptions`（含 `ReadOnly` / `RouteByLatency` / `RouteRandomly`）。
- Martin Kleppmann,「How to do distributed locking」與 antirez 的回應——理解 Redis 鎖在故障轉移下的一致性爭議（對照踩坑 #2 與 `labs/05-distributed-lock`）。
- Redis `INFO replication`、`CLUSTER NODES` / `CLUSTER SLOTS` / `CLUSTER KEYSLOT` 指令參考。
