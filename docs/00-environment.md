# Stage 0：環境建置與心智模型

> 這是 Redis 精通學習路線的第 0 站。目標不是背指令，而是先在腦中建立一張「Redis 是什麼、為什麼快、為什麼會被一條指令拖垮」的地圖，並把可以隨時開火的實驗環境準備好。後面每一個主題（string / hash / list / sorted set / stream / pub-sub / 分散式鎖 / cluster …）都會回頭用到這一頁建立的直覺與工具。

---

## 1. 本階段目標 + 前置知識

### 1.1 學完這一站你應該能夠

- 說得出 Redis 為什麼是**單線程 + 記憶體資料庫**，以及這個設計帶來的「快」與「危險」。
- 用 `docker` 或本 repo 的 `make single-up` 在 30 秒內起一個可用的 Redis 7。
- 熟練用 `redis-cli` 連線、查資料、看編碼（`OBJECT ENCODING`）、看記憶體（`MEMORY USAGE`）、看慢查詢（`SLOWLOG`）。
- 親手重現「一條慢指令阻塞整個 Redis」的現象，並解釋原因。
- 用 Go（`go-redis/v9`）寫出第一支能 `PING` 通、能讀寫 key 的程式，並說得出連線池預設值的意義。
- 知道日後遇到效能/記憶體問題時，該從哪些觀測工具下手。

### 1.2 前置知識（有最好，沒有也能跟）

| 需要的能力 | 用在哪裡 | 不熟怎麼補 |
| --- | --- | --- |
| 基本命令列操作 | 起 docker、跑 `redis-cli` | 會 `cd` / `ls` / 環境變數即可 |
| Docker 基礎 | 起 Redis 容器 | 只需要會 `docker run` / `docker compose` |
| Go 基礎語法 | 寫 demo 程式 | 會 struct / error 處理 / `go run` 就夠 |
| TCP / client-server 概念 | 理解連線池、RESP | 知道「client 送 request，server 回 response」即可 |

本 repo 環境：模組 `github.com/twteam/go-redis-kit`，Go **1.26.1**，客戶端函式庫 `github.com/redis/go-redis/v9`，Redis 伺服器版本 **7.x**。

---

## 2. Redis 心智模型

在打任何指令之前，先把三件事刻進腦子：**單線程 event loop**、**記憶體資料庫**、**RESP 協議**。這三點解釋了 Redis 幾乎所有的「快」與「雷」。

### 2.1 單線程 event loop：為什麼快、為什麼 O(N) 指令會阻塞全庫

Redis 處理**指令的核心邏輯是單線程**的（Redis 6 之後 I/O 讀寫可多線程，但「執行指令」這一步仍是單線程序列化的）。所有 client 送來的指令會被排進**同一條佇列**，由一個 event loop 一條一條地執行。

```
client A ─┐
client B ─┼─▶ [ 單一 event loop ] ─▶ 一次執行一條指令，執行完才處理下一條
client C ─┘
```

#### 常見混淆：「I/O 讀寫多線程」的「讀寫」不是 GET/SET 的讀寫

初學最容易卡在這句：「I/O 讀寫可多線程，但執行指令是單線程」——那 `GET`/`SET` 這種讀寫資料難道不算指令？

關鍵是「讀寫」這個詞被兩種完全不同的東西共用了：

| 詞 | 指的是 | 動到共享資料？ | 線程模型 |
| --- | --- | --- | --- |
| **I/O 讀寫** | 從 **socket** 收 client 送來的 bytes、把回應 bytes 寫回 socket（純網路搬運 + 協議解析） | 否 | Redis 6+ 可多線程（`io-threads`） |
| **指令執行** | 解析出的 `GET k` / `SET k v` 真正去動記憶體裡的 `dict`（key-value 表） | 是 | 永遠單線程 |

「I/O 讀寫」搬的是**網路上的 byte**，不是操作你的 key；它跟 `GET`/`SET` 的「讀寫資料」是**不同階段**。文件那句話的「讀寫」講的是前者，名詞剛好撞名，才讓人混淆。

**一條指令的生命週期（分階段看就懂了）：**

```
client ──TCP──▶ [1 讀 socket bytes] ─▶ [2 解析 RESP] ─▶ [3 執行指令] ─▶ [4 序列化回應] ─▶ [5 寫 socket bytes]
                └────────── 可多線程 (io-threads) ──────┘        └單線程┘         └───── 可多線程 ─────┘
                          搬 byte / parse，各連線互不干擾         動共享 dict        搬 byte，回寫 socket
```

- **階段 1、2、5**：搬網路 byte + parse，各 client 之間**沒有共享狀態** → 可平行 → Redis 6 開 `io-threads` 加速。
- **階段 3**：真的去 `dict` 拿/改 key，動的是**所有 client 共享的同一張記憶體表** → 必須序列化 → **只有 main thread 一條一條做**。

**為什麼階段 3 非單線程不可？** 所有 key 存在同一個 hash table。若多線程同時 `INCR` 同一 key 就會 race，得加鎖。Redis 乾脆**不加鎖**，改用單線程序列執行 → 天生原子、無鎖、無 context switch。這正是它又快又簡單的根。代價就是上面那條鐵律：一個慢指令卡在階段 3，後面全部指令一起等。

**類比**：餐廳有很多**服務生**收單/上菜（I/O 讀寫，平行），但只有**一個廚師**一次炒一道（指令執行，單線程）。服務生再多，菜還是廚師一道道出；廚師卡在一道大菜（慢指令），全部客人一起等。

所以「讀寫多線程」解決的是「連線多、每筆資料大時 socket I/O 成為瓶頸」；「執行單線程」保證「資料操作原子、無鎖」。兩者不衝突：**搬 byte 可平行，動資料要排隊**。

**為什麼這樣反而快？**

- **沒有鎖、沒有 context switch、沒有競態**：多線程資料庫要花大量成本在加鎖、同步、cache 一致性上；Redis 直接把這些成本歸零。
- **資料在記憶體、指令又極短**：單條指令通常是奈秒到微秒等級，序列化執行完全跟得上。
- **每條指令天然是原子的**：因為同一時間只有一條指令在跑，你不需要 transaction 就能保證 `INCR` 不會被別人插隊。這是 Redis 能做計數器、分散式鎖的基礎。

**為什麼危險？——一條慢指令會卡住所有人**

正因為是單線程序列執行，只要**有一條指令跑很久，後面所有 client 全部一起等**。這就是 Redis 最重要、最反直覺的一條鐵律：

> **Redis 沒有「只慢我這一條」這種事。慢的是整台伺服器。**

哪些指令會慢？主要是**時間複雜度 O(N) 且 N 很大**的指令，N 是要掃描/處理的元素數量：

| 危險指令 | 複雜度 | 為什麼危險 |
| --- | --- | --- |
| `KEYS *` | O(N)，N = 全庫 key 數 | 掃描整個 keyspace，百萬 key 直接卡數百毫秒 |
| `FLUSHALL` / `FLUSHDB`（同步） | O(N) | 一次刪光所有 key，期間全阻塞 |
| `SMEMBERS` / `HGETALL` / `LRANGE 0 -1` | O(N)，N = 集合大小 | 大集合一次全撈，序列化 + 傳輸都卡 |
| `SORT` 大 list | O(N + M·log M) | 排序整個集合 |
| `DEBUG SLEEP` | O(1) 但真的睡 | 教學用，直接示範阻塞（見第 5 節） |

**對策先劇透**：用 `SCAN`（游標分批）取代 `KEYS`、用 `HSCAN`/`SSCAN` 取代 `HGETALL`/`SMEMBERS`、`FLUSHALL ASYNC` 背景刪除、避免對大集合做全量操作。這些後面各主題會細講，這裡先建立「O(N) 大 N = 全庫災難」的警覺。

### 2.1.1 速查：原子性 / 複雜度 / 何時算慢（面試高頻）

**哪些操作是原子的？** 因為單線程序列執行，**每一條「單一指令」都是原子的**——`INCR`/`GETSET`/`GETDEL`/`SETNX`/`MSET`/`LPUSH`/`ZADD`… 全部，包括帶多動作的 `SET k v NX EX 30`（設值+條件+過期一次做完）。**不原子的是「多條指令的組合」**：

| 方式 | 原子？ | 說明 |
| --- | --- | --- |
| 單一指令 | ✅ | 天生原子 |
| Lua（`EVAL`） | ✅ | 腳本內多指令原子執行（`INCR`+`EXPIRE` 綁定用這個） |
| `MULTI`/`EXEC` | ✅* | 佇列後不中斷執行，但**無回滾**（指令錯照跑後面） |
| Pipeline | ❌ | 只是批次減 RTT，**不保證原子** |

> 經典坑：`GET` 再 `SET`、`INCR` 再 `EXPIRE`、`GET` 再 `DEL` 都是**兩條指令 → 不原子**。改用單指令（`SET EX`、`GETDEL`）或 Lua。

**怎麼知道某指令複雜度？** 權威來源是官方每個指令頁的 **"Time complexity"**（redis.io/commands）。不查也能猜的心法：

| 動作型態 | 複雜度 | 例 |
| --- | --- | --- |
| 單一元素存取 | **O(1)** | `GET`/`SET`/`INCR`/`HGET`/`HSET`/`SADD`/`LPUSH`/`ZADD` |
| 有序集合定位/範圍 | **O(log N)** ~ O(log N + M) | `ZADD`/`ZRANK`/`ZRANGEBYSCORE` |
| 掃整個集合/keyspace | **O(N)** | `KEYS`/`SMEMBERS`/`HGETALL`/`LRANGE 0 -1`/`SINTER` |

命名暗示：名字帶「範圍/全部」的（`*`、`RANGE`、`GETALL`、`MEMBERS`、`KEYS`）多半 O(N)。

**什麼複雜度算「慢」？** 看的是 **O(N) 的 N 有多大**，不是 O 幾：

| 複雜度 | 慢嗎 |
| --- | --- |
| O(1) | 永遠不慢 |
| O(log N) | 幾乎不慢（N 十億也才 ~30 步） |
| O(N)、**N 小且有界** | 不慢（如 3 元素的 set） |
| O(N)、**N 大或會無限成長** | **慢！阻塞全庫** |

判準一句話：**這操作處理的元素數（N）會不會隨資料/流量無限長大？** 會 → 危險（今天快、資料一多就爆）；固定小範圍 → 安全。實際驗證用 `SLOWLOG`（純 server 耗時）+ `--latency`（量阻塞）。

### 2.2 記憶體資料庫特性

Redis 的資料**主要活在 RAM**，這是它快的另一半原因，也帶來一組要理解的特性：

- **快，但也貴且有上限**：記憶體比磁碟快幾個數量級，但也比磁碟貴。你必須關心「一個 key 到底吃多少記憶體」（用 `MEMORY USAGE`），以及整台的 `maxmemory` 上限。本 repo 的 single profile 就設了 `--maxmemory 256mb`。
- **淘汰策略（eviction）**：記憶體到頂時，Redis 依 `maxmemory-policy` 決定要不要淘汰、淘汰誰。本 repo 用 `allkeys-lru`（記憶體滿時，對所有 key 用近似 LRU 淘汰最久沒用到的）。其他常見的還有 `noeviction`（滿了就寫入報錯）、`volatile-ttl` 等。
- **持久化（persistence）**：記憶體資料斷電會消失，Redis 提供兩種落盤機制補救：
  - **RDB**：定時做整份快照，檔案小、恢復快，但可能掉最後一段資料。
  - **AOF（append-only file）**：把每個寫指令追加寫進 log，資料更不易遺失。本 repo 各容器都開了 `--appendonly yes`。
  - 兩者可同時開；細節屬於後面章節，這裡只要知道「記憶體資料庫不等於資料一定會不見」。
- **編碼會隨資料自動變形**：同一種資料型別，Redis 底層會依大小/內容選不同的記憶體編碼（例如小 hash 用 `listpack`，變大後轉 `hashtable`）。這直接影響記憶體佔用與效能，是我們之後反覆用 `OBJECT ENCODING` 觀察的重點。

### 2.3 RESP 協議簡介

Redis client 與 server 之間講的是 **RESP（REdis Serialization Protocol）**，一個純文字、以 `\r\n` 分隔、極簡的協議。理解它有兩個好處：debug 時看得懂 `MONITOR` / 抓包內容，也能體會為什麼 Redis「解析成本低所以快」。

RESP 用第一個位元組標示型別：

| 前綴 | 型別 | 範例（`\r\n` 以換行示意） |
| --- | --- | --- |
| `+` | 簡單字串 Simple String | `+OK` |
| `-` | 錯誤 Error | `-ERR unknown command` |
| `:` | 整數 Integer | `:1000` |
| `$` | 大量字串 Bulk String | `$5` 換行 `hello` |
| `*` | 陣列 Array | `*2` … |

**client 送出的指令其實也是一個 Array of Bulk Strings。** 例如你在 cli 打 `SET name Redis`，實際送出的位元組是：

```
*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nRedis\r\n
```

翻譯：`*3` = 這是 3 個元素的陣列 → `$3 SET`（長度 3 的字串 SET）→ `$4 name` → `$5 Redis`。server 回一個 `+OK\r\n`。就這麼簡單，沒有 JSON、沒有 XML、沒有複雜握手，所以解析飛快。RESP3（Redis 6+）加了更多型別（map、set、double 等），`go-redis/v9` 支援協商，但心智模型不變。

---

## 3. 環境建置

三種方式，任選其一。**最快是 docker 一行，最完整是本 repo 的 make。**

### 3.1 最快：docker 一行起單機

```bash
docker run -d --name redis-lab -p 6379:6379 redis:7
```

- `-d`：背景執行。
- `--name redis-lab`：給容器取名，之後好 `docker stop redis-lab` / `docker rm redis-lab`。
- `-p 6379:6379`：把容器的 6379 映射到本機 6379（Redis 預設 port）。

確認起來了：

```bash
docker ps                          # 看到 redis-lab 在跑
docker exec -it redis-lab redis-cli PING   # 回 PONG 就成功
```

### 3.2 推薦：用本 repo 的 make（docker-compose 已備好三種 profile）

本 repo 的 `docker-compose.yml` 用 **profile** 把三種拓樸隔開，一次只起一種，並附帶 **RedisInsight** 圖形化工具（port **5540**）。

```bash
make single-up      # 起「單機 redis + RedisInsight(5540)」← Stage 0 用這個
make single-down    # 收掉

make sentinel-up    # 起 1 master + 2 replica + 3 sentinel（之後高可用章節用）
make cluster-up     # 起 6 節點 cluster
make cluster-init   # cluster 起完要再跑這個做 slot 分配
```

`make single-up` 背後其實是 `docker compose --profile single up -d`，它會啟動兩個服務：

| 服務 | 說明 | 對外 port |
| --- | --- | --- |
| `redis` | Redis 7，開了 AOF、`maxmemory 256mb`、`allkeys-lru` | 6379 |
| `redisinsight` | RedisInsight 圖形化管理介面 | 5540 |

起好後打開瀏覽器連 `http://localhost:5540`，新增連線就能用 GUI 瀏覽 key、看記憶體分析、跑指令。

> #### ⚠️ 連線坑：RedisInsight 要用「服務名」不是 localhost
>
> RedisInsight 跑在 **容器裡**，它的 `localhost` / `127.0.0.1` 指的是**它自己那個容器**，不是你的 Mac、也不是 redis 容器。所以連線 Host **不能填 `localhost` / `127.0.0.1`**，會連不到。
>
> 兩個容器在同一條 compose network 上，容器之間要用 **compose 服務名** 互連：
>
> | 欄位 | 填 | 備註 |
> | --- | --- | --- |
> | Host | `redis` | ← compose 裡的服務名，**不是** localhost |
> | Port | `6379` | 容器內部 port |
> | Username | 留空 | 填 `default` 反而可能觸發 `AUTH` 錯誤 |
> | Password | 留空 | 這份 config 沒設密碼 |
>
> 或用 connection URL 一行貼：`redis://redis:6379`（別留 `default@`、別用 `127.0.0.1`）。
>
> 對照：**你（Mac）** 用 `redis-cli` 連是 `localhost:6379`（compose 有 `6379:6379` 把 port 映射到 host）；但**容器對容器**沒經過 host 映射，要走內部網路的服務名 `redis:6379`。備案可用 `host.docker.internal:6379`（Docker Desktop 讓容器連回 host 的特殊 DNS），但服務名 `redis` 最乾淨。
>
> 連不上時先刪掉之前用 `127.0.0.1` 建的那筆紅色連線，重新用 `redis` 建。

各 profile 對外 port 一覽（避免日後搞混）：

| profile | 節點 | 對外 port |
| --- | --- | --- |
| single | redis | 6379 |
| single | redisinsight | 5540 |
| sentinel | master | 6380 |
| sentinel | sentinel1 | 26379 |
| cluster | node1–node6 | 7001–7006 |

### 3.3 本機直接裝（不想用 docker 時）

```bash
# macOS
brew install redis
redis-server                 # 前景起一個預設 config 的 redis
redis-cli PING               # 另開一個終端機測試

# Debian/Ubuntu
sudo apt-get install redis-server
```

> 建議 Stage 0 就用 `make single-up`，因為它跟後面章節共用同一套 compose，環境一致、少踩坑。

---

## 4. redis-cli 上手

`redis-cli` 是你最常用的工具。若用 docker，可以 `docker exec -it redis-lab redis-cli`，或本機裝了就直接 `redis-cli`。

### 4.1 連線與基本查詢

```bash
redis-cli                      # 連本機 6379，進入互動模式
redis-cli -h 127.0.0.1 -p 6379 # 指定 host / port
redis-cli -n 1                 # 連到 DB 1（Redis 預設有 0–15 共 16 個邏輯 DB）
redis-cli PING                 # 一次性指令，回 PONG

# 進互動模式後：
127.0.0.1:6379> SET name Redis
OK
127.0.0.1:6379> GET name
"Redis"
127.0.0.1:6379> TTL name       # 剩餘存活秒數，-1=永不過期，-2=不存在
(integer) -1
127.0.0.1:6379> EXPIRE name 60 # 設 60 秒後過期
(integer) 1
127.0.0.1:6379> TYPE name      # 看資料型別
string
127.0.0.1:6379> DEL name       # 刪除
(integer) 1
127.0.0.1:6379> DBSIZE         # 目前 DB 有幾個 key
(integer) 0
```

### 4.2 觀測用指令（Stage 0 的重點，之後天天用）

**`INFO`——伺服器全景**

```bash
127.0.0.1:6379> INFO            # 一大坨，分很多 section
127.0.0.1:6379> INFO server     # 版本、OS、執行時間
127.0.0.1:6379> INFO memory     # used_memory、maxmemory、碎片率
127.0.0.1:6379> INFO stats      # 每秒指令數、命中率、被拒連線數
127.0.0.1:6379> INFO clients    # 連線數、阻塞中的 client 數
127.0.0.1:6379> INFO keyspace   # 每個 DB 有幾個 key、幾個有 TTL
```

**`OBJECT ENCODING`——看底層編碼（超級重要）**

同一型別會依大小自動切換編碼，這決定記憶體與效能。之後每個資料型別章節都會用它驗證「什麼時候會從省記憶體的編碼變成通用編碼」。

```bash
127.0.0.1:6379> SET n 12345
127.0.0.1:6379> OBJECT ENCODING n      # "int"（純數字用整數編碼）
127.0.0.1:6379> SET s "hello world foo"
127.0.0.1:6379> OBJECT ENCODING s      # "embstr" 或 "raw"（看長度）
127.0.0.1:6379> RPUSH mylist a b c
127.0.0.1:6379> OBJECT ENCODING mylist # "listpack"（小 list）
```

**`MEMORY USAGE`——單一 key 吃多少記憶體**

```bash
127.0.0.1:6379> MEMORY USAGE name      # 回 bytes，含 key/value/overhead
127.0.0.1:6379> MEMORY DOCTOR          # Redis 幫你診斷記憶體健康
```

**`DEBUG SLEEP`——製造阻塞（第 5 節實驗用）**

```bash
127.0.0.1:6379> DEBUG SLEEP 3          # 讓 server 睡 3 秒（阻塞所有人）
```

**`MONITOR`——即時看每一條進來的指令**

```bash
127.0.0.1:6379> MONITOR
OK
1720358400.123456 [0 127.0.0.1:52344] "SET" "name" "Redis"
# 會即時串流所有 client 打的指令，debug 神器，但會拖效能（見第 9 節）
```

**`SLOWLOG`——慢查詢紀錄**

Redis 會把執行時間超過 `slowlog-log-slower-than`（預設 10000 微秒 = 10ms）的指令記下來。

```bash
127.0.0.1:6379> CONFIG GET slowlog-log-slower-than   # 看門檻（微秒）
127.0.0.1:6379> CONFIG SET slowlog-log-slower-than 0 # 設 0 = 記錄所有指令（實驗用）
127.0.0.1:6379> SLOWLOG GET 10                       # 看最近 10 筆慢查詢
127.0.0.1:6379> SLOWLOG LEN                           # 目前有幾筆
127.0.0.1:6379> SLOWLOG RESET                         # 清空
```

每筆紀錄有 **6 個欄位**，最重要的是第 3 欄（耗時，微秒）：

```
1) 1) (integer) 13              # [1] 唯一 ID
   2) (integer) 1783448362      # [2] Unix 時間戳（何時發生）
   3) (integer) 5               # [3] ★耗時（微秒 µs）★ ← 你要找的就是這欄
   4) 1) "INCR"                 # [4] 指令 + 參數
      2) "counter"
   5) "127.0.0.1:60126"         # [5] client 來源
   6) ""                        # [6] client name
```

看的重點永遠是**第 3 欄 µs 最大的那幾筆** + 第 4 欄是什麼指令 → 揪出誰拖垮 server。密碼相關指令會顯示 `AUTH (redacted)`（自動遮蔽）。

三個實測踩過的坑：

1. **門檻別設 0（會被灌爆）**：`slowlog-log-slower-than 0` 記錄「所有指令」，連 Lua 腳本內部呼叫（如迴圈裡的每一次 `SET`，client 欄位為空）都會逐筆記進來，瞬間洗掉有用的紀錄。實驗看完就改回 `10000`（10ms）只留真慢的。
2. **只保留最近 128 筆**（`slowlog-max-len` 預設 128）：`SLOWLOG GET 999999` 也只回 128 筆，舊的被擠掉。它是「最近 N 筆慢指令」的滑動視窗，別靠它存歷史。
3. **SLOWLOG 是純 server 執行時間**：不含把結果傳回 client 的網路/渲染時間。所以 `redis-cli` 顯示 `KEYS *` 花了 4.5 秒，但 SLOWLOG 可能只記幾百 ms——因為那 4.5 秒大半是傳 100 萬筆回你終端，不是 server 阻塞。要量「阻塞別人多久」看 SLOWLOG，不是看 client 端時間。

---

## 5. 阻塞實驗：親手把整台 Redis 卡住

這個實驗是 Stage 0 的靈魂。目的：**用兩個 cli 親眼看到「一條慢指令卡住所有 client」**，把 2.1 的理論變成肌肉記憶。

### 步驟

1. **開終端機 A**，連進 Redis：

   ```bash
   redis-cli
   ```

2. **開終端機 B**，也連進同一個 Redis：

   ```bash
   redis-cli
   ```

3. 在 **終端機 A** 送出一條會睡 3 秒的指令（`DEBUG SLEEP` 讓 server 執行緒真的停住）：

   ```bash
   127.0.0.1:6379> DEBUG SLEEP 3
   ```

4. **在 A 送出後的 3 秒內**，立刻切到 **終端機 B**，打任何一條指令——連最便宜的 `PING` 都行：

   ```bash
   127.0.0.1:6379> PING
   ```

5. **觀察現象**：終端機 B 的 `PING` **不會馬上回 PONG**，而是卡住，一直等到 A 的 3 秒睡眠結束，B 才突然收到 `PONG`。

### 進階：量化阻塞時間

在終端機 B 用 `--latency` 持續量測往返延遲，然後在 A 送 `DEBUG SLEEP`，會看到 latency 飆到 ~3000ms：

```bash
# 終端機 B
redis-cli --latency
min: 0, max: 0, avg: 0.15 (xxx samples)   # 平常都是零點幾 ms
# 這時在終端機 A 打 DEBUG SLEEP 3，B 的 max 會瞬間跳到 ~3000
```

`--latency` 持續送 `PING` 量往返延遲，每秒更新那一行。輸出判讀：

```
min: 0, max: 3001, avg: 34.78 (88 samples)
     │       │           │         └ ping 了幾次（不是 ms，是次數）
     │       │           └ 平均往返（ms）
     │       └ 最慢一次往返（ms）← 抓阻塞看這個
     └ 最快一次（ms）
```

**單位都是毫秒（ms），只有 samples 是次數。** 要用它抓某條指令的阻塞，兩件事必須**同時發生**：終端 B 先開著 `--latency` 跑，終端 A 才送慢指令——因為 `--latency` 只量「它執行期間」發生的事，不是回顧歷史。沒同時跑就抓不到那次尖峰。相關變體：`--latency-history`（分段看趨勢）、`--intrinsic-latency 5`（量機器本身固有延遲，排除 Redis，若它就很高代表是你機器/CPU 問題）。

### 你該得到的結論

- `PING` 本身是 O(1)、超快的指令，它慢**不是因為自己慢，而是因為前面有一條指令佔著唯一的執行緒**。
- 把 `DEBUG SLEEP 3` 換成正式環境的 `KEYS *`（在百萬 key 的庫上）就是一模一樣的災難，只是你可能沒察覺是它造成的。
- 這解釋了為什麼監控 Redis 要盯 **`SLOWLOG` 與 `INFO commandstats`**：找出那條拖累全庫的指令。

---

## 6. Go 連線第一支程式

用 `go-redis/v9` 寫一支能連線、`PING`、讀寫 key 的完整程式。

### 6.1 準備

確認 `go.mod` 有相依（本 repo 已備好；若自建專案）：

```bash
go get github.com/redis/go-redis/v9
go mod tidy
```

### 6.2 完整可跑的 `main.go`

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	// 1. 建立 client。NewClient 不會馬上連線，只是準備好連線池設定。
	//    真正的 TCP 連線是「第一次執行指令時」才從連線池按需建立。
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379", // Redis 位址
		Password: "",               // 沒設密碼就留空
		DB:       0,                 // 用 DB 0
		// --- 連線池相關（先用預設，這裡列出讓你知道有這些旋鈕）---
		// PoolSize:     10 * runtime.GOMAXPROCS, // 預設：每個 CPU 10 條連線
		// MinIdleConns: 0,                        // 預設不預熱閒置連線
		// DialTimeout:  5 * time.Second,          // 建立連線逾時
		// ReadTimeout:  3 * time.Second,          // 讀取逾時
		// WriteTimeout: 3 * time.Second,          // 寫入逾時
	})
	// 程式結束前關掉連線池，釋放所有連線。
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Printf("close redis: %v", err)
		}
	}()

	// 2. 每個操作都要帶 context，用來控制逾時 / 取消。
	//    這是 v9 的核心慣例：所有指令方法第一個參數都是 ctx。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 3. PING 確認連得到。錯誤處理一定要做，別忽略。
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("cannot reach redis: %v", err)
	}
	fmt.Println("PING ok, connected to Redis")

	// 4. 寫一個 key，設 60 秒 TTL。
	if err := rdb.Set(ctx, "greeting", "hello from go", 60*time.Second).Err(); err != nil {
		log.Fatalf("SET failed: %v", err)
	}

	// 5. 讀回來。
	val, err := rdb.Get(ctx, "greeting").Result()
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	fmt.Printf("GET greeting = %q\n", val)

	// 6. 處理「key 不存在」——go-redis 用 redis.Nil 這個哨兵錯誤表示。
	//    這是新手最常踩的坑：key 不存在不是「空字串」，是一個 error。
	_, err = rdb.Get(ctx, "does-not-exist").Result()
	if errors.Is(err, redis.Nil) {
		fmt.Println("key does-not-exist: not found (redis.Nil), 這是正常情況不是故障")
	} else if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
}
```

執行（本 repo 可放進 `labs/` 或直接 `go run`）：

```bash
go run ./main.go
# 或若放進 labs：make lab N=00-hello
```

預期輸出：

```
PING ok, connected to Redis
GET greeting = "hello from go"
key does-not-exist: not found (redis.Nil), 這是正常情況不是故障
```

### 6.3 連線池預設值：你一定要懂的三件事

`go-redis` 內建連線池，你不用自己管 TCP 連線的開關，但要知道它的行為：

| 概念 | 預設行為 | 為什麼重要 |
| --- | --- | --- |
| **連線池大小 `PoolSize`** | `10 × GOMAXPROCS`（每顆 CPU 10 條） | 這是同時能並發打 Redis 的上限；不夠會排隊等連線 |
| **連線何時建立** | 懶惰建立，第一次用到才連 | `NewClient` 不會報「連不到」，錯誤要在第一次指令（如 `Ping`）才看得到 |
| **一條連線同時只跑一條指令** | 池會借出→用完還回 | 呼應 Redis 單線程：連線是序列使用的，池是為了「並發多個 goroutine」而不是加速單條指令 |
| **`Close()`** | 關閉整個池 | 長期程式共用「一個」`*redis.Client`（它是併發安全的），別每次請求都 `NewClient` |

**關鍵慣例**：整個程式**共用一個 `*redis.Client`**（它是 goroutine-safe 的），透過 `context` 控制每次操作的逾時與取消，用 `redis.Nil` 判斷 key 不存在。這三點記住，Go 端就上手了。

---

## 7. 觀測工具總覽表

日後遇到「慢」「記憶體爆」「延遲抖動」時，從這張表挑工具。先建立印象，細節各章會用到。

| 工具 | 怎麼用 | 解決什麼問題 | 注意 |
| --- | --- | --- | --- |
| `redis-cli --bigkeys` | `redis-cli --bigkeys` | 掃出各型別最大的 key，找「大 key」元凶 | 用 SCAN 抽樣，對 prod 相對安全 |
| `redis-cli --latency` | `redis-cli --latency` | 持續量 client→server 往返延遲 | 阻塞實驗量化神器 |
| `redis-cli --latency-history` | 分段輸出延遲趨勢 | 看延遲隨時間變化 | |
| `redis-benchmark` | `redis-benchmark -q -n 100000` | 壓測吞吐量（ops/sec） | 別對 prod 亂壓 |
| `MEMORY DOCTOR` | cli 內打 `MEMORY DOCTOR` | 記憶體健康快診（碎片、大 key 提示） | 一句話報告 |
| `MEMORY USAGE key` | `MEMORY USAGE somekey` | 單一 key 精確佔用 bytes | 找大 key 的第二步 |
| `LATENCY DOCTOR` | 先 `CONFIG SET latency-monitor-threshold 100`，再 `LATENCY DOCTOR` | 分析延遲尖峰的成因（fork、AOF、慢指令） | 要先開 latency monitor 門檻 |
| `LATENCY HISTORY <event>` | `LATENCY HISTORY command` | 看某類延遲事件的歷史 | |
| `SLOWLOG` | `SLOWLOG GET 10` | 找出執行超過門檻的慢指令 | 門檻 `slowlog-log-slower-than` |
| `INFO` / `INFO commandstats` | `INFO commandstats` | 各指令呼叫次數與平均耗時 | 找「又慢又常打」的指令 |
| `MONITOR` | cli 內 `MONITOR` | 即時看每條進來的指令 | **會拖效能，勿長開在 prod** |
| RedisInsight | 瀏覽器開 `http://localhost:5540` | GUI 瀏覽 key、記憶體分析、跑指令 | 本 repo single profile 已附 |

---

## 8. 專案流程說明：這個 repo 怎麼用

本 repo 的結構對應「觀念 → 動手 → 沉澱」三層：

```
go-redis-kit/
├── docs/        ← 觀念與筆記（你正在讀的就是 docs/00-environment.md）
├── labs/        ← 各主題的可跑 demo，用 `make lab N=xx-topic` 執行
├── rediskit/    ← 抽出來的可複用 library 程式碼（封裝好的 client、helper）
├── docker-compose.yml  ← single / sentinel / cluster 三環境
└── Makefile     ← 所有常用指令入口（make help 看全部）
```

三個目錄的分工：

| 目錄 | 放什麼 | 心態 |
| --- | --- | --- |
| `docs/` | 每個主題的觀念、心智模型、踩坑、指令速查 | 讀完能「講給別人聽」 |
| `labs/` | 每個主題一支可獨立跑的 demo（`labs/00-hello`、`labs/05-distributed-lock`…） | 跑得動、改得動、看得到輸出 |
| `rediskit/` | 從 labs 沉澱出來、值得複用的 library | 有測試（`make test` 用 miniredis，免 docker） |

`make help` 會列出所有可用指令（`make test` 跑全單測、`make bench` 跑 benchmark、`make lab N=...` 跑某支 demo）。

### 建議：每個主題都做「三個動作」

1. **手打 cli**：先在 `redis-cli` 裡把該主題的指令親手打一遍，用 `OBJECT ENCODING` / `MEMORY USAGE` 觀察行為。→ 建立直覺。
2. **寫 Go demo**：在 `labs/` 下寫一支對應的 demo，把 cli 觀察到的行為用程式重現。→ 從「會打指令」進到「會用在程式裡」。
3. **記踩坑**：在 `docs/` 對應那頁記下你踩到的坑、反直覺的點。→ 把經驗變成能複習的資產。

這個循環走完一輪，一個主題才算真的「會」。

---

## 9. 踩坑清單（Stage 0 就要刻進骨子裡）

| 禁令 / 陷阱 | 為什麼 | 正確做法 |
| --- | --- | --- |
| **prod 不要打 `KEYS *`** | O(N) 掃全庫，單線程被卡住，全服務延遲飆高 | 用 `SCAN` 游標分批掃描 |
| **prod 不要 `FLUSHALL` / `FLUSHDB`** | 一次刪光所有資料且阻塞；手殘直接災難 | 真要清用 `FLUSHALL ASYNC`；prod 建議 `rename-command` 禁用 |
| **prod 不要用 `DEBUG` 系列**（`DEBUG SLEEP`、`DEBUG OBJECT`…） | `DEBUG SLEEP` 直接阻塞全庫，純教學/測試用 | 只在本機實驗環境用 |
| **`MONITOR` 不要長開在 prod** | 它要把每條指令複製給你，高 QPS 下嚴重拖慢 server | 短暫 debug 用完立刻關；優先看 `SLOWLOG` |
| **不要對大集合做全量操作**（`HGETALL`、`SMEMBERS`、`LRANGE 0 -1`、`SORT`） | O(N) 大 N，撈 + 序列化 + 傳輸三重阻塞 | 用 `HSCAN`/`SSCAN`/`ZSCAN` 分批，或 `LRANGE` 帶範圍 |
| **忽略 `redis.Nil`**（Go） | key 不存在回的是 error 不是空值，`err != nil` 一律 fatal 會誤判 | 用 `errors.Is(err, redis.Nil)` 分辨「不存在」與「真故障」 |
| **每次請求都 `NewClient`**（Go） | 反覆建連線池，浪費資源又可能耗盡連線 | 全程式共用一個 `*redis.Client`（併發安全） |
| **忘記設 TTL / eviction 心態** | 記憶體無聲無息漲爆，撞 `maxmemory` 後開始淘汰或寫入報錯 | 快取類 key 一律設 TTL；理解 `maxmemory-policy` |
| **在 cluster 上用跨 slot 的多 key 指令** | key 分散在不同節點，`MGET a b c` 可能報 CROSSSLOT | 用 hash tag `{user}:a`、`{user}:b` 綁同一 slot（cluster 章節詳談） |

---

## 10. 練習題 + 檢查點 + 延伸閱讀

### 10.1 練習題

1. **起環境**：用 `make single-up` 起單機 Redis，`redis-cli PING` 確認回 `PONG`，再打開 `http://localhost:5540` 用 RedisInsight 連上這個 Redis。
2. **編碼觀察**：在 cli 依序 `SET a 100`、`SET b "a-long-enough-string-value-over-44-bytes-xxxxxxxxxxxx"`、`RPUSH c 1 2 3`，分別用 `OBJECT ENCODING` 看三者編碼有何不同，並用 `MEMORY USAGE` 比較佔用。想想為什麼 `a` 是 `int`。
3. **重現阻塞**：照第 5 節開兩個 cli，一個 `DEBUG SLEEP 3`，另一個在 3 秒內打 `PING`，記錄 `PING` 實際等了多久才回。再用 `redis-cli --latency` 量化一次。
4. **慢查詢**：`CONFIG SET slowlog-log-slower-than 0`，隨便打幾條指令，用 `SLOWLOG GET 5` 看紀錄。找出每筆紀錄裡「耗時（微秒）」欄位在哪。做完記得 `CONFIG SET slowlog-log-slower-than 10000` 改回來。
5. **Go 首發**：把第 6 節的 `main.go` 跑起來，故意把 `Addr` 改成一個不存在的 port（如 `127.0.0.1:6390`），觀察錯誤是在 `NewClient` 還是 `Ping` 時才出現，印證「連線懶惰建立」。

### 10.2 檢查點（能回答這些，才算過關）

- [ ] 我能用自己的話解釋「Redis 為什麼快」與「為什麼一條慢指令會拖垮全庫」。
- [ ] 我知道 `KEYS`、`FLUSHALL`、`MONITOR`、`DEBUG SLEEP` 為什麼在 prod 危險，以及各自的替代方案。
- [ ] 我能說出 RESP 協議大概長怎樣（`+ - : $ *` 五個前綴），以及一條指令送出去是 array of bulk strings。
- [ ] 我能用 `OBJECT ENCODING`、`MEMORY USAGE`、`SLOWLOG`、`INFO` 各回答一個問題。
- [ ] 我能寫出一支用 `go-redis/v9` 連線、`Ping`、`Set`/`Get` 的程式，並正確處理 `redis.Nil`。
- [ ] 我知道 `go-redis` 連線池預設大小是 `10 × GOMAXPROCS`、連線懶惰建立、且應全程式共用一個 client。
- [ ] 我知道本 repo 的 `docs/` `labs/` `rediskit/` 各放什麼，以及「手打 cli → 寫 Go demo → 記踩坑」的循環。

### 10.3 延伸閱讀

- Redis 官方文件首頁：<https://redis.io/docs/latest/>
- 為什麼 Redis 是單線程（官方 FAQ）：<https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/latency/>
- RESP 協議規格：<https://redis.io/docs/latest/develop/reference/protocol-spec/>
- 各指令時間複雜度（每個指令頁都標了 Time complexity）：<https://redis.io/commands/>
- 記憶體優化與編碼：<https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/memory-optimization/>
- `go-redis` 官方文件與 API：<https://redis.uptrace.dev/> 、<https://pkg.go.dev/github.com/redis/go-redis/v9>
- RedisInsight：<https://redis.io/docs/latest/operate/redisinsight/>

---

> 下一站（Stage 1）會進入 **String 型別**：`SET`/`GET`/`INCR` 的原子性、`int`/`embstr`/`raw` 三種編碼的切換臨界點、bitmap 與計數器實戰。帶著這一頁建立的「單線程 + O(N) 警覺 + 觀測工具」直覺往前走。
