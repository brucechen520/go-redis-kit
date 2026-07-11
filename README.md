# go-redis-kit

> 一個「**把 Redis 學到能上線**」的專案：八階段觀念筆記 + 可跑的 Go demo + 一套生產級封裝的設計藍圖。

Redis 很多人「會用」，但**面試被追問細節、上線遇到大 key / 熱 key / 分散式鎖 race 就露餡**。這個 repo 反過來做：每個主題都問到底——底層編碼、時間複雜度、踩坑、成本、實際情境——再用可跑的 lab 驗證，最後收斂成一套可重用的 lib。

- 語言 / 版本：Go **1.26.1** · 模組 `github.com/twteam/go-redis-kit`
- Redis client：`github.com/redis/go-redis/v9`
- 分散式鎖參考：`github.com/go-redsync/redsync/v4` · 測試：`github.com/alicebob/miniredis/v2`（免 docker）

---

## 這個 repo 有什麼

```
go-redis-kit/
├── docs/                # 八階段觀念筆記（7600+ 行，每主題：原理/複雜度/情境/踩坑/範例）
│   ├── 00-environment.md            環境 + 單線程心智模型 + 觀測工具
│   ├── 01-datastructures.md         10 種資料結構全解（String/Hash/List/Set/ZSet/Stream/Bitmap/HLL/Geo/Bitfield）
│   ├── 02-expiry-eviction-memory.md 過期 / 淘汰策略 / 記憶體
│   ├── 03-persistence.md            RDB / AOF / 混合持久化
│   ├── 04-ha-scaling.md             主從 / Sentinel / Cluster
│   ├── 05-concurrency-primitives.md 事務 / Lua / Pipeline / 分散式鎖（核心）
│   ├── 06-application-patterns.md    快取三問 / 限流 / Token
│   └── 07-rediskit-production.md     rediskit 封裝的設計藍圖 + 生產化
├── labs/                # 可跑的 Go demo（go run 直接跑，對照 docs 驗證）
│   ├── 00-hello/                     連線 / Ping / 懶惰連線
│   └── 01-string-counter/            String×4 + Hash×2 + List×2 共 8 個 demo
├── rediskit/            # 生產級封裝（🚧 設計在 docs/07，程式碼待實作）
├── docker-compose.yml   # single / sentinel / cluster 三種環境
├── Makefile
└── go.mod
```

---

## 快速開始

需要 Docker + Go 1.26.1。

```bash
# 1. 起單機 redis（含 RedisInsight GUI on http://localhost:5540）
make single-up

# 2. 密碼：single profile 有設 requirepass（見 docker-compose.yml）
export REDISCLI_AUTH=devpass_change_me   # 換成你自己的
redis-cli ping                            # PONG

# 3. 跑 lab（demo 會自己連 127.0.0.1:6379，密碼用 REDIS_PASSWORD 覆寫）
go run ./labs/00-hello
go run ./labs/01-string-counter

# 收工
make single-down
```

三種環境（`docker-compose.yml` 用 profile 隔開，一次起一種）：

| 環境 | 指令 | 用途 |
| --- | --- | --- |
| single | `make single-up` | 平常學習、跑 lab（含 RedisInsight :5540、`--enable-debug-command`） |
| sentinel | `make sentinel-up` | 練故障轉移、`NewFailoverClient` |
| cluster | `make cluster-up` + `make cluster-init` | 練分片、`MOVED` / hash tag、`NewClusterClient` |

`make help` 看全部指令。

> ⚠️ **安全**：docker-compose 的 redis 綁 `127.0.0.1` 且設了密碼——**別讓它對公網開放**（未授權 Redis 是最常被掃描入侵的目標之一）。

---

## docs — 八階段學習路線

建議依序讀，每階段「**手打 redis-cli → 跑 / 寫 lab → 把踩坑記回該 doc**」。核心是 **階段 1（資料結構）** 與 **階段 5（併發原語 / 鎖）**。

| 階段 | 檔案 | 你會學到 |
| --- | --- | --- |
| 0 | `docs/00-environment.md` | 為何單線程、O(N) 大 N 會卡全庫、SLOWLOG / --latency 判讀 |
| 1 | `docs/01-datastructures.md` | 每種結構的編碼門檻、複雜度、情境、踩坑（含計數器 / HSCAN / 佇列可靠度） |
| 2 | `docs/02-expiry-eviction-memory.md` | 惰性+定期刪除、8 種淘汰策略、大 key / 熱 key |
| 3 | `docs/03-persistence.md` | RDB vs AOF 各自的丟資料情況、fork COW |
| 4 | `docs/04-ha-scaling.md` | 主從延遲、Sentinel 選主、Cluster slot / hash tag |
| 5 | `docs/05-concurrency-primitives.md` | MULTI/Lua/Pipeline 差異、鎖三坑、watchdog、等鎖策略、Lua 成本 |
| 6 | `docs/06-application-patterns.md` | 快取穿透/擊穿/雪崩、限流四算法、Token / JWT 黑名單 |
| 7 | `docs/07-rediskit-production.md` | 把上面收斂成 lib 的設計、可觀測 / 連線池 / 降級 / 測試 |

---

## labs — 可跑的 demo

每個 lab 是獨立 `package main`，`go run` 直接跑（需先 `make single-up`）。程式碼即教材，附繁中註解 + 實際情境。

```bash
go run ./labs/00-hello              # 連線、Ping、懶惰連線（錯誤在 Ping 才冒）
go run ./labs/01-string-counter     # 8 個 demo，一次跑完
```

`labs/01-string-counter` 目前的 8 個 demo：

| # | 主題 | 證明什麼 |
| --- | --- | --- |
| 1 | 併發 INCR | 100 goroutine 同時 +1，原子不少算 |
| 2 | GETDEL 歸檔 | 原子取值歸零，避免「讀後清空」競態 |
| 3 | Lua 限流 | INCR+EXPIRE 綁原子（INCR 沒有內建 EX） |
| 4 | 分片計數器 | 熱 key 打散到多 shard，MGET 加總 |
| 5 | Hash 多欄位 | profile / 購物車 / 計數聚合 / 配置字典 |
| 6 | HGETALL vs HSCAN | 用 SLOWLOG 量：HGETALL 一次卡 ~37ms，HSCAN 單段才 ~1.7ms |
| 7 | LPUSH+LTRIM | 保留最新 N 筆（最近瀏覽） |
| 8 | BLMOVE 可靠佇列 | 崩潰任務搬回重試，不遺失 |

---

## rediskit — 生產級封裝

> 🚧 **狀態**：目前是**設計藍圖**（完整設計、目錄職責、API、生產化考量都在 `docs/07-rediskit-production.md`），`rediskit/*.go` **尚未實作**。下面是**目標 API**——照 docs/07 的 spec 逐檔實作即可。

### 為什麼要封裝

前面每個 lab 各寫各的 cache 邏輯、Lua、key 命名、序列化。要上線就會遇到：重複程式碼、key 命名不一致、直接傳 `*redis.Client` 導致難測、無指標、Redis 一抖動整條線就掛。`rediskit` 把這些收斂成**可重用、可測、可觀測、可降級**的一套。

### 分層

```
業務 API   Cache(GetOrLoad) · Lock · RateLimiter(Allow) · TokenStore
    ↓
中間層     序列化 / key 命名 / TTL 抖動 / singleflight / metrics / tracing / 語意化錯誤
    ↓
go-redis   連線池 / 重試 / cluster
```

### 目標用法（見 docs/07）

```go
kit, _ := rediskit.New(
    rediskit.WithAddr("localhost:6379"),
    rediskit.WithNamespace("myapp"),   // key 統一前綴 myapp:...
    rediskit.WithMetrics(prom),        // 命中率 / 延遲 / 錯誤率
)

// 快取：cache-aside + singleflight（併發回源合併）+ TTL 抖動（防雪崩）
var u User
err := kit.Cache().GetOrLoad(ctx, "user:42", &u, func(ctx context.Context) (any, error) {
    return db.LoadUser(ctx, 42)
}, 10*time.Minute)

// 分散式鎖：token + watchdog 續期 + Lua 驗 holder 釋放
lock, err := kit.Lock("order:42", 30*time.Second)
if err == nil {
    defer lock.Release(ctx)
    // 臨界區
}

// 限流：令牌桶 / 滑動視窗（Lua 原子）
if ok, _ := kit.RateLimiter().Allow(ctx, "api:uid:42"); !ok {
    // 429
}

// token：登出即撤銷 / refresh 輪換
kit.TokenStore().Save(ctx, token, "uid-42", time.Hour)

// 錯誤語意化：不讓業務判 go-redis 的 sentinel
if errors.Is(err, rediskit.ErrCacheMiss) { /* ... */ }
```

設計原則（docs/07 詳述）：包 interface 不漏 `*redis.Client`（可測）、context 必傳、Serializer 可換（JSON/msgpack）、Lua 用 `NewScript`、functional options、singleflight 合併回源、`namespace:entity:id` key 規範、go-redis Hook 埋 metrics/tracing、只對冪等操作重試、Redis 掛了降級（本地 cache / 直打 DB）。

---

## Progress（學習進度）

- [ ] 0. 環境 + 心智模型
- [ ] 1. 資料結構全解
- [ ] 2. 過期 / 淘汰 / 記憶體
- [ ] 3. 持久化 RDB/AOF
- [ ] 4. 高可用 主從/Sentinel/Cluster
- [ ] 5. 併發原語 事務/Lua/Pipeline/鎖 ★
- [ ] 6. 應用模式 快取三問/限流/Token
- [ ] 7. rediskit 封裝實作

---

## 延伸閱讀

官方 docs（redis.io/commands 逐指令看 Time complexity）、《Redis 設計與實作》（黃健宏，底層編碼）、antirez 的 Redlock 原文 + Kleppmann 反駁文（分散式鎖爭議兩面看）。
