# go-redis-kit

> 我的 Redis 深入學習 repo：把 Redis 從「會用」練到「懂原理、上線不踩坑、面試問不倒」。

Redis 很多人「會用」，但**面試被追問細節、上線遇到大 key / 熱 key / 分散式鎖 race 就露餡**。這個 repo 反過來做：每個主題都問到底——底層編碼、時間複雜度、踩坑、成本、實際情境——再用可跑的 Go demo 驗證。這是一個**邊學邊記、持續長大**的個人學習專案。

- 語言 / 版本：Go **1.26.1** · 模組 `github.com/twteam/go-redis-kit`
- Redis client：`github.com/redis/go-redis/v9`

---

## 🎯 這個 repo 想達到什麼

1. **把每個 Redis 主題學到底** —— 不只會打指令，要能說出底層編碼、時間複雜度、什麼情境用、有什麼坑、成本多少。
2. **每個觀念都用可跑的 Go demo 驗證** —— 不是紙上談兵，`go run` 看到結果、用 SLOWLOG 量出差異。
3. **（長期目標）把散落的模式收斂成一套封裝** —— 快取 / 分散式鎖 / 限流 / token 收成可重用、可測、可上線的 lib（`rediskit`）。

---

## 📍 目前進度

| 部分 | 狀態 |
| --- | --- |
| `docs/` 八階段觀念筆記 | ✅ 已寫（7600+ 行，持續補充踩坑） |
| `labs/00-hello` | ✅ 可跑（連線 / Ping / 懶惰連線） |
| `labs/01-string-counter` | ✅ 可跑（String / Hash / List 共 8 個 demo） |
| `labs/` 其餘結構（Set / ZSet / Stream…）的 demo | 🚧 陸續補 |
| `rediskit/` 封裝實作 | 🚧 尚未開始（設計藍圖在 `docs/07`） |

> 學習節奏：每主題「**手打 redis-cli → 跑 / 寫 lab → 把踩坑記回該 doc**」。

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
| single | `make single-up` | 平常學習、跑 lab（含 RedisInsight :5540） |
| sentinel | `make sentinel-up` | 之後練故障轉移 |
| cluster | `make cluster-up` + `make cluster-init` | 之後練分片 / hash tag |

> ⚠️ **安全**：redis 綁 `127.0.0.1` 且設了密碼——別讓它對公網開放（未授權 Redis 是最常被掃描入侵的目標之一）。

---

## docs — 八階段筆記

| 階段 | 檔案 | 主題 |
| --- | --- | --- |
| 0 | `docs/00-environment.md` | 環境 + 單線程心智模型 + SLOWLOG / --latency 判讀 |
| 1 | `docs/01-datastructures.md` | 10 種資料結構（編碼 / 複雜度 / 情境 / 踩坑） |
| 2 | `docs/02-expiry-eviction-memory.md` | 過期 / 淘汰策略 / 記憶體 |
| 3 | `docs/03-persistence.md` | RDB / AOF / 混合持久化 |
| 4 | `docs/04-ha-scaling.md` | 主從 / Sentinel / Cluster |
| 5 | `docs/05-concurrency-primitives.md` | 事務 / Lua / Pipeline / 分散式鎖 ★ |
| 6 | `docs/06-application-patterns.md` | 快取三問 / 限流 / Token |
| 7 | `docs/07-rediskit-production.md` | rediskit 封裝的設計藍圖（尚未實作） |

---

## labs — 可跑的 demo

每個 lab 是獨立 `package main`，`go run` 直接跑（需先 `make single-up`）。程式碼即教材，附繁中註解 + 實際情境。

`labs/01-string-counter` 目前的 8 個 demo：

| # | 主題 | 證明什麼 |
| --- | --- | --- |
| 1 | 併發 INCR | 100 goroutine 同時 +1，原子不少算 |
| 2 | GETDEL 歸檔 | 原子取值歸零，避免「讀後清空」競態 |
| 3 | Lua 限流 | INCR+EXPIRE 綁原子（INCR 沒有內建 EX） |
| 4 | 分片計數器 | 熱 key 打散到多 shard，MGET 加總 |
| 5 | Hash 多欄位 | profile / 購物車 / 計數聚合 / 配置字典 |
| 6 | HGETALL vs HSCAN | 用 SLOWLOG 量：HGETALL 一次卡 ~37ms，HSCAN 單段 ~1.7ms |
| 7 | LPUSH+LTRIM | 保留最新 N 筆（最近瀏覽） |
| 8 | BLMOVE 可靠佇列 | 崩潰任務搬回重試，不遺失 |

---

## 接下來要做

- [ ] 補齊 Set / ZSet / Stream / Bitmap 的 lab demo
- [ ] 動手練 sentinel / cluster 環境（故障轉移、hash tag）
- [ ] 照 `docs/07` 把 `rediskit/` 實作出來（Cache / Lock / RateLimiter / TokenStore）

---

## 延伸閱讀

官方 docs（redis.io/commands 逐指令看 Time complexity）、《Redis 設計與實作》（黃健宏，底層編碼）、antirez 的 Redlock 原文 + Kleppmann 反駁文（分散式鎖爭議兩面看）。
