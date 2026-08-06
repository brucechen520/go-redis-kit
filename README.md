# go-redis-kit

Redis notes and a Go wrapper library (rediskit, work in progress) (Go 1.26.1 · `github.com/redis/go-redis/v9`).

## 環境

需要 Docker + Go 1.26.1。

```bash
# 起單機 redis（含 RedisInsight GUI on http://localhost:5540）
make single-up

# 密碼：single profile 有設 requirepass（見 docker-compose.yml）
export REDISCLI_AUTH=devpass_change_me   # 換成你自己的
redis-cli ping                            # PONG

# 跑 lab（demo 會自己連 127.0.0.1:6379，密碼用 REDIS_PASSWORD 覆寫）
go run ./labs/00-hello
go run ./labs/01-string-counter

# 收工
make single-down
```

三種環境（`docker-compose.yml` 用 profile 隔開，一次起一種）：

| 環境 | 指令 | 用途 |
| --- | --- | --- |
| single | `make single-up` | 平常學習、跑 lab（含 RedisInsight :5540） |
| sentinel | `make sentinel-up` | 練故障轉移 |
| cluster | `make cluster-up` + `make cluster-init` | 練分片 / hash tag |

`make help` 看全部指令。

> ⚠️ redis 綁 `127.0.0.1` 且設了密碼。

## rediskit

`rediskit/` 是把 labs 各階段模式收斂成的可重用 lib（設計 spec 見 `docs/07-rediskit-production.md`）。四個意圖型別：

| 型別 | API | 內建 |
| --- | --- | --- |
| `Cache` | `Get` / `Set` / `Delete` / 泛型 `GetOrLoad` | cache-aside、singleflight、TTL 抖動 |
| `Locker` | `Obtain` / `Release` / `Refresh` / `ObtainWithFence` | SET NX PX、Lua CAS 釋放、fencing token |
| `RateLimiter` | `Allow` / `AllowN` | 令牌桶（Lua 原子判斷+扣減） |
| `TokenStore` | `Save` / `Load` / `Rotate` / `Revoke` | TTL 強制、原子輪替（CAS） |

```go
client, err := rediskit.New(
    rediskit.WithAddr("localhost:6379"),
    rediskit.WithPassword(os.Getenv("REDIS_PASSWORD")),
    rediskit.WithNamespace("app"), // 所有 key 自動帶 app: 前綴
)
if err != nil { log.Fatal(err) }
defer client.Close()

// 快取：miss 時回源 DB，同 key 併發只打一次 DB
u, err := rediskit.GetOrLoad(ctx, client.Cache(), "user:123", 10*time.Minute,
    func(ctx context.Context) (User, error) { return loadUserFromDB(ctx, "123") })

// 鎖：拿不到就跳過（fail-close）
lock, err := client.Locker().Obtain(ctx, "job:daily-report", 30*time.Second)
if errors.Is(err, rediskit.ErrLockNotObtained) { return }
defer lock.Release(ctx)

// 限流：每分鐘 100 次
rl := client.RateLimiter(100, time.Minute)
if err := rl.Allow(ctx, "login:user:123"); errors.Is(err, rediskit.ErrRateLimited) {
    // 回 429
}
```

要點：

- **呼叫端不 import go-redis**。錯誤全用 `errors.Is` 判 `rediskit.Err*` 哨兵：`ErrCacheMiss` / `ErrLockNotObtained` / `ErrLockLost` / `ErrRateLimited` / `ErrTokenNotFound` / `ErrTimeout` / `ErrCanceled` / `ErrClosed`。
- **`ErrTimeout` ≠ `ErrCanceled`**：前者是 Redis/網路慢（該告警），後者是呼叫端不要了（不該觸發降級）。
- **意圖 API 沒涵蓋的**（bitmap、Stream、SCAN…）走 `client.Raw()` 逃生艙——業務碼禁用，包在自己的基礎設施型別裡再用。

## Example

完整逐步範例見 **[docs/09-rediskit-example.md](docs/09-rediskit-example.md)**：連線調參、Cache/GetOrLoad、8 個哨兵錯誤的處理方式、鎖（含 watchdog 續命與 fencing token）、限流 middleware、refresh token 輪替、metrics 掛載、`Raw()` 使用守則、用 miniredis + 假時鐘測你自己的業務碼。每個範例都附「實際情境」說明用在系統的哪個位置。

## 測試

```bash
make test     # go test ./...
make bench    # 只跑 benchmark（ns/op、B/op、allocs/op；量 lib 開銷，不含真實網路）
```

單元測試跑在 [miniredis](https://github.com/alicebob/miniredis) `v2.33.0`（純 Go 記憶體版 Redis），**不需要 Docker**。miniredis v2.33.0 的行為對齊 **Redis 7.2.4**。

需要真的 Redis 時（labs、手動驗證）用 `docker-compose.yml` 的 `redis:7` image，實測版本 **7.4.9**。
