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
