# Lab 01 — String / Hash / List 實戰

一個 `main.go`，8 個 demo，`go run` 一次跑完。對照 `docs/01-datastructures.md`。

## 跑

```bash
make single-up
go run ./labs/01-string-counter
# 位址／密碼可用環境變數覆寫：REDIS_ADDR、REDIS_PASSWORD
```

## 8 個 demo

| # | 函式 | 主題 | 重點 |
| --- | --- | --- | --- |
| 1 | `demoConcurrentINCR` | 併發 INCR | 100 goroutine 同時 +1，原子不少算；`OBJECT ENCODING` = int |
| 2 | `demoGetDelArchive` | GETDEL 歸檔 | 原子取值歸零，避免「GET 再 DEL」兩步之間漏算 |
| 3 | `demoRateLimit` | Lua 限流 | INCR+EXPIRE 綁原子（INCR 沒有內建 EX），第一次就設好 TTL |
| 4 | `demoShardedCounter` | 分片計數器 | 熱 key 打散到 10 個 shard，MGET 加總 |
| 5 | `demoHash` | Hash 多欄位 | profile / 購物車（分出貨方式）/ 計數聚合 / 配置字典 |
| 6 | `demoHGetAllVsHscan` | HGETALL vs HSCAN | 用 **SLOWLOG** 量 server 阻塞：HGETALL 一次 ~37ms、HSCAN 單段 ~1.7ms |
| 7 | `demoListRecentN` | LPUSH+LTRIM | 保留最新 N 筆（最近瀏覽），有界、自動淘汰舊的 |
| 8 | `demoListReliableQueue` | BLMOVE 可靠佇列 | 搬到 processing = 模擬 ack；崩潰任務可回收重試，不遺失 |

## 幾個值得注意的點

- **demo 6 為什麼用 SLOWLOG 而非 client 計時**：client 端計時會混進網路 / GC / 連線池噪音，量不準。SLOWLOG 是 server 純執行時間，最能代表「卡住別人多久」。實測 HSCAN 的**總 CPU 反而比 HGETALL 多**（多次呼叫游標開銷），但**單段阻塞小 ~22 倍**——換到的是「不長時間獨佔 server」，這才是分批的價值。
- **demo 8 的可靠佇列三級**：`BRPOP`（無 ack，crash 掉訊息）< `BLMOVE`（手工 ack，可回收）< Stream（原生 ack / 回收 / consumer group）。要多消費組 + 重放請直接用 Stream。
