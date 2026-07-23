# Lab 05 — 分散式鎖（Distributed Lock）

`lock.go` 是鎖封裝，`main.go` 是真 redis demo，`lock_test.go` 是 miniredis 測試（免 docker）。對照 `docs/05-concurrency-primitives.md`。

## 跑

```bash
# demo（要真 redis）
make single-up
make lab N=05-distributed-lock
# 位址／密碼可用環境變數覆寫：REDIS_ADDR、REDIS_PASSWORD

# 測試（純記憶體 miniredis，免 docker）
go test ./labs/05-distributed-lock/ -v
make test          # 全 repo 一起
```

## API（`lock.go`）

| 函式 | 指令 | 重點 |
| --- | --- | --- |
| `Acquire` | `SET key token NX PX ttl` | NX=互斥；PX=TTL（持有者 crash 也自動釋放，防死鎖）；token=隨機值分辨「這鎖是不是我的」|
| `Release` | Lua：`GET==token` 才 `DEL` | 比對+刪除原子化，避免刪到別人的鎖 |
| `Refresh` | Lua：`GET==token` 才 `PEXPIRE` | watchdog 續命，只有持有者能續 |
| `AcquireWithFence` | `Acquire` + `INCR fenceKey` | 回單調遞增序號，下游擋「過期後才到」的舊請求 |

## 三道防線（docs/05）

1. **holder-safe 釋放**：`Release` 用 Lua 比對 token 才刪 → 不會刪掉別人的鎖。
2. **watchdog 續命**：`Refresh` 用 Lua 比對 token 才延 TTL → 長任務做到一半鎖不會過期，且非持有者續不了。
3. **fencing token**：`AcquireWithFence` 的 `INCR` 序號 → 就算「A 鎖過期、B 拿新鎖」，A 遲來的寫入帶舊 fence 被下游擋掉。鎖只降低機率，fence 才是最後保險。

## Lua 為什麼要 `redis.NewScript`（不裸 EVAL）

- `releaseScript` / `refreshScript` 是**套件層 var，定義一次**。`.Run()` 內部走 **EVALSHA → NOSCRIPT → EVAL**：平常只送 SHA1 省頻寬，cache miss（重啟 / flush / failover）自動 fallback 送全文。
- 值全走 **KEYS / ARGV**，腳本文字固定 → 一個 SHA1、cache 只佔一份。**不動態拼字串**（拼值進腳本會每種參數各佔一份 cache，只增不減撐爆記憶體）。

## 測試涵蓋（`lock_test.go`）

| 測試 | 驗什麼 |
| --- | --- |
| `TestAcquireMutex` | 互斥：A 拿到 B 拿不到；A 釋放後 C 才行 |
| `TestReleaseWrongTokenNoop` | 錯 token 釋放無效，鎖與 token 完好 |
| `TestReleaseAfterExpiryDoesNotDeleteOthersLock` | **核心競態**：A 鎖過期→B 拿走→A 遲來 Release 不刪 B 的鎖 |
| `TestRefresh` | 持有者續命成功且 TTL 拉長；非持有者 / 已過期續不了 |
| `TestFencingTokenMonotonic` | fence 嚴格遞增 |

## 值得注意的點

- **`mr.FastForward(d)` 模擬 TTL 過期**：miniredis 可快轉時間，不用真 `sleep` 就能測「鎖過期後的競態」——這是純記憶體測試能覆蓋時間相關邏輯的關鍵。
- **miniredis 撐 EVALSHA**：`redis.NewScript.Run()` 的 EVALSHA/EVAL 在 miniredis 可跑，所以 Lua 邏輯也免 docker 測。
- **鎖不是銀彈**：GC/STW、時鐘漂移、網路延遲都可能讓「以為還持有」的持有者其實已過期。所以生產一定要有 **fencing token**（第三道防線）在下游做最終防護，別只靠鎖本身。
- **單機 redis 的鎖有單點風險**：master 掛掉 failover 到還沒同步鎖的 replica，兩人可能同時持有。要更強一致用 **Redlock**（多 master）或改用有共識的系統（etcd/zk）——但多數場景「單 redis 鎖 + fencing」已足夠，見 docs/05 選型。
