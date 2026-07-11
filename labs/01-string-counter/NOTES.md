# Lab 02 — HGETALL vs HSCAN 效能差異

## 這個 lab 在證明什麼

大 hash（10 萬 field）用 `HGETALL` 一次全撈 = **O(N)，一整段獨佔單執行緒**，期間**所有其他 client 被卡住**。改用 `HSCAN` 游標分批，每批小、批與批之間讓其他 client 插進來 → 別人幾乎無感。

**重點不是「總耗時」**（HSCAN 多次往返，總牆鐘時間可能還更久），而是**「HGETALL 期間別人被卡多久」**。

## 跑 Go 版

```bash
make single-up
go run ./labs/02-hash-hgetall-vs-hscan
# 位址／密碼可用 REDIS_ADDR、REDIS_PASSWORD 覆寫
```

Go 版在背景用另一條連線狂送 `PING`、記錄最大延遲，同時分別跑 HGETALL / HSCAN，量出「其他 client 被卡多久」。實測（10 萬 field）：

| 讀法 | 本身耗時 | 期間其他 client PING 最大延遲 |
| --- | --- | --- |
| `HGETALL` | ~95 ms | **~35 ms（被卡住）** |
| `HSCAN COUNT 1000`（100 批） | ~367 ms | **~8 ms（幾乎無感）** |

HSCAN 總時間更長（100 次往返），但**不獨佔 server**，這才是它的價值。

## redis-cli 版

### 1. 先造一個大 key（10 萬 field）

```bash
export REDISCLI_AUTH=devpass_change_me   # 你的 redis 若有密碼
redis-cli DEL big:hash
redis-cli eval "for i=1,100000 do redis.call('HSET','big:hash','f'..i,i) end" 0
redis-cli HLEN big:hash            # 100000
redis-cli OBJECT ENCODING big:hash # hashtable
```

### 2. 量 server 端「純執行」耗時（SLOWLOG）

`SLOWLOG` 記的是 server 阻塞時間，不含把結果傳回 client 的網路時間 → 最能代表「卡住別人多久」。

```bash
redis-cli CONFIG SET slowlog-log-slower-than 0   # 全記（實驗用）

redis-cli SLOWLOG RESET
redis-cli HGETALL big:hash > /dev/null           # 一次全撈
redis-cli SLOWLOG GET 5 | ...                     # 找 HGETALL 那筆，第 3 欄=微秒

redis-cli SLOWLOG RESET
redis-cli HSCAN big:hash 0 COUNT 1000 > /dev/null # 單批
redis-cli SLOWLOG GET 5 | ...                     # 找 HSCAN 那筆

redis-cli CONFIG SET slowlog-log-slower-than 10000  # 改回門檻
```

實測（server 端純執行）：
- `HGETALL` 10 萬 field：**≈ 134,000 µs ≈ 134 ms**（整段期間所有 client 都等）
- `HSCAN ... COUNT 1000` 單批：**≈ 1,000 µs ≈ 1 ms**

一次 HGETALL 阻塞 134ms，而 HSCAN 把它切成 ~100 個 1ms 小段。

### 3. 兩終端「親眼看阻塞」（最直觀）

```bash
# 終端 B：持續量往返延遲
redis-cli --latency

# 終端 A：對大 hash 跑 HGETALL → 回終端 B 看 max 飆高
redis-cli HGETALL big:hash > /dev/null
# 對照：改跑 HSCAN 迴圈 → 終端 B 的 max 幾乎不動
```

### 收尾

```bash
redis-cli DEL big:hash
redis-cli CONFIG SET slowlog-log-slower-than 10000
```

## 實際情境會用在哪

任何**線上環境要遍歷/讀取大集合**的場景，一律用 SCAN 家族（`HSCAN`/`SSCAN`/`ZSCAN`/`SCAN`），別用 `HGETALL`/`SMEMBERS`/`LRANGE 0 -1`/`KEYS` 全撈：大 hash 全量匯出／備份、批次清理遷移、統計聚合、大 profile／設定表遍歷。核心紀律：**單執行緒下，一次 O(N) 大 N 的操作會卡住全庫；改用游標分批就不會。**

## 兩條紀律（跟編碼無關）

1. **永遠用 `cursor==0` 判斷結束**（別用回傳筆數，某批可能空但沒掃完）。
2. **別假設每批固定 N 筆**（`COUNT` 是提示不是保證）。listpack 小 hash 甚至會忽略 COUNT 一次全回、cursor 直接 0；hashtable 才真的分批。但你的迴圈兩種編碼**同一段 code**，不用判斷（見 `docs/01` Hash 章）。
