# Stage 1：Redis 資料結構全解

> 模組：`github.com/twteam/go-redis-kit`｜Go 1.26.1｜Client：`github.com/redis/go-redis/v9`
>
> 本文件是 Redis 十大資料結構的深度學習與參考資料。每個結構都依照固定模板拆解：
> **指令清單 → 底層編碼（含轉碼門檻 config）→ 時間複雜度 → 使用情境 → 踩坑清單 → redis-cli 範例 → Go 範例 → 專案流程說明**。
>
> 讀法建議：先看下方「情境對照表」建立全局地圖，再依需求跳到對應章節。ZSet 一節最長最重要，務必完整讀完。

---

## 0. 情境 → 選哪個結構 → 成本 對照表

| 你想做的事 | 首選結構 | 次選 / 替代 | 主要成本與注意 |
|---|---|---|---|
| 計數器（PV、庫存、按讚數） | String（INCR） | Hash field（HINCRBY） | O(1)；注意溢位、非數字報錯 |
| 快取單一物件（JSON 序列化） | String | Hash（拆欄位） | String 整包讀寫；Hash 可局部更新 |
| 物件的多個欄位、局部更新 | Hash | 多個 String key | listpack 省記憶體；欄位過多會轉 hashtable |
| 訊息佇列（可容忍掉訊息） | List（LPUSH/BRPOP） | Stream | List 無 ack、consumer 崩潰會掉訊息 |
| 訊息佇列（要可靠、要重放） | Stream | List + 手動 ack | consumer group、XPENDING 需監控堆積 |
| 最近 N 筆記錄（時間軸、日誌） | List + LTRIM | ZSet（時間戳為 score） | LTRIM O(N)；中間存取 LINDEX O(N) |
| 去重集合、標籤、共同好友 | Set | — | SMEMBERS 大 set 會阻塞 |
| 排行榜（分數排序） | ZSet | — | 見 ZSet 章，最重要 |
| 延遲隊列 / 定時任務 | ZSet（到期時間為 score） | Stream + 外部排程 | ZRANGEBYSCORE 掃到期項 |
| 滑動窗限流 | ZSet（時間戳為 score） | Bitmap / 固定窗計數 | 每次請求要 ZREMRANGEBYSCORE 清窗 |
| 時間序索引 / 範圍查詢 | ZSet | Stream | 用 score 做範圍過濾 |
| 簽到、日活布林標記 | Bitmap | Set | offset 過大會撐爆記憶體 |
| 海量去重計數（UV 基數） | HyperLogLog | Set（精確但貴） | 固定 12KB、0.81% 誤差、不能交集 |
| 附近的人 / 地理範圍查詢 | Geo | — | 底層是 ZSet |
| 多個小計數器打包省記憶體 | Bitfield | 多個 String | 手動管理 offset 與型別 |

**選型三問**：
1. 需要「精確」還是可接受「估算」？→ 精確用 Set/ZSet，估算用 HLL。
2. 需要「可靠交付 + 重放」還是「盡力而為」？→ 可靠用 Stream，盡力用 List。
3. 需要「排序 / 範圍查詢」嗎？→ 需要就 ZSet，不需要用 Set / Hash。

---

## 1. String 字串

Redis 最基礎的型別，value 是二進位安全（binary-safe）的位元組序列，最大 512MB。所有「一個 key 對一個值」的場景都從這裡開始。

### 指令清單

| 指令 | 說明 |
|---|---|
| `SET key value [EX s\|PX ms\|EXAT\|NX\|XX\|GET\|KEEPTTL]` | 設值，可帶過期、條件、回傳舊值 |
| `GET key` / `GETDEL key` / `GETEX key` | 取值 / 取後刪 / 取並改 TTL |
| `SETNX` / `SETEX` / `PSETEX` | 不存在才設 / 帶秒 TTL / 帶毫秒 TTL |
| `MSET` / `MGET` / `MSETNX` | 批次設 / 批次取 / 批次不存在才設 |
| `INCR` / `DECR` / `INCRBY` / `DECRBY` | 整數加減 |
| `INCRBYFLOAT` | 浮點加（無 DECRBYFLOAT，用負數） |
| `APPEND key value` | 尾端追加，回傳新長度 |
| `STRLEN` / `SETRANGE` / `GETRANGE` | 長度 / 覆寫子串 / 取子串 |
| `SUBSTR` | 舊版 GETRANGE 別名 |

### 底層編碼（含轉碼門檻）

String 有三種內部編碼，`OBJECT ENCODING key` 可查：

| 編碼 | 條件 | 特性 |
|---|---|---|
| `int` | value 是可用 long 表示的整數 | 直接存 8 byte long，且會命中**共享整數池** |
| `embstr` | 字串 ≤ 44 bytes | redisObject 與 SDS 一次配置在連續記憶體，讀取快、配置快 |
| `raw` | 字串 > 44 bytes | redisObject 與 SDS 分開配置 |

- **共享整數池**：Redis 啟動時預先建立 0~9999 的整數物件（`OBJ_SHARED_INTEGERS`，預設 10000），多個 key 存相同小整數時共用同一物件以省記憶體。注意：**開啟 maxmemory + LRU/LFU 淘汰時共享整數池會失效**（因為淘汰需要獨立的 idle time）。
- **embstr 一旦被修改就會退化成 raw**：embstr 被設計為唯讀優化，任何 `APPEND`、`SETRANGE` 都會轉成 raw，且**不會轉回去**。
- 44 這個門檻來自：64 byte 記憶體塊 − redisObject（16）− SDS header（3）− `\0`（1）= 44。

相關 config：`set-max-intset-entries` 屬於 Set，不要混淆；String 沒有可調的轉碼門檻，44 是編譯期常數。

### 時間複雜度

| 操作 | 複雜度 |
|---|---|
| GET/SET/INCR/DECR/STRLEN | O(1) |
| APPEND | 平攤 O(1)，可能觸發 realloc |
| SETRANGE/GETRANGE | O(N)，N 為涉及長度 |
| MSET/MGET | O(N)，N 為 key 數 |

### 使用情境

- **計數器**：PV/UV 前置計數、庫存扣減、限流計數、發號器（`INCR` 天生原子）。
- **快取物件**：把 struct 序列化成 JSON/Protobuf 存整包。
- **分散式鎖**：`SET lock token NX PX 30000`，配合 Lua 解鎖。
- **Session / Token**：`SET session:<id> data EX 3600`。

### 踩坑清單

1. **INCR 對非整數字串報錯**：`SET k "abc"; INCR k` → `ERR value is not an integer`。計數器 key 一定要保證只放數字。
2. **INCR 溢位**：超過 int64 範圍（9,223,372,036,854,775,807）會 `ERR increment or decrement would overflow`。極端計數場景要留意。
3. **INCRBYFLOAT 精度**：浮點運算有精度問題，金額類**不要**用它，改用整數存「分」。
4. **APPEND 記憶體碎片**：反覆 APPEND 讓字串成長會不斷 realloc、退化 raw 並產生碎片；大量追加場景考慮 List。
5. **大 value 阻塞**：單一 500MB value 的 GET/SET 會長時間佔用單執行緒，且複製/持久化放大延遲。value 建議控制在數十 KB 內。
6. **SET 帶 EX 才原子**：先 `SET` 再 `EXPIRE` 兩步不是原子，中間崩潰會留下永久 key。永遠用 `SET k v EX n`。

### redis-cli 範例

```bash
# 編碼觀察
127.0.0.1:6379> SET n 100
OK
127.0.0.1:6379> OBJECT ENCODING n
"int"
127.0.0.1:6379> SET s "hello"
OK
127.0.0.1:6379> OBJECT ENCODING s
"embstr"
127.0.0.1:6379> APPEND s " world, this string is definitely longer than forty-four bytes now"
(integer) 71
127.0.0.1:6379> OBJECT ENCODING s
"raw"

# 原子計數器
127.0.0.1:6379> INCR page:home:pv
(integer) 1
127.0.0.1:6379> INCRBY page:home:pv 10
(integer) 11

# 帶過期的原子寫入（分散式鎖雛形）
127.0.0.1:6379> SET lock:order:42 token-abc NX EX 30
OK
127.0.0.1:6379> SET lock:order:42 token-xyz NX EX 30
(nil)
```

### Go 範例

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	// 原子計數器
	pv, err := rdb.Incr(ctx, "page:home:pv").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("pv =", pv)

	// 帶 TTL 的條件寫入（NX）
	ok, err := rdb.SetNX(ctx, "lock:order:42", "token-abc", 30*time.Second).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("acquired lock:", ok)

	// 觀察編碼
	enc, _ := rdb.Do(ctx, "OBJECT", "ENCODING", "page:home:pv").Text()
	fmt.Println("encoding:", enc) // int
}
```

### 專案流程說明：實作「文章瀏覽計數器 + 每小時歸檔」

1. 每次請求 `INCR article:<id>:pv:current`，O(1) 不阻塞。
2. 排程每小時執行：用 `GETDEL article:<id>:pv:current` 原子取值並歸零（避免讀後清空的競態）。
3. 把取回的增量寫入資料庫累加，或 `INCRBY article:<id>:pv:total <delta>`。
4. 熱門文章的 `total` 命中 int 編碼且可能在共享整數池外，記憶體可控。
5. 若要防止某文章計數 key 永久佔用，冷資料可設 TTL 讓其自然淘汰。

### 大 value 抓法 與 原子過期

**大 value 阻塞**：單線程下，一個超大 value（如 500MB）的 `GET`/`SET` 會長時間佔住 event loop，讓其他 client 全部等；複製（replication）、持久化（AOF 寫、RDB fork 的 COW）、甚至 `DEL` 都被放大。建議單 value 控制在**數十 KB 內**；大東西拆分、或丟物件儲存（S3/MinIO）只在 Redis 存參照（Claim Check）。

怎麼抓大 key：

```bash
redis-cli --bigkeys      # 底層用 SCAN(不阻塞,可跑 prod)；每型別找最大 key
                         # ⚠️ 量的是「元素數量」不是記憶體，List/Hash 可能誤判
redis-cli --memkeys      # 較新版本：直接按記憶體找，比 --bigkeys 準
```

```
MEMORY USAGE k            # 單一 key 實際佔幾 bytes (含 overhead)
MEMORY USAGE k SAMPLES 0  # 大集合精確算(0=全部)
STRLEN k                  # String bytes；LLEN/HLEN/SCARD/ZCARD/XLEN 看各型別元素數
OBJECT ENCODING k         # raw = 大字串
```

離線最徹底：用 `redis-rdb-tools` 掃 RDB 檔算每個 key 記憶體，完全不碰線上。抓到後：大字串拆分或存參照；大集合用 `HSCAN`/`SSCAN` 分批，別 `HGETALL`/`SMEMBERS` 全撈；刪大 key 用 **`UNLINK`（背景刪）** 不用 `DEL`（阻塞）。

**原子過期：`SET k v EX n`，別分兩步**

「先 `SET` 再 `EXPIRE`」兩步之間崩潰，會留下**無 TTL 的永久 key**（記憶體洩漏）。用 `SET` 內建的 `EX` 一步原子完成：

| flag | 意思 |
| --- | --- |
| `EX n` / `PX n` | n **秒** / **毫秒**後過期 |
| `EXAT` / `PXAT` | 在某 unix 秒 / 毫秒時間點過期 |
| `KEEPTTL` | 保留原本 TTL 不重設 |
| `NX` | key **不存在**才設（搶佔 — 分散式鎖用） |
| `XX` | key **已存在**才設（更新現有） |

```
SET k v EX 60                    # 值 + 過期，原子
SET lock:x <token> NX EX 30      # 分散式鎖：不存在才設 + 30 秒防死鎖
```

```go
// go-redis：第四參數帶 expiration 即走原子 SET ... EX
rdb.Set(ctx, "k", "v", 60*time.Second)
rdb.SetNX(ctx, "lock:x", token, 30*time.Second)   // SET ... NX EX
rdb.SetXX(ctx, "k", "v", 60*time.Second)          // SET ... XX EX
```

**⚠️ 例外：`INCR` 沒有原子 `EX`。** 只有 `SET` 有內建 `EX`；`INCR`/`HSET`（7.4 前）都沒有。所以「計數 + 首次設過期」不能寫成 `INCR` 再 `EXPIRE`（同樣兩步不原子），要用 **Lua** 綁（見下一節「計數器深入」的限流範例）。

| 操作 | 原子帶過期 |
| --- | --- |
| SET | `SET k v EX n` ✅ 內建（go-redis `Set(..., ttl)`） |
| SETNX（鎖） | `SET k v NX EX n` ✅（go-redis `SetNX(..., ttl)`） |
| INCR（計數） | ❌ 無內建 → **Lua 綁 INCR+EXPIRE** |
| HSET | ❌ 無（Hash field 過期要 7.4+ `HEXPIRE`） |

### 計數器深入：原子性 / 帶窗 / 分片 / 熱 key

計數器是 String 最常見的用途，核心只有一件事：**原子性**。

**為什麼 `INCR` 能免鎖免事務就正確**

`INCR` 是原子的「讀-改-寫」一步完成，對比自己用 `GET` + `SET` 兩步的錯誤寫法：

```
# ❌ 錯：GET + SET 兩步，併發會 lost update
v = GET counter      # 連線 A、B 同時讀到 100
v = v + 1            # 各自算出 101
SET counter 101      # 兩者都寫 101 → 少算一次

# ✅ 對：INCR 一步原子
INCR counter         # A→101、B→102，序列執行不會丟
```

因為 Redis **單線程序列執行指令**（見 `docs/00`），`INCR` 天生不可能被插隊，所以不需要鎖或事務就正確。這是計數器能放心用 Redis 的根本原因。`HINCRBY`、`INCRBYFLOAT`、`ZINCRBY` 同理。

**帶時間窗計數（限流 / 日活）**

「先 `INCR` 再 `EXPIRE`」兩步之間若崩潰，key 會**無 TTL 永不過期**。用 Lua 把兩步綁成原子：

```lua
-- 只在第一次（計數從 0 變 1）時設過期
local c = redis.call('INCR', KEYS[1])
if c == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return c
```

```go
// go-redis v9
var rateScript = redis.NewScript(`
local c = redis.call('INCR', KEYS[1])
if c == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
return c`)

c, _ := rateScript.Run(ctx, rdb, []string{"rate:uid:9:1min"}, 60).Int()
if c > 100 {
    // 每分鐘超過 100 次，擋下
}
```

這個「INCR + EXPIRE 綁原子」就是 **固定窗限流** 的核心。`INCR` 和 `EXPIRE` 之所以成對，是因為：光 `INCR` 是永遠往上漲的計數器；加 `EXPIRE` 才變成「每個時間窗自動歸零」的**每窗計數器**，而每窗計數器最常見的用途就是「比對上限 → 限流」。

**實際情境（都是「每 X 時間最多 Y 次」）：**

| 場景 | key 設計 | 規則 |
| --- | --- | --- |
| API 限流 | `rate:uid:9:1min` | 每次請求 `INCR`，>100 擋（每人每分鐘 100 次） |
| 登入防爆破 | `login:fail:ip:x:15min` | 失敗才 `INCR`，>5 鎖 15 分鐘 |
| OTP/簡訊防濫發 | `otp:phone:x:1hour` | 發一次 `INCR`，>3 擋（每小時最多 3 封） |
| 防刷 / 防濫用 | `action:uid:x:1min` | 任何「每 X 時間最多 Y 次」的限制 |

也可以「只計數不擋」當**每窗指標**：`INCR requests:1min` / `INCR errors:5xx:1min` 配 `EXPIRE` 自動清舊窗，監控系統定時讀值畫圖（QPS、每分鐘錯誤數）。

**TTL 要「設一次」，別每次延長。** 固定窗只在 `count==1` 設 `EXPIRE`，讓窗自然到期重置。若每次請求都 `EXPIRE key 60` 續期 → 持續有流量時 key 永不過期、計數永不歸零 → 一旦超限就把正常用戶**永久鎖死**（限流 bug）。「每次延長 TTL」是 session / 心跳那種 keep-alive 用的（活著就續命），不是限流計數。真正的精準滑動窗要用 ZSet 存時間戳（見第 5 章），不是靠延長 EXPIRE 硬湊。

**limit 檢查放哪？三種寫法（一種有 race 陷阱）：**

| 寫法 | 安全？ | 擋下會 INCR？ | 說明 |
| --- | --- | --- | --- |
| 外層先 `GET` 檢查 → 再 `INCR` | ❌ **race** | — | GET 到 INCR 之間有空隙，兩個請求同時讀到 4、都判「未滿」、都 INCR → 超收（TOCTOU） |
| 先 `INCR` → 用**回傳值**在外層判斷 | ✅ | 會（count 續漲） | `INCR` 原子已把請求序列化，每個拿到唯一序號，基於回傳值判斷安全 |
| `GET`+檢查+`INCR` 全進 Lua | ✅ | 不會（count 封頂） | 要「擋下就不 INCR」時的唯一安全做法（檢查與 INCR 必須同一原子） |

「擋下不 INCR」的 Lua 版：

```lua
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local c = tonumber(redis.call('GET', KEYS[1]) or '0')
if c >= limit then return -1 end            -- 已達上限，直接擋，不 INCR
c = redis.call('INCR', KEYS[1])
if c == 1 then redis.call('EXPIRE', KEYS[1], window) end
return c
```

原則：**檢查若基於「另一次獨立的 GET」，就必須跟 INCR 綁進同一 Lua 原子**；檢查若基於「INCR 自己的原子回傳值」，放外層也安全。千萬別「先 GET 檢查、沒超過再呼叫 Lua INCR」——那是把檢查和 INCR 拆成兩次操作，中間會 race。

**登入防爆破：兩種鎖定設計（含原子寫法）**

「失敗 N 次鎖 M 分鐘」有兩種做法，差在「M 分鐘」從何時起算：

*設計 A：單一 key，第一次失敗就給 TTL*（簡單，多數場景夠）。計數 key 本身就是窗；「鎖定」= 窗內 count > 上限。缺點：TTL 從**第一次失敗**起算，實際鎖定時長可能略短。原子性靠「INCR 原子 + 回傳值判斷」即可：

```lua
-- KEYS[1]=fail 計數；ARGV[1]=窗秒數
local c = redis.call('INCR', KEYS[1])
if c == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
return c        -- 外層判斷 c > 5 → 擋（基於 INCR 原子回傳值，安全）
```

登入**成功**要 `DEL login:fail:ip:x` 清計數（單指令原子），否則正常用戶打錯幾次後計數殘留、下次很快被鎖。

*設計 B：計數 key + 另開 lock key*（精準鎖滿固定時間）。超過門檻才 `SET` 一個獨立的 15 分 lock 旗標，「M 分鐘」從**超門檻那一刻**起算。因為含「檢查鎖 + INCR + 開鎖 + 歸零」多步，**必須整包進一個 Lua** 才原子：

```lua
-- KEYS[1]=fail 計數, KEYS[2]=lock 旗標
-- ARGV[1]=計數窗秒, ARGV[2]=上限, ARGV[3]=鎖定秒數
if redis.call('EXISTS', KEYS[2]) == 1 then
  return -1                                     -- 已鎖定，直接擋
end
local c = redis.call('INCR', KEYS[1])
if c == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
if c >= tonumber(ARGV[2]) then
  redis.call('SET', KEYS[2], 1, 'EX', ARGV[3])  -- 超門檻：開鎖
  redis.call('DEL', KEYS[1])                     -- 計數歸零
  return -1
end
return c
```

⚠️ **Cluster**：KEYS[1]、KEYS[2] 要落**同一 slot** 才能同腳本操作 → 用 hash tag `login:fail:{ip}`、`login:locked:{ip}`（大括號內相同 → 同 slot）。

| | A：單 key TTL | B：計數 + 另開 lock |
| --- | --- | --- |
| key 數 | 1 | 2 |
| 「M 分鐘」從何算 | 第一次失敗 | 超門檻那刻（精準） |
| 原子做法 | INCR 原子 + 外層判回傳值 | 多步 → **整包 Lua** |
| 適合 | 一般 | 要精準鎖定期 / 高安全 |

進階：production 常用 **漸進式鎖定**（第 1 次觸發鎖 1 分、第 2 次 5 分、第 3 次 30 分），用另一個 key 記「觸發次數」決定鎖多久，嚇阻力更強。

**熱 key 分片（sharded counter）**

單一計數器被狂打（例如全站總 PV），那個 key 所在的 shard CPU 會被打滿。解法：把一個邏輯計數器拆成 N 個實體 key，寫入隨機打散、讀取時加總：

```
# 寫：隨機打散到 10 個分片
INCR pv:total:shard:{0..9}     # 每次挑一個 shard

# 讀：一次取回全部再加總
MGET pv:total:shard:0 ... pv:total:shard:9   # 於應用層 sum
```

代價是「即時精確讀」要多做一次加總，換得寫入壓力分散到多個 key/shard。適合寫多讀少的超熱計數。

**實際情境會用在哪**：核心條件是「單一邏輯計數器被極高併發寫入狂打，那個 key 的 shard 變 CPU 熱點」。

| 場景 | 為何是超熱 key |
| --- | --- |
| 全站總 PV / 總請求數 | 每個請求都 `INCR` 同一個 key |
| 爆紅直播 / 短影音的即時觀看數、按讚數 | 百萬人同時對同一支內容的一個 key +1 |
| 廣告曝光 / 點擊全域計數 | 高頻曝光全打同一計數 |
| 全域限流計數 | 整個系統流量打同一個 rate key |
| 秒殺 / 搶購的全域參與人數 | 瞬間巨量併發（注意：庫存扣減要另解，不能只靠分片） |

**不適用**：需要「即時精確讀 + 強一致」的（如庫存餘量），分片會讀到過時值。

**發號器（分散式 ID）**

```
INCRBY order:id 100     # 一次領一段（步長 100），減少 Redis 往返
```

服務本地用完這 100 個號再回頭領下一段，Redis 壓力大降。

**踩坑補充**

- **溢位**：int64 上限約 9.2×10¹⁸，超過 `INCR` 回 `ERR ... would overflow`。
- **金額別用 `INCRBYFLOAT`**：浮點有精度誤差，用整數存「分」再 `INCRBY`。
- **持久化**：純記憶體計數重啟即失，要保留就開 AOF，或定期用 `GETDEL` 落 DB（見上面歸檔流程）。

---

## 2. Hash 雜湊

一個 key 底下一組 field-value，適合表達「一個物件的多個屬性」。相較於用多個 String key，Hash 更省記憶體且能局部更新。

### 指令清單

| 指令 | 說明 |
|---|---|
| `HSET key f v [f v ...]` / `HSETNX` | 設欄位 / 不存在才設 |
| `HGET` / `HMGET` / `HGETALL` | 取單欄 / 多欄 / 全部 |
| `HDEL` / `HEXISTS` / `HLEN` | 刪欄 / 是否存在 / 欄位數 |
| `HKEYS` / `HVALS` | 所有 field / 所有 value |
| `HINCRBY` / `HINCRBYFLOAT` | 欄位整數 / 浮點加 |
| `HSCAN key cursor [MATCH] [COUNT] [NOVALUES]` | 游標遍歷（大 hash 必用） |
| `HRANDFIELD key [count [WITHVALUES]]` | 隨機欄位 |
| `HEXPIRE` / `HPEXPIRE` / `HTTL` / `HPERSIST`（7.4+） | **欄位級 TTL** |

### 底層編碼（含轉碼門檻）

| 編碼 | 條件 | config |
|---|---|---|
| `listpack` | 欄位數 ≤ 門檻 **且** 每個 field/value 長度 ≤ 門檻 | `hash-max-listpack-entries`（預設 128）、`hash-max-listpack-value`（預設 64） |
| `hashtable` | 任一超過門檻 | — |

- listpack 是連續記憶體的緊湊結構（Redis 7.0 取代舊的 ziplist），小 hash 用它非常省記憶體，代價是查詢為 O(N) 線性掃描——但 N 很小所以實務上很快。
- **一旦轉成 hashtable 就不會轉回 listpack**，即使刪除欄位讓數量降回門檻以下。
- 舊版 config 名稱是 `hash-max-ziplist-entries` / `hash-max-ziplist-value`，Redis 7 起用 listpack 名稱（舊名仍相容）。

### 時間複雜度

| 操作 | 複雜度 |
|---|---|
| HSET/HGET/HDEL/HEXISTS/HINCRBY | O(1)（hashtable）；listpack 為 O(N) 但 N 小 |
| HMGET/HDEL 多欄 | O(N)，N 為欄位數 |
| HGETALL/HKEYS/HVALS | O(N)，N 為總欄位數 |
| HSCAN | 每次 O(1)~O(COUNT) |

### 使用情境

- **物件欄位儲存**：user profile（name、email、age…），可只更新單一欄位。
- **購物車**：`cart:<uid>` 的 field 是商品 id、value 是數量，`HINCRBY` 加減。
- **計數聚合**：一個 key 存一個維度下多個子計數（如各狀態訂單數）。
- **配置字典**：feature flag 集合。

#### 情境展開（含常見設計問題）

**Hash 不能巢狀**：field 的 value 只能是字串，**不能塞另一個 hash**。要表達「巢狀」有三招：

```
# a. field 名編碼層級（扁平化，最常用）
HSET user:1 addr:home:city 台北 addr:work:city 新竹
# b. 拆成獨立 key（命名慣例串起來）
HSET user:1 name alice
HSET user:1:addr:home city 台北 zip 100
# c. value 存 JSON 字串（有巢狀，但無法原子更新子欄位；深層巢狀改用 RedisJSON 模組）
HSET user:1 address '{"home":{"city":"台北"}}'
```

**購物車分出貨方式**（如 24h 倉 vs 廠商出貨）：**分兩個 hash** 最乾淨，頁面呼叫兩個：

```
HSET cart:uid9:24h    prod123 2  prod456 1     # 24h 出貨
HSET cart:uid9:vendor prod789 3                # 廠商出貨
```

> ⚠️ 真實電商購物車通常比「field→數量」複雜（要存價格快照、規格、促銷），常改成 `HSET cart:uid9 prod123 '{"qty":2,"price":990}'`（value 存 JSON），或整個購物車存 **DB 當本體、Redis 當熱快取**。單純 Hash 是最簡版。

**百萬用戶各一個購物車 hash 會爆記憶體嗎？** 一個小購物車（listpack）約 100 bytes–1KB，1M 約 500MB–1GB，對正常 Redis **不算爆**。但要規劃：① 加 **TTL** 讓棄置購物車過期（真實只有一小部分用戶當下有活躍車）；② 登入用戶購物車存 **DB 當 source of truth，Redis 當快取**，冷的自然淘汰；③ 設 `maxmemory` + `allkeys-lru` 兜底。沒規劃地讓 1M 永久 hash 堆著才會慢慢吃爆。

**計數聚合**：一個 key 存「一個維度下的多個子計數」，`HINCRBY` 原子加、`HGETALL` 一次讀整組：

```
HSET   order:stats:2026-07 pending 120 paid 3400 shipped 2900 cancelled 45
HINCRBY order:stats:2026-07 paid 1        # 有訂單付款 → paid +1
HGETALL order:stats:2026-07               # 一次讀整個維度
```

**配置字典 / feature flag**：一個 hash 存一組設定，改單項用 `HSET`、啟動時 `HGETALL` 一次載入 app：

```
HSET config:features new_checkout on dark_mode off
HGET config:features new_checkout         # "on"
HSET config:features dark_mode on          # 開關單一 flag
HSET config:tenant:42 max_upload_mb 50 theme dark locale zh-TW   # 每租戶配置
```

**實際情境會用在哪**：user/商品 profile（部分欄位更新）、購物車、每日/每商家統計聚合（報表雛形）、feature flag 與租戶配置字典、session 屬性包。核心優勢：**相關欄位歸一個 key、單欄位原子加減（HINCRBY）、一次 HGETALL 讀整組**，比「每個欄位開一個 String key」省 key 又整齊。

### 踩坑清單

1. **HGETALL 對大 hash 阻塞**：欄位上萬時一次拉回全部會阻塞單執行緒且吃頻寬，改用 `HSCAN` 分批，或 `HMGET` 只取需要的欄位。
2. **big key 風險**：一個 hash 塞百萬欄位是典型 big key，刪除（DEL）時會阻塞，應用 `HSCAN` + `HDEL` 漸進刪，或 `UNLINK` 異步刪整個 key。
3. **轉碼不可逆 + 記憶體跳變**：跨過門檻後記憶體佔用明顯上升且不會回退。設計時預估欄位規模，決定要不要調 config。
4. **Hash 無法對「整個 key」以外設 TTL（7.4 前）**：7.4 之前 TTL 只能設在 key 層級，無法讓單一 field 過期；需要欄位級過期就得升到 7.4 用 `HEXPIRE`。
5. **HINCRBY 同樣有非數字報錯與溢位**：與 String 的 INCR 相同限制。
6. **NOVALUES 需要 7.4+**：`HSCAN ... NOVALUES` 只回 field 不回 value，舊版沒有。
7. **Hash 是無序的，別依賴 field 順序**：Hash = map / 字典語意，`HGETALL`/`HSCAN` 回傳順序**不保證**。陷阱：小 hash 用 listpack 編碼（連續陣列、依序 append）**剛好保留插入序**，看起來「有序」；但欄位一多轉成 hashtable 順序就完全亂掉。**今天靠這假象寫的邏輯，資料長大就爆**。要順序請用對的結構——按分數/優先級用 **ZSet**（score 排序）、按插入序用 **List** / **Stream**。

各型別有序性對照：

| 型別 | 有序？ | 依什麼排 |
| --- | --- | --- |
| Hash | **無序** | 無（field→value 對應） |
| **ZSet** | **有序** | 按 score（同分則 member 字典序） |
| List | 有序 | 插入位置（index） |
| Set | **無序** | 無 |
| Stream | 有序 | 訊息 ID（時間遞增） |

### redis-cli 範例

```bash
127.0.0.1:6379> HSET user:1001 name "Alice" age 30 city "Taipei"
(integer) 3
127.0.0.1:6379> HGET user:1001 name
"Alice"
127.0.0.1:6379> HINCRBY user:1001 age 1
(integer) 31
127.0.0.1:6379> OBJECT ENCODING user:1001
"listpack"

# 欄位級 TTL（Redis 7.4+）：讓 email 欄位 60 秒後過期
127.0.0.1:6379> HEXPIRE user:1001 60 FIELDS 1 email
1) (integer) 1
127.0.0.1:6379> HTTL user:1001 FIELDS 1 email
1) (integer) 60

# 大 hash 安全遍歷
127.0.0.1:6379> HSCAN user:1001 0 COUNT 100 NOVALUES
```

### Go 範例

對應前面四個使用情境：

```go
ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

// 情境 1：物件欄位儲存（user profile）— 固定 schema，欄位有界
rdb.HSet(ctx, "user:1001", "name", "Alice", "age", 30, "city", "Taipei")
rdb.HIncrBy(ctx, "user:1001", "age", 1)        // 只更新單一欄位
rdb.HExpire(ctx, "user:1001", 60*time.Second, "email") // 欄位級 TTL（7.4+）

// 情境 2：購物車 — 分出貨方式各一個 hash，field=商品 id、value=數量
rdb.HIncrBy(ctx, "cart:uid9:24h", "prod123", 2)     // 加 2 件
rdb.HIncrBy(ctx, "cart:uid9:vendor", "prod789", 1)

// 情境 3：計數聚合 — 一個 key 多子計數，原子加、一次讀整組
rdb.HIncrBy(ctx, "order:stats:2026-07", "paid", 1)
stats, _ := rdb.HGetAll(ctx, "order:stats:2026-07").Result() // 固定維度，安全
_ = stats

// 情境 4：配置字典 / feature flag
rdb.HSet(ctx, "config:features", "new_checkout", "on", "dark_mode", "off")
flag, _ := rdb.HGet(ctx, "config:features", "new_checkout").Result()
_ = flag
```

**HGETALL 安全與否看「欄位有沒有上界」，不是看 listpack。** 固定 schema（profile、config、固定維度統計）欄位由程式寫死、不會無限長 → HGETALL 永遠安全。欄位會隨資料/流量成長的（以 id、時間戳當 field）→ **一律 HSCAN，絕不 HGETALL**（正因為它可能「突然變大」）。不確定就用 `HLEN`（O(1)）當保險絲：

```go
func readHash(ctx context.Context, rdb *redis.Client, key string) (map[string]string, error) {
	n, err := rdb.HLen(ctx, key).Result() // O(1)，幾乎零成本
	if err != nil {
		return nil, err
	}
	if n <= 500 { // 有界 → 一次撈
		return rdb.HGetAll(ctx, key).Result()
	}
	// 太大 → 游標分批（listpack/hashtable 同一段 code，見上一節）
	out := make(map[string]string)
	var cursor uint64
	for {
		kvs, next, err := rdb.HScan(ctx, key, cursor, "*", 500).Result()
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(kvs); i += 2 {
			out[kvs[i]] = kvs[i+1]
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return out, nil
}
```

> 別用 `OBJECT ENCODING` 判（門檻 128 太低且不對：500 field 的 hashtable HGETALL 也 OK，用編碼判會誤殺）。要判大小用 `HLEN`（筆數）或 `MEMORY USAGE`（bytes）。

**實測 HGETALL vs HSCAN 的差距**（10 萬 field 大 hash，用 SLOWLOG 量 server 端純執行時間，`labs/01-string-counter` demo 6）：

| 操作 | server 端 |
| --- | --- |
| `HGETALL`（一次全撈） | 一次阻塞 **≈ 37 ms**（整段期間所有 client 都等） |
| `HSCAN ... COUNT 1000`（100 批全掃） | 加總 **≈ 80 ms**、單批最大 **≈ 1.7 ms**、平均 **≈ 0.8 ms/批** |

**反直覺但重要**：HSCAN 的 **server 總 CPU 反而「更多」**（80 vs 37 ms，因為 100 次呼叫各有游標開銷、重複遍歷 hashtable）。它**不是更省**——換到的是「**單段阻塞很短**」：HGETALL 一次卡 37ms，HSCAN 最差單批才 ~1.7ms（~22 倍小），批與批之間讓其他 client 插進來。

所以取捨真相是：**HSCAN 花更多總 CPU，但從不長時間獨佔 server**。線上寧可多花一點總 CPU，也不要一次 O(N) 把整台 Redis 卡住。用 SLOWLOG 量（不是 client 端計時，那會混進網路/GC 噪音）：`CONFIG SET slowlog-log-slower-than 0` 後 `SLOWLOG RESET` → 各跑一次 → `SLOWLOG GET` 看第 3 欄微秒。

### HSCAN 深入：listpack vs hashtable 行為，與「編碼無關」的迴圈

初次玩 HSCAN 常撞到這個：對**小 hash** 下 `HSCAN key 0 COUNT 1`，卻**一次全回、cursor 直接是 0**，`COUNT` 好像被忽略。

原因是編碼：

- **listpack（小 hash）**：底層是一塊連續陣列，沒有可分批遍歷的結構 → Redis **一次全回 + cursor 0**，`COUNT` 對它**無效**。傳任何 cursor 都當「從頭全給」。
- **hashtable（大 hash）**：欄位超過 `hash-max-listpack-entries`（預設 128）或單一 value 超 `hash-max-listpack-value`（64 bytes）就轉成真正的雜湊表，這時 `COUNT` 才有作用、cursor 才會分批前進。

```
127.0.0.1:6379> OBJECT ENCODING fruits     # 4 個欄位 → "listpack" → HSCAN 一次全回
127.0.0.1:6379> eval "for i=1,200 do redis.call('HSET','big','f'..i,i) end" 0
127.0.0.1:6379> OBJECT ENCODING big        # 超過 128 → "hashtable"
127.0.0.1:6379> HSCAN big 0 COUNT 10       # cursor 非 0，開始分批
1) "17"
2) 1) "f1" ...
```

**但上層 Go code 完全不用判斷是哪種編碼。** cursor 協議對客戶端透明——你的迴圈只認一條規則「**cursor 回 0 才停**」，兩種編碼都滿足：listpack 第一次 `next` 就是 0（跑一圈就 break）、hashtable `next` 非 0（跑多圈到 0）。**同一段迴圈走天下**：

```go
var cursor uint64
for {
    kvs, next, err := rdb.HScan(ctx, key, cursor, "*", 100).Result()
    if err != nil {
        return err
    }
    for i := 0; i < len(kvs); i += 2 {
        field, value := kvs[i], kvs[i+1]
        _ = field
        _ = value
    }
    cursor = next
    if cursor == 0 { // ← 唯一結束條件，listpack/hashtable 通用
        break
    }
}
```

或用 go-redis 的 `HScan(...).Iterator()`，連 cursor 迴圈都封裝掉。

兩條紀律（跟編碼無關）：**① 永遠用 `cursor==0` 判斷結束**（別用回傳筆數，某批可能空但沒掃完）；**② 別假設每批固定 N 筆**（`COUNT` 是提示不是保證，就算 hashtable 也可能多、可能少）。業務邏輯**不需要碰 `OBJECT ENCODING`**，那只在除錯/記憶體分析時看。

**實際情境會用在哪**：大 hash 全量匯出／備份、批次清理遷移（`MATCH` 篩 field 逐批 `HDEL`）、大 user profile／設定表遍歷、邊掃邊統計、只取 field 名做列表（`NOVALUES`，7.4+）。核心紀律：**線上遍歷大集合一律用 SCAN 家族**（`HSCAN`/`SSCAN`/`ZSCAN`/`SCAN`），別用 `HGETALL`/`SMEMBERS`/`KEYS` 全撈把單執行緒卡住。

### 專案流程說明：實作「使用者資料快取 + 敏感欄位自動過期」

1. 讀資料庫後 `HSET user:<id>` 寫入 name、email、phone、profile 各欄位。
2. 敏感欄位（如一次性驗證碼 `otp`）用 `HEXPIRE user:<id> 300 FIELDS 1 otp`，300 秒後只讓該欄位消失、其餘保留。
3. 前端只更新暱稱時 `HSET user:<id> name <new>`，不需重寫整個物件。
4. 整個 user key 設一個較長的兜底 TTL（如 1 天），避免冷資料常駐。
5. 監控：對 `user:*` 定期 `HLEN` 抽樣，若某 key 欄位異常膨脹則告警（防 big key）。

---

## 3. List 列表

雙向鏈結的有序元素序列，兩端操作 O(1)。天生適合佇列、堆疊、最近 N 筆。

### 指令清單

| 指令 | 說明 |
|---|---|
| `LPUSH` / `RPUSH` / `LPUSHX` / `RPUSHX` | 左/右推入（X 版本需 key 已存在） |
| `LPOP` / `RPOP key [count]` | 左/右彈出 |
| `BLPOP` / `BRPOP key... timeout` | 阻塞式彈出（佇列消費核心） |
| `BLMOVE` / `LMOVE src dst LEFT\|RIGHT LEFT\|RIGHT` | 原子搬移（可靠佇列） |
| `BRPOPLPUSH`（已棄用，用 BLMOVE） | 同上舊寫法 |
| `LRANGE key start stop` | 範圍取值 |
| `LINDEX` / `LSET` | 依索引取 / 設 |
| `LLEN` / `LINSERT` / `LREM` | 長度 / 插入 / 刪除元素 |
| `LTRIM key start stop` | 只保留區間（裁切） |
| `LPOS key element` | 找元素位置 |

### 底層編碼（含轉碼門檻）

Redis 3.2 起 List 統一用 **quicklist**：一個雙向鏈結串起多個 listpack 節點（每個節點是一段連續記憶體）。

| 相關 config | 預設 | 說明 |
|---|---|---|
| `list-max-listpack-size`（= `list-max-ziplist-size`） | 128 | 單一 quicklist 節點內 listpack 的最大條目數；負值表示按大小限制（-2 = 8KB） |
| `list-compress-depth` | 0 | 兩端各保留幾個節點不壓縮（0 = 不壓縮），中間節點用 LZF 壓縮省記憶體 |

- Redis 7.2 起小 List 也可能直接是單一 `listpack` 編碼，超過門檻才變 `quicklist`。用 `OBJECT ENCODING` 觀察。

### 時間複雜度

| 操作 | 複雜度 |
|---|---|
| LPUSH/RPUSH/LPOP/RPOP/LLEN | O(1) |
| LINDEX / LSET | O(N)（要走鏈結到該位置） |
| LRANGE | O(S+N)，S 為起點偏移 |
| LTRIM | O(N)，N 為被移除元素數 |
| LINSERT / LREM | O(N) |

### 使用情境

- **簡單訊息佇列**：生產者 `LPUSH`，消費者 `BRPOP` 阻塞等待，天然的先進先出（從對向操作）。
- **最近 N 筆**：`LPUSH` + `LTRIM key 0 N-1` 永遠只留最新 N 筆（動態、時間軸、瀏覽歷史）。
- **堆疊**：同端 push/pop。
- **可靠佇列**：`BLMOVE queue processing LEFT RIGHT` 把訊息搬到「處理中」清單，處理完再刪，崩潰可回收。

### 踩坑清單

1. **無 ack，consumer 崩潰掉訊息**：`BRPOP` 一旦彈出訊息就離開 Redis，若消費者在處理途中崩潰，這則訊息永久遺失。要可靠就用 `BLMOVE` 搬到 processing 清單模擬 ack，或直接用 Stream。
2. **LINDEX / LSET O(N)**：List 不是陣列，隨機存取要走鏈結。頻繁按索引存取請改用別的結構（ZSet / 陣列型 String）。
3. **LRANGE 大範圍阻塞**：`LRANGE key 0 -1` 對百萬元素會阻塞且吃頻寬，分頁取。
4. **big key**：長度失控的 List 是 big key，`DEL` 阻塞，用 `LTRIM` 控長度或 `UNLINK` 異步刪。
5. **BRPOP 的 key 順序語意**：`BRPOP k1 k2 0` 會依序檢查 k1、k2，回傳第一個有資料的；且回傳值包含 key 名，別忘了解析。
6. **timeout=0 是無限等待**：阻塞指令 timeout 設 0 代表永久阻塞，注意連線池佔用。

### redis-cli 範例

```bash
# 佇列：生產與消費
127.0.0.1:6379> LPUSH tasks "job-1" "job-2"
(integer) 2
127.0.0.1:6379> BRPOP tasks 5
1) "tasks"
2) "job-1"

# 最近 5 筆瀏覽記錄
127.0.0.1:6379> LPUSH history:u1 "page-a"
(integer) 1
127.0.0.1:6379> LPUSH history:u1 "page-b"
(integer) 2
127.0.0.1:6379> LTRIM history:u1 0 4
OK
127.0.0.1:6379> LRANGE history:u1 0 -1
1) "page-b"
2) "page-a"

# 可靠佇列：搬到處理中
127.0.0.1:6379> BLMOVE tasks tasks:processing LEFT RIGHT 5
```

### Go 範例

```go
ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

// 生產者
rdb.LPush(ctx, "tasks", "job-1", "job-2")

// 消費者（阻塞 5 秒）
res, err := rdb.BRPop(ctx, 5*time.Second, "tasks").Result()
if err == redis.Nil {
	fmt.Println("timeout, no job")
} else if err != nil {
	panic(err)
} else {
	// res[0] = key 名, res[1] = 值
	fmt.Println("got job:", res[1])
}

// 最近 N 筆
rdb.LPush(ctx, "history:u1", "page-b")
rdb.LTrim(ctx, "history:u1", 0, 4) // 只留最新 5 筆

// 可靠佇列：原子搬移
job, err := rdb.BLMove(ctx, "tasks", "tasks:processing", "LEFT", "RIGHT", 5*time.Second).Result()
_ = job
```

### 專案流程說明：實作「可靠背景工作佇列」

1. 生產者 `LPUSH queue <payload>` 投遞任務。
2. 消費者用 `BLMOVE queue processing:<worker-id> LEFT RIGHT 0` 原子地把任務移到自己的處理中清單——即使拿到後立刻崩潰，任務仍在 processing 清單內。
3. 處理成功後 `LREM processing:<worker-id> 1 <payload>` 移除。
4. 監控程序定期掃描各 `processing:*` 清單，把停留過久的任務用 `LMOVE` 搬回主 queue 重試（實現 at-least-once）。
5. 主 queue 若允許有界，可用 `LTRIM` 或在生產端檢查 `LLEN` 做背壓。
6. 若需求升級為「多消費組 + 精確 ack + 重放」，遷移到 Stream（見第 6 章）。

### 三個常見疑問澄清

**(1) `LPUSH` + `LTRIM` 保留「最新 N 筆」是什麼意思**

只想留最近 N 筆（最近瀏覽、最近搜尋、動態時間軸），舊的自動丟。每次來新資料：推到頭 + 砍到只剩 N 個。

```
LPUSH recent:views:uid9 <新item>     # 從左(頭)塞，最新的永遠在 index 0
LTRIM recent:views:uid9 0 99         # 只保留 index 0~99（最新 100 筆），其餘刪掉
```

`LPUSH` 從左塞 → 最新在 index 0；`LTRIM key 0 99` 只留前 100 個。每次「LPUSH + LTRIM」→ list **永遠是最新 100 筆、有界、自動淘汰舊的**，記憶體不會無限長。**實際情境**：使用者最近瀏覽紀錄、最近搜尋關鍵字、最新 N 則通知/動態、首頁「最新上架」。

**(2) `LINDEX`/`LSET` 為什麼 O(N)：「隨機存取走鏈結」**

List 底層是**鏈結串列（quicklist）**，不是陣列。取任意 index i：

| | 陣列 | 鏈結串列 |
| --- | --- | --- |
| 取 index i | `arr[i]` 直接算位址 → **O(1)** | 從頭一個一個走到第 i 個 → **O(i)=O(N)** |

「隨機存取」= 跳到任意位置。陣列能直接跳（算偏移量），鏈結串列**只能從頭沿鏈往下數**。所以 `LINDEX k 5000`（取第 5000 個）、`LSET k 5000 x`（改第 5000 個）都要走到那 → **O(N)**。**例外：頭尾 O(1)**——Redis 對 index 0 / -1 有優化，`LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`LINDEX 0`/`LINDEX -1` 都 O(1)。

**結論**：List 適合**頭尾操作**（當 queue/stack，O(1)），**不適合中間隨機存取**（O(N)）。要「用 index 隨機取任意位置」別用 List。

**(3) 佇列可靠度：`BRPOP` 無 ack vs `BLMOVE` 可回收 vs Stream**

同一個 List，兩種佇列寫法一爛一好，加上 Stream 共三級：

| 做法 | 可靠？ | ack | 崩潰回收 | consumer group | 複雜度 |
| --- | --- | --- | --- | --- | --- |
| `LPUSH`+`BRPOP` | ❌ 掉訊息 | 無 | 無 | 無 | 最簡單 |
| `LPUSH`+`BLMOVE` | ✅ | 手工 `LREM` | 手工（processing→queue） | 無 | 中，要自己刻 |
| **Stream** | ✅ | `XACK` | `XCLAIM` 內建 | 內建 | 用起來最省事 |

- **`BRPOP` 無 ack**：一 pop，msg 立刻離開 queue；consumer 處理到一半 crash → msg 已消失又沒處理完 → **永久遺失**。
- **`BLMOVE` 可回收**：`BLMOVE queue processing RIGHT LEFT 0` **原子地搬到「處理中」清單**（不是刪掉）；處理完才 `LREM`（= ack）。crash 時 msg 還在 processing → 回收程式搬回 queue 重試 → 不掉。用「處理中清單」手工模擬 ack（`BLMOVE` 舊名 `BRPOPLPUSH`）。
- **「或直接用 Stream」**：BLMOVE 雖可靠但要自己管處理中清單/回收/重試/去重，且沒 consumer group。Stream 把 ack（`XACK`）、待確認清單（PEL）、回收（`XCLAIM`）、多消費者負載均衡（consumer group）**全原生內建** → 要可靠又省事，別用 List 硬刻，直接用 Stream（見第 6 章）。

**實際情境**：容忍掉訊息的輕量任務用 `BRPOP`（如「盡力而為」的通知）；要可靠但不想引入 Stream 用 `BLMOVE`；要可靠 + 多消費組 + 重放（訂單、金流事件）用 Stream。

---

## 4. Set 集合

無序、不重複的元素集合，支援交集/聯集/差集，適合去重與集合運算。

### 指令清單

| 指令 | 說明 |
|---|---|
| `SADD` / `SREM` / `SCARD` | 加 / 刪 / 基數（元素數） |
| `SISMEMBER` / `SMISMEMBER` | 是否為成員 / 批次判斷 |
| `SMEMBERS` | 取全部成員（謹慎） |
| `SSCAN key cursor [MATCH] [COUNT]` | 游標遍歷（大 set 必用） |
| `SRANDMEMBER key [count]` | 隨機成員（不刪） |
| `SPOP key [count]` | 隨機彈出（刪） |
| `SINTER` / `SINTERCARD` / `SINTERSTORE` | 交集 / 只算交集數 / 交集存新 key |
| `SUNION` / `SUNIONSTORE` | 聯集 |
| `SDIFF` / `SDIFFSTORE` | 差集 |
| `SMOVE src dst member` | 移動成員 |

### 底層編碼（含轉碼門檻）

| 編碼 | 條件 | config |
|---|---|---|
| `intset` | 全部成員都是整數 **且** 數量 ≤ 門檻 | `set-max-intset-entries`（預設 512） |
| `listpack` | 成員數 ≤ 門檻且值長度 ≤ 門檻（含非整數小集合，7.2+） | `set-max-listpack-entries`（128）、`set-max-listpack-value`（64） |
| `hashtable` | 超過任一門檻 | — |

- intset 是排序的整數陣列，二分查找 O(log N)，非常省記憶體。
- 加入一個非整數成員會讓 intset 立刻升級（7.2+ 先到 listpack，再到 hashtable）。
- 轉碼同樣**不可逆**。

### 時間複雜度

| 操作 | 複雜度 |
|---|---|
| SADD/SREM/SISMEMBER/SCARD | O(1)（hashtable）；intset 為 O(log N) 查找、加入 O(N) |
| SMEMBERS | O(N) |
| SINTER | O(N*M)，最壞；實作會從最小集合開始 |
| SUNION/SDIFF | O(N) 總元素數 |
| SPOP/SRANDMEMBER count | O(count) |

### 使用情境

- **標籤 / 去重**：文章標籤、去重的訪客 id 集合（小規模）。
- **共同好友 / 共同興趣**：`SINTER user:A:friends user:B:friends`。
- **抽獎**：`SPOP` 隨機且不重複地抽出中獎者。
- **權限 / 白名單黑名單**：`SISMEMBER` O(1) 判斷。

### 踩坑清單

1. **SMEMBERS 對大 set 阻塞**：百萬成員一次拉回會阻塞單執行緒 + 吃頻寬，改 `SSCAN` 分批或 `SRANDMEMBER count` 抽樣。
2. **SINTER / SUNION 對大集合是重運算**：跨大集合做交集會長時間佔用單執行緒；可用 `SINTERCARD` 只要數量、或 `SINTERSTORE` 落地後分批取，或在從節點跑。
3. **big key**：巨大 Set `DEL` 阻塞，用 `SSCAN`+`SREM` 或 `UNLINK`。
4. **intset 升級成本**：加一個非整數成員瞬間整個結構重建為 hashtable，記憶體與 CPU 都跳變。
5. **Set 無序**：不要假設 `SMEMBERS` / `SPOP` 的回傳順序，需要排序請用 ZSet。
6. **STORE 類指令會覆寫目標 key**：`SINTERSTORE dst ...` 會直接覆蓋 dst 既有內容。

### redis-cli 範例

```bash
127.0.0.1:6379> SADD user:A:friends 1 2 3 4
(integer) 4
127.0.0.1:6379> SADD user:B:friends 3 4 5 6
(integer) 4
127.0.0.1:6379> OBJECT ENCODING user:A:friends
"intset"

# 共同好友
127.0.0.1:6379> SINTER user:A:friends user:B:friends
1) "3"
2) "4"

# 只要共同好友數量（省頻寬）
127.0.0.1:6379> SINTERCARD 2 user:A:friends user:B:friends
(integer) 2

# 大 set 安全遍歷
127.0.0.1:6379> SSCAN user:A:friends 0 COUNT 100
```

### Go 範例

可跑版見 `labs/01-string-counter` demo 9/10/11。對應三個實際情境：

```go
ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

// 情境 1：標籤 / 去重 / 成員檢查（文章標籤、用戶興趣、黑白名單）
rdb.SAdd(ctx, "article:42:tags", "go", "redis", "go") // 自動去重 → {go, redis}
yes, _ := rdb.SIsMember(ctx, "article:42:tags", "go").Result() // O(1) 查在不在
_ = yes                                                        // 權限/黑名單檢查都靠這
rdb.SCard(ctx, "article:42:tags")                              // 數量
rdb.SRem(ctx, "article:42:tags", "go")                        // 移除

// 情境 2：共同好友（交集）/ 好友推薦（差集）
rdb.SAdd(ctx, "friends:A", "u1", "u2", "u3", "u4")
rdb.SAdd(ctx, "friends:B", "u3", "u4", "u5", "u6")
common, _ := rdb.SInter(ctx, "friends:A", "friends:B").Result()    // 共同好友 {u3,u4}
recommend, _ := rdb.SDiff(ctx, "friends:B", "friends:A").Result()  // 推薦給 A {u5,u6}
n, _ := rdb.SInterCard(ctx, 0, "friends:A", "friends:B").Result()  // 只要交集數量（省傳輸）
_, _, _ = common, recommend, n

// 情境 3：抽獎（SPOP 中獎即移除，不重複中獎 / SRANDMEMBER 抽樣不移除）
rdb.SAdd(ctx, "lottery:pool", "user1", "user2" /* ... */)
rdb.SRandMemberN(ctx, "lottery:pool", 3) // 抽樣預覽，不動池子
rdb.SPopN(ctx, "lottery:pool", 2)        // 抽 2 位中獎並移除 → 同一人不會中兩次

// 大 set 一律用 SSCAN 游標分批（別 SMEMBERS 全撈，O(N) 卡全庫）
var cursor uint64
for {
	members, next, err := rdb.SScan(ctx, "big:set", cursor, "*", 200).Result()
	if err != nil {
		panic(err)
	}
	_ = members
	cursor = next
	if cursor == 0 { // cursor 回 0 才結束
		break
	}
}
```

**實際情境**：去重（UV 精確去重，量大改 HyperLogLog）、標籤 / 興趣集合、黑白名單（`SISMEMBER` O(1)）、共同好友 / 推薦（`SINTER` / `SDIFF`）、抽獎（`SPOP` 不重複中獎）、隨機抽樣（`SRANDMEMBER`）。**大 set 遍歷用 `SSCAN`；`SINTER`/`SDIFF`/`SUNION` 對大集合是 O(N)，熱點要小心或改離線算。**

#### 集合運算的成本與「大基數該不該用 Redis」

`SINTER` 兩個 set ≈ **O(min(|A|,|B|))**（Redis 先遍歷較小的 set，逐一 `SISMEMBER` 查另一個 O(1)）；M 個 set 最壞 **O(N×M)**。因為是單線程，**基數一大就長時間卡住全庫**。關鍵一樣看「基數有沒有上界」：

| 情境 | 基數 | Live `SINTER`？ |
| --- | --- | --- |
| 好友（Facebook 上限 ~5000） | **有界，幾千** | ✅ O(幾千) ≈ 微秒~低 ms，沒問題 |
| 追蹤者 / 粉絲（大 V 幾百萬，無上界） | **無界，百萬** | ❌ O(百萬) 卡爆，**別在熱路徑做** |

大基數（無界）的功能，**生產通常不會用 Redis live 算**。處理方式：

1. **`SINTERCARD ... LIMIT k`**：只要「有沒有共同」或「最多 k 個」→ 掃到 k 就停，O(k) 不掃完。
2. **背景 / 離線預算**：`SINTERSTORE` 在排程或非同步 job 算好存快取，熱請求只讀結果，不在使用者請求當下算。
3. **只取前 N + 快取**：社群多半只顯示「3 個共同好友」，不需要全量。
4. **超大社交圖直接不用 Redis**：改用圖資料庫（Neo4j）、離線批次（Spark/Hadoop）預計算、或近似演算法——FB/LinkedIn 這種不會對百萬粉絲做 live 交集。
5. **丟 replica 分流**：交集是讀操作，可在 replica 跑，至少不卡 master 的寫。

原則同前：**O(N) 危不危險看 N 會不會無限成長——有界（好友）就用，無界（粉絲）就別在熱路徑做全量交集。**

### 專案流程說明：實作「共同好友推薦」

1. 每個使用者的好友存為 `user:<id>:friends`（整數 uid → intset 省記憶體）。
2. 要推薦時，對目標與候選人 `SINTERCARD 2 A B` 得共同好友數作為推薦分數（不需拉回實際名單，省頻寬）。
3. 若要顯示「你們的 3 個共同好友」，再 `SINTER` 取交集並 `LIMIT`（`SINTERCARD` 亦支援 LIMIT 提前止損）。
4. 好友數破萬的大 V 用 `SSCAN` 分批處理，避免 `SMEMBERS` 阻塞。
5. 熱門的兩兩交集結果可 `SINTERSTORE` 落地並設短 TTL 當快取。

---

## 5. Sorted Set / ZSet 有序集合（★ 本章最重要）

每個成員關聯一個 double 分數（score），成員唯一但分數可重複，**按 score 排序**（score 相同則按成員字典序）。這是 Redis 最強大的結構，排行榜、延遲隊列、限流、時間序索引全靠它。

### 指令清單

| 指令 | 說明 |
|---|---|
| `ZADD key [NX\|XX] [GT\|LT] [CH] [INCR] score member ...` | 加成員/更新分數 |
| `ZSCORE` / `ZMSCORE` | 取分數 / 批次 |
| `ZINCRBY key incr member` | 分數增減 |
| `ZRANK` / `ZREVRANK key member [WITHSCORE]` | 正/逆序排名（0-based） |
| `ZRANGE key start stop [BYSCORE\|BYLEX] [REV] [LIMIT] [WITHSCORES]` | 統一範圍查詢（6.2+） |
| `ZREVRANGE` / `ZRANGEBYSCORE` / `ZRANGEBYLEX` | 舊版範圍查詢（仍可用） |
| `ZCARD` / `ZCOUNT key min max` | 總數 / 分數區間內數量 |
| `ZREM` / `ZREMRANGEBYRANK` / `ZREMRANGEBYSCORE` / `ZREMRANGEBYLEX` | 刪成員 / 按各種範圍刪 |
| `ZPOPMIN` / `ZPOPMAX` / `BZPOPMIN` / `BZPOPMAX` | 彈出最小/最大（阻塞版） |
| `ZRANGESTORE` | 範圍查詢結果存到新 key |
| `ZUNION(STORE)` / `ZINTER(STORE)` / `ZDIFF(STORE)` | 集合運算（可帶權重 WEIGHTS、聚合 AGGREGATE） |
| `ZLEXCOUNT` / `ZRANDMEMBER` | 字典區間計數 / 隨機成員 |

### 底層編碼（含轉碼門檻）

| 編碼 | 條件 | config |
|---|---|---|
| `listpack` | 成員數 ≤ 門檻 **且** 每個成員長度 ≤ 門檻 | `zset-max-listpack-entries`（預設 128）、`zset-max-listpack-value`（預設 64） |
| `skiplist` | 超過任一門檻 | — |

- **skiplist 編碼實際是「跳表 + hashtable」雙結構**：跳表維護排序（支援範圍/排名查詢 O(log N)），hashtable 做 member→score 的 O(1) 反查（`ZSCORE`）。兩者同步維護，這是 ZSet 又能排序又能 O(1) 查分的關鍵。
- 舊 config 名 `zset-max-ziplist-entries` / `zset-max-ziplist-value` 相容。
- 轉碼不可逆。

### 時間複雜度

| 操作 | 複雜度 |
|---|---|
| ZADD/ZREM/ZSCORE/ZINCRBY | O(log N)（skiplist）；ZSCORE 反查 O(1) |
| ZRANK/ZREVRANK | O(log N) |
| ZRANGE / ZRANGEBYSCORE | O(log N + M)，M 為回傳數 |
| ZCOUNT | O(log N) |
| ZREMRANGEBYSCORE/BYRANK | O(log N + M)，M 為刪除數 |
| ZPOPMIN/MAX | O(log N) |
| ZUNIONSTORE/ZINTERSTORE | O(N*K + M*log M) |

### 使用情境（逐一展開）

#### 5.1 排行榜（Leaderboard）

最經典用法：score 為分數/積分，成員為玩家 id。

```bash
127.0.0.1:6379> ZADD game:rank 1500 "alice" 1800 "bob" 1200 "carol"
(integer) 3
127.0.0.1:6379> ZINCRBY game:rank 300 "carol"     # 加分
"1500"
# Top 3（由高到低）
127.0.0.1:6379> ZRANGE game:rank 0 2 REV WITHSCORES
1) "bob"
2) "1800"
3) "alice"
4) "1500"
5) "carol"
6) "1500"
# 查某人排名（逆序，0-based，要 +1 才是名次）
127.0.0.1:6379> ZREVRANK game:rank "alice"
(integer) 1
```

**同分排序坑**：score 相同時按成員字典序，非「先到先得」。若要「同分先達者在前」，常見技巧是把 score 設計成 `score * 大係數 - 時間戳`，讓時間成為次要排序鍵，仍是單一 double。注意 double 只有 53 bit 整數精度，組合分數要避免超過 2^53。

#### 5.2 延遲隊列 / 定時任務（Delayed Queue）

score = 任務到期的 Unix 時間戳，成員 = 任務 payload/id。

```bash
# 投遞：60 秒後執行
127.0.0.1:6379> ZADD delay:queue 1720000060 "task:send-email:42"
(integer) 1
# 排程輪詢：取所有已到期任務
127.0.0.1:6379> ZRANGEBYSCORE delay:queue -inf 1720000060 LIMIT 0 100
1) "task:send-email:42"
# 取出後刪除（多消費者要用 Lua 保證原子「取+刪」，避免重複消費）
127.0.0.1:6379> ZREM delay:queue "task:send-email:42"
```

**原子取出 Lua**（避免多 worker 搶到同一任務）：

```lua
-- KEYS[1] = zset, ARGV[1] = now
local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 10)
if #jobs > 0 then
  redis.call('ZREM', KEYS[1], unpack(jobs))
end
return jobs
```

#### 5.3 滑動窗限流（Sliding Window Rate Limit）

score = 請求時間戳（毫秒），成員 = 唯一請求 id。每次請求：先清窗、再算窗內數量、再加入。

```bash
# 窗 = 60000ms，限 100 次。now = 1720000000000
127.0.0.1:6379> ZREMRANGEBYSCORE ratelimit:u1 0 1719999940000   # 清掉 60s 前
127.0.0.1:6379> ZCARD ratelimit:u1                               # 窗內數量
127.0.0.1:6379> ZADD ratelimit:u1 1720000000000 "req-uuid"       # 記錄本次
127.0.0.1:6379> EXPIRE ratelimit:u1 60                           # 兜底過期
```

完整邏輯應包在 Lua 內原子執行（見下方 Go 範例的 script）。

#### 5.4 時間序索引 / 範圍查詢（Time-series Index）

score = 時間戳，成員 = 事件 id，用 `ZRANGEBYSCORE` 做「某時間區間的事件」。也可用 `BYLEX` 對等分數的成員做字典範圍查詢，實作二級索引。

```bash
127.0.0.1:6379> ZADD events 1720000001 "e1" 1720000005 "e2" 1720000010 "e3"
(integer) 3
# 查 [1720000002, 1720000008] 區間
127.0.0.1:6379> ZRANGEBYSCORE events 1720000002 1720000008
1) "e2"
```

### 踩坑清單

1. **成員數 vs 分數精度**：score 是 IEEE 754 double，安全整數上限 2^53。用時間戳（毫秒）沒問題，但「score 組合編碼」時要小心溢位失去精度。
2. **ZADD 預設會更新既有成員分數**：不想覆蓋分數要用 `NX`；只想在更大時更新用 `GT`（排行榜「只升不降」很有用）。
3. **ZRANGEBYSCORE + LIMIT 分頁陷阱**：大 offset 的 `LIMIT offset count` 仍要掃過前面元素，深分頁慢；改用「上次最後 score 當游標」的 seek 分頁。
4. **big key**：千萬級成員的 ZSet，範圍刪除/全量掃描會阻塞。定期 `ZREMRANGEBYRANK key 0 -(N+1)` 修剪只留 Top N。
5. **限流窗 key 要設 TTL**：滑動窗 ZSet 若使用者不再請求，`ZREMRANGEBYSCORE` 不會被觸發，key 會殘留；務必配 `EXPIRE` 兜底。
6. **-inf / +inf 與開區間**：`ZRANGEBYSCORE` 用 `(` 前綴表開區間（`(5` = 大於 5）；min/max 寫反會回空。
7. **WITHSCORES 回傳是 member/score 交錯**，解析別錯位；go-redis 用 `ZRangeWithScores` 回 `[]redis.Z` 較安全。
8. **ZINCRBY 不存在成員會當 0 起算**，符合排行榜語意但要意識到。

### redis-cli 範例（綜合）

```bash
# 排行榜「只升不降」加分
127.0.0.1:6379> ZADD game:rank GT CH 2000 "alice"
(integer) 1
# 查名次區間（第 10~20 名）
127.0.0.1:6379> ZRANGE game:rank 9 19 REV WITHSCORES
# 只留 Top 100（修剪 big key）
127.0.0.1:6379> ZREMRANGEBYRANK game:rank 0 -101
```

### Go 範例

```go
ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

// 排行榜：加分（只升不降）
rdb.ZAddArgs(ctx, "game:rank", redis.ZAddArgs{
	GT:      true,
	Ch:      true,
	Members: []redis.Z{{Score: 2000, Member: "alice"}},
})

// Top 3
top, _ := rdb.ZRevRangeWithScores(ctx, "game:rank", 0, 2).Result()
for i, z := range top {
	fmt.Printf("#%d %v = %.0f\n", i+1, z.Member, z.Score)
}

// 查某玩家名次（+1 = 人類可讀名次）
rank, _ := rdb.ZRevRank(ctx, "game:rank", "alice").Result()
fmt.Println("alice rank:", rank+1)

// 滑動窗限流（原子 Lua）
var slidingWindow = redis.NewScript(`
local key    = KEYS[1]
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[3])
local member = ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count >= limit then
  return 0
end
redis.call('ZADD', key, now, member)
redis.call('PEXPIRE', key, window)
return 1
`)

now := time.Now().UnixMilli()
allowed, _ := slidingWindow.Run(ctx, rdb,
	[]string{"ratelimit:u1"},
	now, int64(60000), 100, fmt.Sprintf("%d-%d", now, rand.Int()),
).Int()
fmt.Println("allowed:", allowed == 1)

// 延遲隊列：取到期任務
due, _ := rdb.ZRangeByScore(ctx, "delay:queue", &redis.ZRangeBy{
	Min: "-inf", Max: fmt.Sprint(now / 1000), Offset: 0, Count: 100,
}).Result()
_ = due
```

### 專案流程說明：實作「即時排行榜 + 週期歸檔」

1. 玩家得分事件到達 → `ZADD game:rank:live GT CH <score> <player>`（只記錄更高分）。
2. 前端排行榜頁 → `ZREVRANGE game:rank:live <offset> <offset+size-1> WITHSCORES` 分頁；個人名次 `ZREVRANK`。
3. 深分頁不用大 offset，改用「上一頁最後一名的 score 當游標」做 `ZREVRANGEBYSCORE (lastScore -inf LIMIT 0 size`。
4. 控制記憶體：排程每晚 `ZREMRANGEBYRANK game:rank:live 0 -10001` 只保留 Top 10000。
5. 賽季結算：把 `game:rank:live` `RENAME` 為 `game:rank:season:<n>` 冷凍歸檔，開新的 live key。
6. 若排行榜需要「同分先達者靠前」，score 用 `points*1e7 + (1e7 - normalizedTimestamp)` 之類組合，並確保不超過 2^53。

---

## 6. Stream 串流

Redis 5.0 引入的 append-only 日誌型結構，每筆訊息有唯一遞增 ID 與多個 field-value。支援 consumer group、ack、重放，是 Redis 內建的可靠訊息佇列。

### 指令清單

| 指令 | 說明 |
|---|---|
| `XADD key [NOMKSTREAM] [MAXLEN\|MINID [~] N] * f v ...` | 追加訊息（`*` = 自動 ID） |
| `XLEN` / `XRANGE` / `XREVRANGE` | 長度 / 範圍讀（`-` `+` 為極值） |
| `XREAD [COUNT] [BLOCK ms] STREAMS key... id...` | 讀（非消費組，`$` = 只讀新訊息） |
| `XGROUP CREATE key group id\|$ [MKSTREAM]` | 建消費組 |
| `XREADGROUP GROUP g c [COUNT] [BLOCK] STREAMS key >` | 消費組讀（`>` = 未投遞的新訊息） |
| `XACK key group id ...` | 確認處理完成 |
| `XPENDING key group [IDLE] [start end count] [consumer]` | 查未 ack（PEL） |
| `XCLAIM` / `XAUTOCLAIM` | 轉移逾時未 ack 的訊息給其他 consumer |
| `XDEL` / `XTRIM key MAXLEN\|MINID [~] N` | 刪訊息 / 修剪 |
| `XINFO STREAM\|GROUPS\|CONSUMERS` | 觀測資訊 |
| `XSETID` | 設定 last-id |

### 底層編碼

Stream 用 **Radix tree（rax）** 索引訊息 ID，訊息以 listpack 打包成塊（macro node）儲存，consumer group 的狀態（last-delivered-id、PEL）另存。`OBJECT ENCODING` 顯示 `stream`。沒有像其他結構那種 listpack↔某某 的轉碼門檻。

### 時間複雜度

| 操作 | 複雜度 |
|---|---|
| XADD | O(1)（有 MAXLEN ~ 時平攤 O(1)） |
| XLEN | O(1) |
| XRANGE / XREAD | O(log N + M)，M 為回傳數 |
| XACK / XDEL | O(1) per id |
| XTRIM MAXLEN | O(N)，被刪數量；`~` 近似修剪更快 |

### 使用情境

- **可靠訊息佇列**：需要 at-least-once、多消費者負載均衡、崩潰後未 ack 訊息可回收。
- **事件溯源 / 日誌**：append-only，可用 `XRANGE` 重放歷史。
- **多消費組扇出**：同一 stream 建多個 group，各組獨立消費全量（廣播 + 組內均衡）。

### 踩坑清單

1. **Pending（PEL）堆積**：`XREADGROUP` 讀出訊息後若 consumer 不 `XACK`（崩潰、bug、忘了 ack），訊息永遠留在 PEL。必須有機制用 `XPENDING` + `XAUTOCLAIM` 回收，否則堆積成災、記憶體漲、重複處理判斷失準。
2. **XADD 不修剪會無限成長**：Stream 是 append-only，不 trim 會吃爆記憶體。用 `XADD key MAXLEN ~ 100000 * ...`（`~` 近似修剪，效能好）或定期 `XTRIM`。
3. **`>` vs 具體 ID 的語意**：`XREADGROUP ... STREAMS key >` 讀「從未投遞的新訊息」；給具體 ID（如 `0`）則讀「該 consumer 自己 PEL 中未 ack 的舊訊息」——崩潰重啟後要先用 `0` 把自己的 PEL 處理完再切 `>`。
4. **XACK 不刪訊息**：ack 只是把訊息移出 PEL，訊息仍在 stream 佔記憶體，清理靠 XTRIM/XDEL。
5. **消費組不存在報錯**：`XREADGROUP` 前必須先 `XGROUP CREATE`，且對空 stream 要加 `MKSTREAM`。
6. **XAUTOCLAIM 的 min-idle-time**：設太短會把處理中（只是慢）的訊息搶走造成重複，設太長則故障恢復慢，要依處理時長調。
7. **at-least-once 不是 exactly-once**：claim + 重試可能重複投遞，消費端業務要做冪等。

### redis-cli 範例

```bash
# 建 stream 與消費組
127.0.0.1:6379> XADD orders MAXLEN ~ 100000 * order_id 1001 amount 250
"1720000000000-0"
127.0.0.1:6379> XGROUP CREATE orders billing 0 MKSTREAM
OK

# 消費者讀新訊息
127.0.0.1:6379> XREADGROUP GROUP billing worker-1 COUNT 10 BLOCK 5000 STREAMS orders >
1) 1) "orders"
   2) 1) 1) "1720000000000-0"
         2) 1) "order_id"
            2) "1001"
            3) "amount"
            4) "250"

# 處理完 ack
127.0.0.1:6379> XACK orders billing 1720000000000-0
(integer) 1

# 查堆積的未 ack
127.0.0.1:6379> XPENDING orders billing
# 回收閒置超過 60s 的訊息給 worker-2
127.0.0.1:6379> XAUTOCLAIM orders billing worker-2 60000 0
```

### Go 範例

```go
ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

// 生產：帶近似修剪
rdb.XAdd(ctx, &redis.XAddArgs{
	Stream: "orders",
	MaxLen: 100000,
	Approx: true, // MAXLEN ~
	Values: map[string]any{"order_id": 1001, "amount": 250},
})

// 建消費組（忽略已存在錯誤）
rdb.XGroupCreateMkStream(ctx, "orders", "billing", "0")

// 消費新訊息
streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
	Group:    "billing",
	Consumer: "worker-1",
	Streams:  []string{"orders", ">"},
	Count:    10,
	Block:    5 * time.Second,
}).Result()
if err != nil && err != redis.Nil {
	panic(err)
}
for _, s := range streams {
	for _, msg := range s.Messages {
		// 冪等處理業務
		process(msg.Values)
		rdb.XAck(ctx, "orders", "billing", msg.ID) // ack
	}
}

// 回收逾時未 ack（每個 worker 週期跑）
claimed, _, _ := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
	Stream:   "orders",
	Group:    "billing",
	Consumer: "worker-1",
	MinIdle:  60 * time.Second,
	Start:    "0",
	Count:    100,
}).Result()
_ = claimed
```

`func process(v map[string]any) {}` 為示意，實作務必冪等。

### 專案流程說明：實作「訂單事件可靠處理管線」

1. 下單服務 `XADD orders MAXLEN ~ 1e5 * ...` 寫入訂單事件，近似修剪控制長度。
2. 計費服務建立 group `billing`，多個 worker 用同一 group + 不同 consumer 名，Redis 自動把訊息分派給不同 worker（組內負載均衡）。
3. 每個 worker 啟動時先用 `XREADGROUP ... STREAMS orders 0` 處理自己 PEL 裡的殘留訊息，再切成 `>` 讀新訊息。
4. 處理成功才 `XACK`；業務做成冪等（用 order_id 去重）以容忍重複投遞。
5. 監控守護程序定期 `XPENDING orders billing` 看堆積量並告警；對 idle 過久的訊息 `XAUTOCLAIM` 轉給健康的 worker。
6. 需要「通知服務」也消費同一批事件時，另建 group `notify`，兩組互不影響（扇出）。
7. 記憶體治理：ack 不釋放空間，靠 XADD 的 MAXLEN ~ 持續修剪 + 監控 `XLEN`、`XINFO STREAM`。

### List vs Stream：訊息佇列該選哪個

呼應第 0 章對照表，把差異講清楚：

| | List（`LPUSH`/`BRPOP`） | Stream（`XADD`/`XREADGROUP`/`XACK`） |
|---|---|---|
| 讀取後 | **立即從 list 移除** | 保留，進 PEL 等 ack |
| ack | 無 | 手動 `XACK` |
| consumer 崩潰 | **訊息永久遺失** | 留在 PEL，`XCLAIM`/`XAUTOCLAIM` 重新認領 |
| 重放 | 不行（pop 走就沒了） | 可以（`XRANGE`／從任意 ID 重讀） |
| consumer group | 無 | 有（組內負載均衡；多組各收全部＝fan-out） |
| 監控 | 幾乎不用 | 要看 `XPENDING` 防堆積 |
| 成本 | 極省 | 較貴（ID／PEL／group 狀態） |

一句話：**容忍掉訊息、要極省 → List；要可靠交付 + 重放 + 多消費組 → Stream。**

### Redis Stream vs Kafka

Stream 像「內建在 Redis 的輕量 Kafka」，但規模、持久化、生態差很多：

| 面向 | Redis Stream | Kafka |
|---|---|---|
| 儲存 | 記憶體（+AOF/RDB），**受 RAM 限制**，要 `MAXLEN` 修剪 | **磁碟 log**，保留天/週，TB 級 |
| 持久保證 | RAM 掉了靠 AOF，較弱 | 多 broker 複製（replication factor），強 |
| 吞吐 | 單線程單機，約十萬/s 級 | 水平分區跨 broker，**百萬/s** |
| 擴展 | 單節點（cluster 也是單 stream 落單一 slot） | partition 天生水平擴 |
| 重放 | 有限（RAM，被 trim 就沒） | 極強（offset seek、長期保留） |
| 順序 | 單 stream 全序 | **per-partition** 有序 |
| offset 模型 | PEL 逐則 ack | consumer 存 offset，可任意 rewind |
| delivery | at-least-once | at-least-once，可 exactly-once（transaction） |
| 生態 | 無 | Connect / Streams / Schema Registry / ksqlDB |
| 延遲 | 極低（記憶體） | 低（但比 Redis 高） |
| 維運 | 已有 Redis＝零額外 | 要獨立 cluster（Zookeeper/KRaft） |

**選型**：

- **Redis Stream**：已經有 Redis、量中等、要低延遲、不想多養一套。輕量任務隊列、即時通知。
- **Kafka**：高吞吐、要長期保留 + 重放、大數據管線、多消費組、跨團隊 event backbone。

> 實務對照：像跨服務（mail / ticket / sla）的事件骨幹、要長期保留與多 consumer group 的場景，就選 **Kafka**；Stream 適合「已有 Redis 又只需要輕量可靠佇列」的情況，不要拿 Stream 硬扛 Kafka 等級的持久化與吞吐。

---

## 7. Bitmap 位圖

不是獨立型別，而是把 String 當成 bit 陣列操作。用極少記憶體表達海量布林狀態。

### 指令清單

| 指令 | 說明 |
|---|---|
| `SETBIT key offset 0\|1` | 設某位 |
| `GETBIT key offset` | 取某位 |
| `BITCOUNT key [start end [BYTE\|BIT]]` | 數 1 的個數（可指定範圍） |
| `BITPOS key bit [start [end [BYTE\|BIT]]]` | 找第一個 0/1 的位置 |
| `BITOP AND\|OR\|XOR\|NOT dest key ...` | 位運算到新 key |
| `BITFIELD`（見第 10 章） | 打包多欄位整數操作 |

### 底層編碼

底層就是 String（`raw`/`embstr`），`OBJECT ENCODING` 看到的是字串編碼。位是按 offset 定址，offset/8 決定第幾個 byte。

### 時間複雜度

| 操作 | 複雜度 |
|---|---|
| SETBIT/GETBIT | O(1)（但首次設大 offset 需配置整段記憶體） |
| BITCOUNT | O(N)，N 為 byte 數 |
| BITPOS | O(N) |
| BITOP | O(N)，最長 key 長度 |

### 使用情境

- **簽到 / 日活布林標記**：一個 user 一個 bit，`SETBIT active:20260707 <uid> 1`；`BITCOUNT` 得當日活躍數。
- **每月簽到卡**：`sign:<uid>:202607`，第 N 天簽到就 `SETBIT key N-1 1`，`BITCOUNT` 得當月簽到天數。
- **多日交集**：`BITOP AND` 算「連續 N 天都活躍」的用戶。
- **布林特徵向量**：feature flag per user。

### 記憶體估算

bit 陣列大小 = 最大 offset / 8 bytes。
- 1 千萬 uid 的日活 bitmap：10,000,000 / 8 ≈ **1.25 MB**，極省。
- 但**記憶體由最大 offset 決定，不是實際設了幾個 1**：只設 offset=10,000,000 一個 bit，也會配置 ~1.25MB。

### 踩坑清單

1. **大 offset 撐爆記憶體**：`SETBIT key 4000000000 1`（40 億）會瞬間配置 500MB。offset 上限為 2^32-1（約 42 億，對應 512MB）。**絕不能拿稀疏的大數值（如雪花 ID、時間戳）直接當 offset**，要先映射成從 0 起的密集整數（如自增 uid）。
2. **稀疏資料反而浪費**：只有少數用戶且 uid 很大時，bitmap 大量是 0 卻仍佔滿記憶體，這種情況用 Set 反而省。bitmap 適合「密集 + 高基數」。
3. **BITCOUNT 全量掃描**：對超大 bitmap 頻繁 `BITCOUNT` 有 CPU 成本，考慮快取結果或分段。
4. **BITOP 對齊到最長 key**：不同長度的 bitmap 做 BITOP，短的視為補 0，結果 key 長度 = 最長者。
5. **offset 從 0 起算**：第 1 個 bit 是 offset 0，簽到「第 1 天」對應 offset 0，別差一。

### redis-cli 範例

```bash
# 日活：uid=100、105 今天活躍
127.0.0.1:6379> SETBIT active:20260707 100 1
(integer) 0
127.0.0.1:6379> SETBIT active:20260707 105 1
(integer) 0
127.0.0.1:6379> BITCOUNT active:20260707
(integer) 2

# 連續兩天都活躍的用戶數
127.0.0.1:6379> SETBIT active:20260708 100 1
(integer) 0
127.0.0.1:6379> BITOP AND active:both active:20260707 active:20260708
(integer) 14
127.0.0.1:6379> BITCOUNT active:both
(integer) 1

# 當月簽到：第 7 天簽到
127.0.0.1:6379> SETBIT sign:u100:202607 6 1
(integer) 0
127.0.0.1:6379> BITCOUNT sign:u100:202607
(integer) 1
```

### Go 範例

```go
ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

// 標記日活
rdb.SetBit(ctx, "active:20260707", 100, 1)
rdb.SetBit(ctx, "active:20260707", 105, 1)

// 當日活躍數
dau, _ := rdb.BitCount(ctx, "active:20260707", nil).Result()
fmt.Println("DAU:", dau)

// 連續兩天活躍
rdb.SetBit(ctx, "active:20260708", 100, 1)
rdb.BitOpAnd(ctx, "active:both", "active:20260707", "active:20260708")
both, _ := rdb.BitCount(ctx, "active:both", nil).Result()
fmt.Println("both days:", both)

// 某用戶今天有無活躍
b, _ := rdb.GetBit(ctx, "active:20260707", 100).Result()
fmt.Println("u100 active:", b == 1)
```

### 專案流程說明：實作「用戶簽到日曆 + 連續簽到天數」

1. key 設計 `sign:<uid>:<yyyymm>`，一個月一個 bitmap，最多 31 bit（不到 4 byte）。
2. 用戶今天（本月第 D 天）簽到 → `SETBIT sign:<uid>:202607 (D-1) 1`。
3. 顯示本月已簽到天數 → `BITCOUNT sign:<uid>:202607`。
4. 判斷今天是否已簽 → `GETBIT`。
5. 算「連續簽到」→ 取整個 bitmap（`GET`）到應用層做 bit 掃描，或用 `BITPOS` 找最近的 0。
6. uid 必須是密集自增整數（不能用手機號、UUID），否則 offset 過大撐爆；若 uid 稀疏，改用 `sign:<uid>` 的 Hash 或 Set 存日期。

---

## 8. HyperLogLog（HLL）

機率性基數（cardinality，去重計數）估算結構，用固定 ~12KB 記憶體估算任意規模的不重複元素數，誤差率標準約 0.81%。

### 指令清單

| 指令 | 說明 |
|---|---|
| `PFADD key element ...` | 加入元素（自動去重估算） |
| `PFCOUNT key [key ...]` | 估算基數（多 key 為聯集基數） |
| `PFMERGE dest src ...` | 合併多個 HLL 到目標 |
| `PFDEBUG` / `PFSELFTEST` | 內部調試（少用） |

### 底層編碼

HLL 也是存在 String 裡（`OBJECT ENCODING` 為 string）。有兩種內部表示：
- **sparse**：元素少時用稀疏表示更省（可能只幾百 byte）。
- **dense**：元素多時轉為稠密的 16384 個 register，固定 12KB。
- 由 `hll-sparse-max-bytes`（預設 3000）控制何時從 sparse 轉 dense。

### 時間複雜度

| 操作 | 複雜度 |
|---|---|
| PFADD | O(1) |
| PFCOUNT 單 key | O(1) |
| PFCOUNT 多 key / PFMERGE | O(N)，N 為 key 數 |

### 使用情境

- **UV（不重複訪客）估算**：`PFADD uv:20260707 <visitor_id>`，`PFCOUNT` 得當日 UV。
- **海量去重計數**：搜尋詞去重、去重 IP 數、去重設備數——只要「數量」不要「名單」。
- **跨維度合併**：`PFMERGE uv:week uv:day1 ... uv:day7` 得週 UV（自動去重）。

### 為什麼用 HLL（vs Set）

| | Set（精確） | HyperLogLog（估算） |
|---|---|---|
| 1 億去重元素記憶體 | 數 GB | **固定 12KB** |
| 精確度 | 100% | 標準誤差 0.81% |
| 能列舉成員 | 能 | **不能** |
| 能算交集 | 能（SINTER） | **不能** |
| 能算聯集基數 | 能 | 能（PFMERGE/PFCOUNT 多 key） |

### 踩坑清單

1. **不能取回成員**：HLL 只存估算狀態，無法列出到底有哪些元素，也無法判斷「某元素在不在」。需要這些就用 Set。
2. **不能做交集**：只有聯集（PFMERGE / 多 key PFCOUNT）。要算「同時出現在 A 和 B 的去重數」，HLL 做不到，得用 Set 或容斥近似（|A|+|B|-|A∪B|，但誤差會放大，不可靠）。
3. **有誤差，別用於計費/對帳**：0.81% 是標準誤差，個別值可能偏差更多。精確場景（金額、法規）不可用。
4. **小基數時可能也不精確**：雖然 Redis 對小基數有校正，但語意上它就是估算器。
5. **PFCOUNT 有寫入副作用**：首次 `PFCOUNT` 可能把 sparse 快取結果寫回（會產生寫入），在唯讀從節點需注意（新版已改善，但要意識到 PFCOUNT 非純讀）。
6. **12KB 是上限不是下限**：sparse 表示時遠小於 12KB，別以為每個 HLL 都佔 12KB。

### redis-cli 範例

```bash
127.0.0.1:6379> PFADD uv:20260707 "user-1" "user-2" "user-3" "user-1"
(integer) 1
127.0.0.1:6379> PFCOUNT uv:20260707
(integer) 3
127.0.0.1:6379> PFADD uv:20260708 "user-3" "user-4"
(integer) 1
# 兩天合併 UV（去重）
127.0.0.1:6379> PFMERGE uv:2days uv:20260707 uv:20260708
OK
127.0.0.1:6379> PFCOUNT uv:2days
(integer) 4
# 等同直接多 key PFCOUNT
127.0.0.1:6379> PFCOUNT uv:20260707 uv:20260708
(integer) 4
```

### Go 範例

```go
ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

// 記錄訪客（自動去重）
rdb.PFAdd(ctx, "uv:20260707", "user-1", "user-2", "user-3", "user-1")

// 當日 UV 估算
uv, _ := rdb.PFCount(ctx, "uv:20260707").Result()
fmt.Println("UV:", uv) // ≈3

// 合併多天
rdb.PFAdd(ctx, "uv:20260708", "user-3", "user-4")
rdb.PFMerge(ctx, "uv:2days", "uv:20260707", "uv:20260708")
total, _ := rdb.PFCount(ctx, "uv:2days").Result()
fmt.Println("2-day UV:", total) // ≈4
```

### 專案流程說明：實作「網站每日/每週 UV 統計」

1. 每個請求 `PFADD uv:<yyyymmdd> <visitor_id>`，O(1)、記憶體幾乎不隨訪客數成長。
2. 日報 → `PFCOUNT uv:<yyyymmdd>`。
3. 週報 → `PFCOUNT uv:d1 uv:d2 ... uv:d7`（多 key 聯集，自動去重跨日重複訪客），或先 `PFMERGE` 成 `uv:week:<n>` 再 count 以便快取。
4. 每個日 key 設 TTL（如 40 天）自動清理。
5. 若某天需要「精確 UV 對帳」或「要抓出具體訪客名單」，該維度改用 Set（接受更高記憶體成本）。
6. 明確告知業務方：此數字是估算值（±0.81%），不可用於結算。

---

## 9. Geo 地理空間

儲存經緯度座標並支援距離計算與範圍查詢。底層其實就是 ZSet——把經緯度用 GeoHash 編碼成一個 52-bit 整數當 score。

### 指令清單

| 指令 | 說明 |
|---|---|
| `GEOADD key [NX\|XX\|CH] lon lat member ...` | 加入座標 |
| `GEOPOS key member ...` | 取回座標 |
| `GEODIST key m1 m2 [m\|km\|mi\|ft]` | 兩點距離 |
| `GEOSEARCH key FROMMEMBER m\|FROMLONLAT lon lat BYRADIUS r unit\|BYBOX w h unit [ASC\|DESC] [COUNT n] [WITHCOORD WITHDIST WITHHASH]` | 範圍搜尋（6.2+ 主力） |
| `GEOSEARCHSTORE` | 搜尋結果存到新 key |
| `GEOHASH` | 取 GeoHash 字串 |
| `GEORADIUS` / `GEORADIUSBYMEMBER` | 舊版搜尋（已棄用，用 GEOSEARCH） |

### 底層編碼

就是 **Sorted Set**（`OBJECT ENCODING` 顯示 `listpack` 或 `skiplist`）。member = 地點名，score = 52-bit interleaved GeoHash。所以所有 ZSet 指令都能直接作用在 Geo key 上（如 `ZREM` 刪點、`ZCARD` 數點、`ZRANGE` 列點）。

### 時間複雜度

| 操作 | 複雜度 |
|---|---|
| GEOADD | O(log N)（就是 ZADD） |
| GEOPOS/GEODIST | O(log N) / O(1) |
| GEOSEARCH BYRADIUS/BYBOX | O(N + log M)，會掃相關 GeoHash 格 |

### 使用情境

- **附近的人 / 附近的店**：`GEOSEARCH ... FROMLONLAT <me> BYRADIUS 3 km ASC COUNT 20`。
- **司機/騎手調度**：找乘客附近可用司機。
- **地理圍欄 / 配送範圍**：判斷點是否在半徑內。

### 踩坑清單

1. **精度與邊界**：GeoHash 是格點近似，半徑邊界附近可能有極小誤差；但對「附近的人」等場景足夠。
2. **無法刪點的專用指令**：Geo 沒有 `GEODEL`，刪點要用 ZSet 的 `ZREM key member`（因為底層是 ZSet）。
3. **經緯度順序是 lon, lat（先經度後緯度）**：很多人習慣 lat,lng 會寫反，導致點跑到地球另一邊。
4. **有效範圍**：經度 -180~180、緯度約 -85.05112878~85.05112878，超出報錯。
5. **COUNT 不加會回全部命中**：大範圍搜尋務必加 `COUNT` 限制，否則可能回海量結果阻塞。
6. **BYRADIUS ANY 優化**：`GEOSEARCH ... COUNT n ANY` 找到 n 個就返回（不保證最近），要「最近的 n 個」不能加 ANY，要配 `ASC`。
7. **big key**：把全國所有點塞一個 key 會變 big key + 搜尋慢；可按城市/網格分 key。

### redis-cli 範例

```bash
# 加入店家（注意順序：經度 緯度）
127.0.0.1:6379> GEOADD shops 121.5654 25.0330 "store-taipei101"
(integer) 1
127.0.0.1:6379> GEOADD shops 121.5170 25.0478 "store-taipei-main"
(integer) 1

# 兩點距離
127.0.0.1:6379> GEODIST shops store-taipei101 store-taipei-main km
"4.8..."

# 找某座標 3km 內、最近的 10 家（含距離與座標）
127.0.0.1:6379> GEOSEARCH shops FROMLONLAT 121.55 25.04 BYRADIUS 3 km ASC COUNT 10 WITHDIST WITHCOORD

# 刪一個點（用 ZSet 指令）
127.0.0.1:6379> ZREM shops "store-taipei101"
(integer) 1
```

### Go 範例

```go
ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

// 加入座標
rdb.GeoAdd(ctx, "shops",
	&redis.GeoLocation{Name: "store-101", Longitude: 121.5654, Latitude: 25.0330},
	&redis.GeoLocation{Name: "store-main", Longitude: 121.5170, Latitude: 25.0478},
)

// 附近搜尋：座標 3km 內最近 10 家
res, _ := rdb.GeoSearchLocation(ctx, "shops", &redis.GeoSearchLocationQuery{
	GeoSearchQuery: redis.GeoSearchQuery{
		Longitude:  121.55,
		Latitude:   25.04,
		Radius:     3,
		RadiusUnit: "km",
		Sort:       "ASC",
		Count:      10,
	},
	WithCoord: true,
	WithDist:  true,
}).Result()
for _, loc := range res {
	fmt.Printf("%s  %.2fkm  (%.4f,%.4f)\n", loc.Name, loc.Dist, loc.Longitude, loc.Latitude)
}

// 刪點（底層 ZSet）
rdb.ZRem(ctx, "shops", "store-101")
```

### 專案流程說明：實作「附近的騎手匹配」

1. 騎手上線/移動時 `GEOADD drivers:online <lon> <lat> <driver_id>` 持續更新位置。
2. 訂單產生時取餐廳座標，`GEOSEARCH drivers:online FROMLONLAT <lon> <lat> BYRADIUS 3 km ASC COUNT 20 WITHDIST` 找最近 20 名候選。
3. 派單邏輯在候選中依距離 + 評分挑選；被指派的騎手 `ZREM drivers:online <driver_id>` 暫時移出可派池。
4. 騎手下線 `ZREM`；長時間未更新座標的用排程清理（可搭配另一個 ZSet 記錄 last-seen 時間戳做過期）。
5. 全國規模時按城市分 key（`drivers:online:<city>`）避免 big key 與跨城無意義搜尋。

---

## 10. Bitfield 位域

在單一 String 上把「連續 bit 區段」當成任意寬度的整數（有號/無號）來讀寫，可把多個小計數器緊湊打包，一條指令原子操作多個欄位。

### 指令清單

`BITFIELD key [子操作...]`，子操作可鏈式組合：

| 子操作 | 說明 |
|---|---|
| `GET type offset` | 讀取（type 如 `u8`、`i16`；offset 可用 `#n` 表示第 n 個該型別欄位） |
| `SET type offset value` | 寫入，回傳舊值 |
| `INCRBY type offset increment` | 加減 |
| `OVERFLOW WRAP\|SAT\|FAIL` | 設定後續 INCRBY 溢位行為 |

- type：`u1`~`u63`（無號，最大 63 位）、`i1`~`i64`（有號）。
- `#n` 語法：`u8 #3` = 第 3 個 u8 欄位（offset = 3*8 = 24）。

### 底層編碼

底層還是 String（bit 陣列），與 Bitmap 同源。`OBJECT ENCODING` 為 string 編碼。

### 時間複雜度

| 操作 | 複雜度 |
|---|---|
| 每個 GET/SET/INCRBY 子操作 | O(1) |
| 一條 BITFIELD | O(子操作數) |

### 使用情境

- **多計數器打包**：一個用戶的多種小計數（如各功能點擊數）打包在一個 key 的不同 bit 段，比多個 String key 省 key 開銷與記憶體。
- **緊湊狀態機 / 標誌位**：把多個枚舉狀態壓進固定寬度欄位。
- **有上限的計數器**：用 `OVERFLOW SAT` 讓計數器封頂不溢位（如信譽分 0~255）。

### OVERFLOW 三種模式

- `WRAP`（預設）：溢位回繞（u8 從 255 +1 → 0）。
- `SAT`：飽和，封頂/封底（u8 從 255 +1 → 255；i8 觸底停在下限）。
- `FAIL`：溢位時該子操作回 nil、不改值。

### 踩坑清單

1. **offset 與型別要自己算對**：用 `#n` 索引比手算 bit offset 安全；混用不同型別時 `#n` 是「以該型別為單位」，容易錯位。
2. **型別寬度要留足**：u8 只到 255，計數器會爆；預估上限選寬度，或用 SAT 防溢位。
3. **同樣有大 offset 撐爆記憶體問題**：`BITFIELD key SET u8 '#100000000' 1` 會配置巨大字串，offset 要密集。
4. **有號無號別搞混**：`i8` 範圍 -128~127，`u8` 是 0~255，讀寫型別必須一致，否則值解讀錯誤。
5. **可讀性差、難維運**：把語意壓進 bit 段後，資料變得不透明，除錯困難；只有在記憶體真的是瓶頸時才值得。
6. **OVERFLOW 只影響其後的子操作**：要在對應 INCRBY 之前宣告。

### redis-cli 範例

```bash
# 在一個 key 打包三個 u8 計數器（第 0、1、2 個）
127.0.0.1:6379> BITFIELD counters SET u8 '#0' 10 SET u8 '#1' 20 SET u8 '#2' 30
1) (integer) 0
2) (integer) 0
3) (integer) 0
127.0.0.1:6379> BITFIELD counters GET u8 '#1'
1) (integer) 20

# 帶飽和的加法（封頂 255）
127.0.0.1:6379> BITFIELD counters OVERFLOW SAT INCRBY u8 '#0' 250
1) (integer) 255
127.0.0.1:6379> BITFIELD counters OVERFLOW SAT INCRBY u8 '#0' 100
1) (integer) 255

# 溢位失敗模式
127.0.0.1:6379> BITFIELD counters OVERFLOW FAIL INCRBY u8 '#2' 250
1) (nil)
```

### Go 範例

```go
ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

// 打包設定三個 u8 欄位
rdb.BitField(ctx, "counters",
	"SET", "u8", "#0", 10,
	"SET", "u8", "#1", 20,
	"SET", "u8", "#2", 30,
)

// 讀第 1 個欄位
vals, _ := rdb.BitField(ctx, "counters", "GET", "u8", "#1").Result()
fmt.Println("field#1 =", vals[0]) // 20

// 飽和加法防溢位
res, _ := rdb.BitField(ctx, "counters",
	"OVERFLOW", "SAT", "INCRBY", "u8", "#0", 250).Result()
fmt.Println("saturated =", res[0]) // 255
```

### 專案流程說明：實作「用戶功能使用次數面板（記憶體極限優化）」

1. 假設要記錄每個用戶對 8 個功能的當日使用次數（每個 0~255 足夠），為每個用戶用一個 key `feat:<uid>:<yyyymmdd>`，內部 8 個 u8 欄位。
2. 用戶使用功能 k → `BITFIELD feat:<uid>:<date> OVERFLOW SAT INCRBY u8 '#k' 1`，封頂 255 不溢位。
3. 面板讀取 → 一條 `BITFIELD ... GET u8 #0 GET u8 #1 ...` 原子取回全部 8 個欄位。
4. 每個 key 佔 8 byte（8 個 u8），比 8 個獨立 String key 省下大量 key metadata 記憶體。
5. key 設當日 TTL 自動清理。
6. 前提是「記憶體真的是瓶頸」且欄位語意穩定；否則用 Hash（`HINCRBY`）可讀性更好，維運成本更低。

---

## 11. 練習題

> 建議先自己想指令與資料模型，再驗證。

**String**
1. 設計一個「每秒最多 5 次」的簡易固定窗限流，只用 String 的 `INCR` 與 `EXPIRE` 兩指令，寫出邏輯並說明它與滑動窗（ZSet）的差異與缺陷（窗邊界突刺）。

**Hash**
2. 用一個 Hash 存購物車（商品→數量），實作「加入商品」「減少數量到 0 時移除該商品」「清空購物車」三個操作的指令序列。並說明何時這個 Hash 會從 listpack 轉成 hashtable。

**List**
3. 用 List 實作「最近 20 筆搜尋歷史且去重」（同一詞再搜要移到最前面）。提示：`LREM` + `LPUSH` + `LTRIM`。

**Set**
4. 給定 `user:A:tags`、`user:B:tags`、`user:C:tags`，寫出「A 與 B 共同、但 C 沒有」的標籤查詢（交集後差集），並說明如何避免對大 Set 阻塞。

**ZSet（做兩題）**
5. 實作一個排行榜，要求「同分數時較早達成者排前面」。設計 score 的組合編碼公式，並說明 2^53 精度限制如何影響你的設計。
6. 用 ZSet 實作延遲隊列，寫出「投遞一個 30 秒後執行的任務」與「消費端原子取出所有到期任務」的完整指令（消費端請用 Lua 保證原子）。

**Stream**
7. 一個 consumer 崩潰後重啟，如何確保它先處理完自己未 ack 的訊息再消費新訊息？寫出兩階段的 `XREADGROUP` 指令，並說明 `0` 與 `>` 的差異。

**Bitmap**
8. 用 bitmap 算「本週（7 天）每天都登入」的用戶數，寫出 `BITOP` + `BITCOUNT` 指令。若 uid 是 UUID 而非自增整數，為什麼不能用 bitmap？該改用什麼？

**HyperLogLog**
9. 你要同時知道「網站總 UV」和「同時訪問了 A 頁與 B 頁的去重用戶數」。哪一個 HLL 能做、哪一個不能？不能的那個該用什麼結構？

**Geo**
10. 寫出「找出座標 (121.55, 25.04) 周圍 2km 內最近 5 家店，並回傳距離」的 `GEOSEARCH` 指令。若要刪除其中一家店，為什麼不能用 GEODIST 類指令，該用什麼？

**Bitfield**
11. 用 BITFIELD 在單一 key 打包 4 個信譽維度計數器（各 0~255，封頂不溢位），寫出「初始化」「某維度 +50」「一次讀回全部 4 個」的指令。

---

## 12. 檢查點（自我檢核）

讀完本文件，你應該能不查資料回答：

- [ ] String 的三種編碼是什麼？embstr 與 raw 的分界（44 bytes）與轉換不可逆性。
- [ ] 為何 `SET k v EX n` 比先 SET 再 EXPIRE 好？
- [ ] Hash / Set / ZSet 各自的 listpack 轉碼 config 名稱與預設值，以及「轉碼不可逆」的共同特性。
- [ ] List 為什麼 LINDEX 是 O(N)？可靠佇列如何用 BLMOVE 模擬 ack？
- [ ] intset 何時升級？升級的代價？
- [ ] ZSet 的 skiplist 編碼為什麼同時需要跳表與 hashtable？
- [ ] 用 ZSet 實作排行榜、延遲隊列、滑動窗限流、時間序索引，各自 score 與 member 怎麼設計？
- [ ] Stream 的 `>` 與具體 ID 在 XREADGROUP 中的差別？PEL 堆積如何監控與回收（XPENDING/XAUTOCLAIM）？
- [ ] 為什麼 ack 不釋放記憶體？Stream 如何做記憶體治理？
- [ ] Bitmap 的記憶體由什麼決定？為什麼稀疏大 offset 危險？
- [ ] HLL 的三個關鍵數字（~12KB、0.81%、16384 registers）？為什麼不能做交集、不能列舉成員？
- [ ] Geo 底層是什麼結構？經緯度參數順序？如何刪點？
- [ ] Bitfield 的 OVERFLOW 三模式（WRAP/SAT/FAIL）分別行為？何時該用 Bitfield 而非 Hash？
- [ ] 給一個新需求，你能用「選型三問」在 30 秒內選出合適結構並說出成本。

---

## 13. 延伸閱讀

**官方指令參考（redis.io）**
- Commands 總表：https://redis.io/commands/
- Data types 概覽：https://redis.io/docs/latest/develop/data-types/
- String：https://redis.io/docs/latest/develop/data-types/strings/
- Hash（含 field TTL / HEXPIRE，7.4+）：https://redis.io/docs/latest/develop/data-types/hashes/
- List：https://redis.io/docs/latest/develop/data-types/lists/
- Set：https://redis.io/docs/latest/develop/data-types/sets/
- Sorted Set：https://redis.io/docs/latest/develop/data-types/sorted-sets/
- Stream 教學：https://redis.io/docs/latest/develop/data-types/streams/
- Bitmap：https://redis.io/docs/latest/develop/data-types/bitmaps/
- Bitfield：https://redis.io/docs/latest/develop/data-types/bitfields/
- HyperLogLog：https://redis.io/docs/latest/develop/data-types/probabilistic/hyperloglogs/
- Geospatial：https://redis.io/docs/latest/develop/data-types/geospatial/
- 記憶體優化與編碼 config：https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/memory-optimization/

**書籍**
- 黃健宏《Redis 設計與實作》（第二版）：
  - 第 2 章 SDS、第 7~8 章物件系統與各編碼（int/embstr/raw、ziplist/listpack、intset、skiplist、quicklist）——理解本文件「底層編碼」章節的最佳補充。
  - 對照本文件時特別留意：書中的 ziplist 在 Redis 7 已被 listpack 取代，config 名稱也從 `*-ziplist-*` 改為 `*-listpack-*`（舊名相容）。

**go-redis/v9**
- 官方文件：https://redis.io/docs/latest/develop/clients/go/
- GitHub：https://github.com/redis/go-redis

---

> 下一階段（Stage 2）預告：持久化（RDB/AOF）、過期與淘汰策略、事務與 Lua、Pipeline、發布訂閱、以及本文件多次提到的 big key 治理與 SCAN 家族的系統化實踐。
