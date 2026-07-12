# Stage 5：並發原語 — 事務、Lua、Pipeline、分散式鎖

> 模組：`github.com/twteam/go-redis-kit` · Go 1.26.1 · 客戶端 `github.com/redis/go-redis/v9` v9.7.0 · 分散式鎖 `github.com/go-redsync/redsync/v4` v4.13.0

---

## 1. 本階段目標

Redis 是**單執行緒**處理命令的（指單一 shard 的命令執行迴圈；I/O 多工在 6.0 後可多執行緒，但命令本身仍串列執行）。這個特性是本階段所有「原子性」討論的根基：只要一段邏輯能在「一次命令」或「一段不可中斷的腳本」內完成，它對其他客戶端就是原子的。

本階段要徹底搞懂四種並發原語，以及它們各自「保證什麼、不保證什麼」：

| 原語 | 原子性 | 減少 RTT | 可放棄/回滾 | 典型用途 |
|------|--------|----------|-------------|----------|
| 事務 MULTI/EXEC | 是（整批一次執行，不被插隊） | 是 | 只能 DISCARD（執行前），**執行中不回滾** | CAS 樂觀鎖、多 key 一致變更 |
| Lua 腳本 | 是（腳本執行期間獨佔） | 是（一次往返） | 無回滾，但可在腳本內用邏輯自行判斷 | 條件式讀寫、複雜原子操作、鎖釋放 |
| Pipeline | **否**（只是批次送） | 是（核心目的） | 無 | 大量獨立命令批次化 |
| 分散式鎖 | 依實作 | — | 靠 TTL/主動釋放 | 跨行程/跨機器互斥 |

學完你應該能回答：

- 為什麼 Redis 事務「不是真事務」？指令錯了會怎樣？
- `WATCH` 樂觀鎖（CAS）在 go-redis 裡怎麼寫？衝突時如何重試？
- `redis.NewScript` 為什麼能自動 `EVALSHA` 又能 fallback？
- Pipeline 和事務差在哪？為什麼 Pipeline **不是**原子的？
- 一個「正確」的單節點分散式鎖長什麼樣？三大坑（釋放別人的鎖、業務超 TTL、主從切換丟鎖）各自的時序與解法是什麼？
- watchdog 續期、fencing token、Redlock 分別解決什麼問題、代價是什麼？
- 什麼時候該用 Redis 鎖、什麼時候絕對不該（例如金流）？

本階段所有 Go 範例皆為可編譯的 go-redis/v9 真實程式碼。假設你已用 `redis.NewClient` 取得 `*redis.Client`，並有 `ctx context.Context`。

---

## 2. 事務：MULTI / EXEC / DISCARD / WATCH

### 2.1 四個命令的語意

| 命令 | 作用 |
|------|------|
| `MULTI` | 開始一個事務，之後的命令**不立即執行**，而是排入佇列（伺服器回 `QUEUED`） |
| `EXEC` | 一次性、原子地執行佇列中所有命令，回傳每個命令的結果陣列 |
| `DISCARD` | 放棄佇列，什麼都不執行 |
| `WATCH key [key ...]` | 對 key 做**樂觀鎖監視**：從 WATCH 到 EXEC 之間，只要任一被監視的 key 被任何客戶端改動，EXEC 就整批放棄（回傳 nil） |
| `UNWATCH` | 取消所有監視 |

一次典型的 CAS 交易在 redis-cli 裡長這樣：

```bash
WATCH balance:1001
# 讀出目前值，在應用層算好新值
GET balance:1001         # 假設 100
MULTI
DECRBY balance:1001 30
INCRBY balance:2002 30
EXEC
# 若在 WATCH 之後、EXEC 之前有人改了 balance:1001，EXEC 回 (nil)，整批不執行
```

`EXEC` 回 `(nil)`（go-redis 是 `redis.TxFailedErr`）就代表「被監視的 key 變了、樂觀鎖失敗」，你要**重讀、重算、重試**。

### 2.2 為什麼「不是真事務」— 三個關鍵反直覺

這是最多人誤解的地方。Redis 事務**只保證兩件事**：隔離性（EXEC 期間不被其他命令插隊）與批次原子提交（要嘛整批進佇列後一起跑，要嘛 DISCARD/WATCH 失敗整批不跑）。它**不提供** RDBMS 那種「失敗自動回滾」。

**(1) 沒有回滾（no rollback）。** 一旦 `EXEC` 開始跑，佇列裡的命令會**全部執行完**，即使中間某個命令在執行時報錯。錯的命令回傳錯誤，但它**不會**讓前後其他命令撤銷。antirez 的官方立場：只有「程式 bug」才會造成執行期錯誤（例如對 string 做 `INCR` 的型別錯誤），這種東西應該在開發期就被抓出來，正式環境不該發生；為此加入回滾機制只會拖慢 Redis，不值得。

**(2) 兩類錯誤，行為不同：**

- **入列期錯誤（語法錯 / 命令不存在）**：在 `MULTI` 後打了一個 Redis 根本不認得的命令，伺服器**立刻回錯**且**標記整個事務為髒**，之後 `EXEC` 會直接失敗、**一個命令都不執行**（自 Redis 2.6.5 起）。

  ```bash
  MULTI
  SET k1 v1
  NOTACOMMAND foo        # (error) ERR unknown command  ← 立即報錯
  SET k2 v2
  EXEC                   # (error) EXECABORT ← 整批放棄，k1、k2 都沒被設
  ```

- **執行期錯誤（型別不對等，語法本身合法）**：命令能入列（`QUEUED`），但真正跑的時候才發現錯，例如對一個 string key 做 list 操作。此時**錯的那條回錯，其餘照跑**：

  ```bash
  SET counter hello
  MULTI
  INCR counter           # QUEUED（語法合法，能入列）
  SET flag done          # QUEUED
  EXEC
  # 1) (error) ERR value is not an integer or out of range   ← INCR 失敗
  # 2) OK                                                     ← 但 SET flag 照樣成功！
  ```

  **記住這個心智模型：Redis 事務 = 「一段不被插隊的命令序列」，不等於「全有或全無的資料一致性單位」。** 需要真正的條件式原子邏輯，請用 Lua（第 3 節）。

**(3) 佇列裡看不到中間值。** 因為命令是先排隊、EXEC 才一起跑，你**無法**在事務內「讀出 A 的結果再拿去當 B 的參數」。要做「讀-改-寫」的條件邏輯，只有兩條路：WATCH 樂觀鎖（在應用層讀改）或 Lua（在腳本內讀改）。

### 2.3 WATCH 樂觀鎖 = CAS（Compare-And-Swap）

`WATCH` 把「悲觀鎖」變成「樂觀鎖」：不預先鎖住 key，而是先記住它的狀態，等到 `EXEC` 時檢查「這段期間有沒有人動過」，動過就整批放棄、由客戶端重試。這正是 CAS 的精神。適合**衝突不頻繁**的場景；衝突很頻繁時重試成本高，改用 Lua 或悲觀鎖更好。

時序（兩個客戶端搶改同一個 key）：

```
Client A                         Client B
WATCH k                          
GET k -> 100                     
                                 WATCH k
                                 GET k -> 100
                                 MULTI; SET k 130; EXEC  -> OK  (k 現在 130)
MULTI                            
SET k 120                        
EXEC -> (nil)  ← A 被監視的 k 已被 B 改動，整批放棄，A 需重試
```

### 2.4 Go 範例：TxPipelined + Watch 實作 CAS 轉帳（完整可跑）

go-redis 把「WATCH + MULTI + EXEC」封裝成 `client.Watch(ctx, fn, keys...)`：你傳入的 `fn` 收到一個 `*redis.Tx`，在裡面做讀取，然後呼叫 `tx.TxPipelined` 送出要原子執行的寫入。若被監視的 key 變了，`Watch` 回傳 `redis.TxFailedErr`，你自行重試。

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

// transfer 從 from 扣 amount、加到 to，使用 WATCH 樂觀鎖確保餘額不會超扣。
// 衝突（TxFailedErr）時自動重試 maxRetries 次。
func transfer(ctx context.Context, rdb *redis.Client, from, to string, amount int64, maxRetries int) error {
	// txf 是一次 CAS 嘗試。
	txf := func(tx *redis.Tx) error {
		// 1) 在事務外先讀（此時 from、to 已被 WATCH 監視）。
		balance, err := tx.Get(ctx, from).Int64()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		// 2) 應用層做業務判斷。
		if balance < amount {
			return fmt.Errorf("餘額不足：%s 有 %d，需要 %d", from, balance, amount)
		}
		// 3) 把「要原子執行」的寫入放進 TxPipelined。
		//    這段等同 MULTI ... EXEC；若監視的 key 在此期間被改動，Watch 會回 TxFailedErr。
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.DecrBy(ctx, from, amount)
			pipe.IncrBy(ctx, to, amount)
			return nil
		})
		return err
	}

	// 重試迴圈：TxFailedErr 代表 CAS 衝突，重讀重算。
	for i := 0; i < maxRetries; i++ {
		err := rdb.Watch(ctx, txf, from) // 監視來源帳戶（餘額判斷的依據）
		if err == nil {
			return nil // 成功
		}
		if errors.Is(err, redis.TxFailedErr) {
			continue // 有人改了 from，重試
		}
		return err // 其他錯誤（含餘額不足）直接回
	}
	return errors.New("transfer 達到最大重試次數仍失敗（高競爭）")
}

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	rdb.Set(ctx, "acct:A", 100, 0)
	rdb.Set(ctx, "acct:B", 0, 0)

	if err := transfer(ctx, rdb, "acct:A", "acct:B", 30, 5); err != nil {
		log.Fatalf("transfer 失敗: %v", err)
	}

	a, _ := rdb.Get(ctx, "acct:A").Int64()
	b, _ := rdb.Get(ctx, "acct:B").Int64()
	fmt.Printf("A=%d B=%d\n", a, b) // A=70 B=30
}
```

> 重點：`tx.TxPipelined` 內只放**寫入**，讀取要放在它**外面**（那才受 WATCH 保護）。若把 `Get` 放進 `TxPipelined`，它只會被入列、拿不到值，違背 CAS 的初衷。

---

## 3. Lua 腳本：EVAL / EVALSHA / SCRIPT LOAD

### 3.1 為什麼要 Lua

WATCH 的樂觀鎖在高競爭下要不斷重試。Lua 換個思路：把整段「讀-判斷-寫」邏輯**送到伺服器端一次執行**。因為 Redis 命令串列執行，**腳本執行期間不會有任何其他命令插進來**，所以整段腳本天然原子。而且只需**一次 RTT**。

### 3.2 三個命令

| 命令 | 作用 |
|------|------|
| `EVAL script numkeys key... arg...` | 直接送腳本原文＋參數執行 |
| `SCRIPT LOAD script` | 把腳本載入伺服器快取，回傳其 SHA1 |
| `EVALSHA sha1 numkeys key... arg...` | 用 SHA1 執行已快取的腳本（省去每次傳整段腳本的頻寬） |

`KEYS[]` 與 `ARGV[]`：腳本裡用 `KEYS[1]`、`KEYS[2]`…拿 key 名稱，`ARGV[1]`、`ARGV[2]`…拿其他參數。**務必把 key 放 KEYS、其餘放 ARGV**——這不只是慣例，Cluster 模式靠 KEYS 判斷 slot 路由，放錯會導致 CROSSSLOT 錯誤或路由到錯的節點。

```bash
# 只有當 key 的值等於預期值時才刪除（鎖釋放的經典用法）
EVAL "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end" 1 mylock token-abc123
```

### 3.3 原子性保證與限制

- **原子**：腳本從第一行到最後一行是一個不可分割的執行單位。
- **無回滾**：和事務一樣，腳本中途若某個 `redis.call` 出錯（例如型別錯），會中止腳本並回錯，但**之前已執行的寫入不會撤銷**。用 `redis.pcall`（protected call）可捕捉錯誤自行處理而不中止。
- **阻塞全庫**：腳本執行期間**整個 Redis 實例被獨佔**，其他客戶端全部等待。所以腳本必須**快**。
  - **坑：腳本內別跑慢操作。** 不要在腳本裡 `KEYS *`、掃大 range、跑 O(N) 的大集合運算、或迴圈幾十萬次。超過 `lua-time-limit`（預設 5 秒）Redis 會開始對其他連線回 `BUSY`，但此時只能 `SCRIPT KILL`（僅限未寫入的腳本）或 `SHUTDOWN NOSAVE`，非常痛。

### 3.4 確定性複製與 `redis.replicate_commands()`

主從複製 / AOF 需要「replica 重放腳本能得到和 master 一樣的結果」。早期 Redis 採**腳本複製**（把整段腳本傳給 replica 重跑），因此**禁止腳本產生非確定性結果**——不能在寫入前呼叫 `TIME`、`randomkey`、`SRANDMEMBER`、未排序集合的隨機順序等，否則 master 與 replica 資料會分歧。

Redis 3.2+ 引入 `redis.replicate_commands()`，切換成**效果複製（effects replication）**：不再傳腳本，而是把腳本實際執行產生的寫入命令傳給 replica。這樣腳本內就能安全使用隨機/時間，因為傳過去的是「已定案的具體寫入」。

- **Redis 5.0+**：效果複製成為**預設**，你通常不需要再手動呼叫 `redis.replicate_commands()`（呼叫也無害）。
- **實務建議**：仍盡量讓腳本邏輯確定；若必須用隨機/時間，確保跑在支援效果複製的版本上。時間類參數的最佳實務是**從 `ARGV` 傳進去**（由客戶端提供 `now`），而非在腳本內取 `TIME`，這樣最確定、也最好測試。

### 3.5 go-redis 的 `redis.NewScript`：自動 EVALSHA / fallback

go-redis 提供 `redis.NewScript(src)`，它把上面「先試 EVALSHA、失敗再 EVAL」的最佳實務封裝好：

- 第一次 `Run` 時先算出腳本 SHA1，直接嘗試 `EVALSHA`；
- 若伺服器回 `NOSCRIPT`（腳本不在快取，例如 Redis 重啟或第一次執行），go-redis **自動 fallback 成 `EVAL`**（送完整腳本，順帶把它載入快取）；
- 之後的呼叫都走 `EVALSHA`，省頻寬。

你只管寫腳本，快取管理交給它。

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

// incrByWithCap：原子地把 key 加 delta，但不超過 cap；回傳加完後的值。
// 全程在伺服器端一次完成，無競爭問題、一次 RTT。
var incrByWithCap = redis.NewScript(`
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local delta   = tonumber(ARGV[1])
local cap     = tonumber(ARGV[2])
if current + delta > cap then
    return redis.error_reply('would exceed cap')
end
local newval = current + delta
redis.call('SET', KEYS[1], newval)
return newval
`)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	rdb.Set(ctx, "quota:user:1", 90, 0)

	// Run 會自動 EVALSHA，NOSCRIPT 時 fallback 成 EVAL。
	// 參數順序：ctx, client, []string{keys...}, args...
	n, err := incrByWithCap.Run(ctx, rdb, []string{"quota:user:1"}, 5, 100).Int64()
	if err != nil {
		log.Fatalf("run script: %v", err)
	}
	fmt.Println("new value:", n) // 95

	// 再加 10 會超過 cap=100，腳本回錯。
	_, err = incrByWithCap.Run(ctx, rdb, []string{"quota:user:1"}, 10, 100).Result()
	fmt.Println("expect error:", err) // would exceed cap
}
```

> 進階：可用 `script.Load(ctx, rdb)` 在啟動時預先把腳本推進所有節點快取；`script.Hash()` 取得 SHA1；Cluster 模式下 go-redis 會處理各節點的載入。

### 3.6 Lua 的成本與適用邊界（何時用 Lua vs 單指令 vs MULTI）

常聽到「Lua 很 cost 大」——這是**半對的簡化**。Lua 的成本 = **腳本裡做多少事**，因為它整段獨佔單線程。拆開看：

**Lua 其實「便宜」的地方**

- 小腳本（如限流 `INCR`+`EXPIRE`，幾行 O(1)）在同一線程 in-process 執行，微秒級。
- **一次 RTT 取代多條指令** → 比分開發好幾個命令更省網路（RTT 才是主要成本）。
- `EVALSHA` 用 SHA 快取，第一次載入後只送 hash，不重傳腳本文字。
- 換到原子性，省掉 WATCH/MULTI 的重試迴圈。

所以一個 O(1) 小腳本，通常比「多次獨立指令」**還便宜**。

**Lua 真正「貴 / 危險」的地方**

- **阻塞整個單線程**：腳本原子執行 = 跑腳本時其他 client 全等。腳本內若跑 O(N) 大 N / 大迴圈（如 `for i=1,1000000`）就會卡住整台 Redis（見 3.3）。**這才是「Lua 貴」的真正所指。**
- 不可中斷、除錯/監控成本高、動態拼字串會撐爆腳本快取（用 `NewScript` 定義一次，別每次組新的）。

**判準：Lua 成本看「腳本內複雜度」，不是 Lua 本身。**

- O(1) / 有界小腳本（限流、鎖、條件計數）→ 便宜，該用 ✅
- 腳本內 O(N) 大 N / 大迴圈 → 貴且危險，別放進 Lua ❌

所以限流 / 鎖用 Lua 是**合理且最佳**的成本選擇（幾個 O(1) 指令、微秒級、一次 RTT、換到原子）。面試答法：「**貴的是腳本內做的事，不是 Lua 本身；小 O(1) 腳本反而省 RTT，我只在需要原子且工作量有界時用，絕不在腳本內跑大迴圈。**」

**三選一決策表**

| 需求 | 用什麼 | 為什麼 |
| --- | --- | --- |
| 單一操作 | **單指令**（`INCR`/`DEL`/`SET k v EX n`/`SETNX`） | 本身就原子，別包 Lua |
| 多命令一起原子、**但不需讀中間值分支** | `MULTI`/`EXEC` 或 Pipeline+MULTI | 排隊一次執行 |
| 多命令原子、**且要讀中間值再決定下一步** | **Lua** | MULTI 拿不到中間結果、不能分支；只有 Lua 能 `if redis.call(...) then ...` |
| 腳本內要跑重運算 / O(N) 大 N | **都不要**，改拆到應用層 | 會阻塞全庫 |

一句話：**能用單指令就別用 Lua；MULTI 不能讀值分支，「條件式原子」只能 Lua；但 Lua 腳本必須短而有界。**

**若極度在意那幾微秒**：固定窗可接受微小 race → `INCR`+`EXPIRE` 用 pipeline 分兩步（省 Lua，但有極小無-TTL 風險）；或用 Redis Functions（7.0+，腳本後繼、載入一次可重用）、`redis-cell` 模組（`CL.THROTTLE` 原生限流，需裝模組）。

---

## 4. Pipeline：批次減少 RTT

### 4.1 核心價值：省 RTT，不是原子

每個 Redis 命令的成本 ≈ **網路往返時間（RTT）＋ 極短的伺服器處理時間**。當你要送 1000 個命令，一來一回等 1000 次 RTT，網路延遲會主宰總耗時。Pipeline 讓你**一次把 N 個命令全部送出、再一次收回 N 個回應**，把 N 次 RTT 壓成 1 次。跨機房、雲端環境（RTT 動輒 0.5~2ms）下，Pipeline 常帶來數十倍吞吐提升。

### 4.2 Pipeline **不是**原子的（與事務的核心差異）

這是必考點：

| | Pipeline | 事務（MULTI/EXEC） |
|---|----------|--------------------|
| 目的 | 減少 RTT | 原子性 + 隔離性 |
| 執行 | 命令**照序送達、依序執行**，但**其他客戶端的命令可以插進來** | EXEC 期間**不被插隊** |
| 原子性 | **無**。中間別的連線可能穿插執行 | 有 |
| 部分失敗 | 各命令獨立成敗，互不影響 | 執行期錯誤那條回錯，其餘照跑（也不回滾） |
| 回傳 | 每個命令各自的結果 | EXEC 回一個結果陣列，或 nil（WATCH 失敗） |

換句話說：**Pipeline 只是「打包傳輸」的優化，不改變命令之間的隔離語意。** 你在 pipeline 中間插入的 1000 個 `INCR`，別的客戶端仍可能在你第 500 條和第 501 條之間執行它自己的命令。需要原子就包在 `TxPipeline`（MULTI/EXEC）或 Lua 裡。

go-redis 的對應：`rdb.Pipeline()` / `rdb.Pipelined(fn)` 是**非事務** pipeline；`rdb.TxPipeline()` / `rdb.TxPipelined(fn)` 會自動用 `MULTI`/`EXEC` 包起來（第 2.4 節用的就是這個）。

### 4.3 分批：100~1000 一批，防 buffer 爆

Pipeline 把命令累積在客戶端與伺服器的緩衝區，`EXEC`/flush 前不釋放。若一次塞幾十萬條：

- 客戶端記憶體與伺服器 output buffer 膨脹，可能觸發 `client-output-buffer-limit` 被斷線；
- 這批命令期間，其他請求的回應被延後，長尾延遲惡化。

實務準則：**每批 100~1000 條，跑完一批再送下一批**（依命令大小與網路調整）。

### 4.4 Go 範例：Pipelined 分批寫入

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

// batchSet 分批寫入大量 key，每批 batchSize 條，避免緩衝區爆掉。
func batchSet(ctx context.Context, rdb *redis.Client, kv map[string]string, batchSize int) error {
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]
		// Pipelined：一次送 batch 條 SET，一次收回。非原子，純粹省 RTT。
		_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, k := range batch {
				pipe.Set(ctx, k, kv[k], 0)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("batch [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

// 讀取時同樣可批次：先 pipe 出所有命令，再逐一取結果。
func batchGet(ctx context.Context, rdb *redis.Client, keys []string) (map[string]string, error) {
	cmds := make([]*redis.StringCmd, len(keys))
	_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, k := range keys {
			cmds[i] = pipe.Get(ctx, k) // 先把 *StringCmd 存起來，flush 後才有值
		}
		return nil
	})
	// redis.Nil（key 不存在）會讓整批 err 非 nil，但個別 cmd 仍可讀，故不在此直接 return。
	if err != nil && err != redis.Nil {
		return nil, err
	}
	out := make(map[string]string, len(keys))
	for i, c := range cmds {
		v, e := c.Result()
		if e == redis.Nil {
			continue // 此 key 不存在，略過
		}
		if e != nil {
			return nil, e
		}
		out[keys[i]] = v
	}
	return out, nil
}

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	data := map[string]string{}
	for i := 0; i < 5000; i++ {
		data[fmt.Sprintf("k:%d", i)] = fmt.Sprintf("v:%d", i)
	}
	if err := batchSet(ctx, rdb, data, 500); err != nil { // 每批 500
		log.Fatal(err)
	}
	got, err := batchGet(ctx, rdb, []string{"k:0", "k:1", "k:nope"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(got) // map[k:0:v:0 k:1:v:1]
}
```

> 注意 pipeline 內的命令回傳值（`*StringCmd` 等）在 `Pipelined` 的 `fn` 回傳、緩衝被 flush **之後**才填入結果。所以要在迴圈裡先把 cmd 物件存起來，flush 後再讀 `.Result()`。

---

## 5. 分散式鎖（本階段最重要）

多個行程 / 多台機器要互斥地存取某個共享資源時，就需要「分散式鎖」。Redis 常被拿來當鎖，因為快又簡單。但**魔鬼全在細節**——一個看似能動的鎖，藏著三個會在生產環境咬人的坑。我們從最單純的正解一路推導到 Redlock。

### 5.1 單節點正解：`SET k <token> NX PX ttl`

**一行命令**就是正確的獲取鎖方式：

```bash
SET lock:order:1001 <token> NX PX 30000
```

逐 flag 拆解：

| 部分 | 意義 | 少了會怎樣 |
|------|------|-----------|
| `SET lock:order:1001` | 用一個 key 代表「這把鎖」 | — |
| `<token>` | value 存一個**隨機唯一 token**（如 UUID），代表「這把鎖是我持有的」 | 少了 token → 釋放時無法辨認是不是自己的鎖 → **坑 (a)** |
| `NX` | Not eXists：**只有 key 不存在時才設定成功**。這是互斥的核心——同時只有一個客戶端能 SET 成功 | 少了 NX → 會覆蓋別人的鎖，根本沒互斥 |
| `PX 30000` | 設定 30000ms 過期。**必須有 TTL**，否則持有者當機後鎖永不釋放（死鎖） | 少了 TTL → 崩潰即死鎖 |

`SET ... NX PX` 是**單一原子命令**：設值＋設過期一次完成。**千萬別**用「`SETNX` 成功後再 `EXPIRE`」兩步——這兩步之間若客戶端崩潰，就留下一把沒有 TTL 的永久鎖。

回傳 `OK` 代表搶到，回傳 `nil` 代表沒搶到（鎖被別人持有）。

### 5.2 釋放：Lua 驗證腳本（GET 比對 token 再 DEL）

釋放**不能**直接 `DEL lock:order:1001`，必須「先確認這把鎖還是我持有的（value == 我的 token）才刪」，而且這個「檢查再刪」**必須原子**——用 Lua：

```lua
-- release.lua：只有當鎖的 value 等於我的 token 時才刪除
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
```

為什麼一定要 Lua 而非「`GET` 比對後 `DEL`」兩步？見下面坑 (a) 的時序。

### 5.3 三大坑（逐一時序拆解）

#### 坑 (a)：釋放了別人的鎖 → 用 token 解決

**不安全的釋放**（`GET` 比對 + `DEL` 兩步，或根本直接 `DEL`）會發生：

```
時間軸
C1: SET lock v1 NX PX 30000       → 搶到，開始做事
C1: 業務執行超過 30s ...           → 鎖 TTL 到期，自動釋放！
C2: SET lock v2 NX PX 30000       → C2 搶到（合法）
C1: 業務終於做完，DEL lock         → 刪掉的是 C2 的鎖！C2 還在做事卻沒鎖了
C3: SET lock v3 NX PX 30000       → C3 也搶到 → C2、C3 同時持有 → 互斥破功
```

**解法**：value 存唯一 token；釋放時「value 是我的 token 才刪」。且比對＋刪除要原子（Lua），否則「比對通過後、DEL 前」鎖又過期並被別人搶走，還是會刪錯。token 把「鎖」和「鎖的一次持有」綁定起來，讓釋放操作認得主人。

#### 坑 (b)：業務執行超過 TTL → watchdog 續期

TTL 設多長都是賭博：設短了怕業務沒做完鎖就掉（引發坑 a 的骨牌）；設長了怕持有者崩潰後別人要等很久。**根本解法是 watchdog（看門狗）**：TTL 給一個保守值（如 30s），開一個背景 goroutine 定期「續命」，只要業務還在跑就把 TTL 往後延，業務結束就停止續期讓鎖自然到期。

```
C1: SET lock v1 NX PX 30000
    ├─ watchdog 每 10s（TTL/3）PEXPIRE lock 30000  ← 只要業務還在，就一直把到期時間推回 30s 後
    ├─ 業務做了 70s（>30s）也不會掉鎖
    └─ 業務完成 → 停止 watchdog → Lua 驗證釋放
```

#### 坑 (c)：主從切換丟鎖 → Redlock（或別靠 Redis）

單節點 Redis（含 master-replica + Sentinel）複製是**非同步**的。時序：

```
C1: SET lock v1 NX PX 30000  → 寫到 master，master 回 OK（C1 以為搶到）
    ↓ 此時這筆寫入「還沒」複製到 replica
master 當機，Sentinel 把 replica 提升為新 master
    ↓ 新 master 上「根本沒有」這把鎖
C2: SET lock v2 NX PX 30000  → 在新 master 搶到
    → C1 和 C2 同時認為自己持有鎖！互斥破功
```

這是**單點 Redis 鎖的本質侷限**：只要有故障轉移 + 非同步複製，就可能兩個客戶端同時持鎖。針對這點，antirez 提出 **Redlock**（見 5.6）。但 Redlock 也有爭議，強一致場景更該用 fencing token 或別把互斥責任交給 Redis（見 5.5、5.7、第 6 節）。

### 5.4 watchdog 實作原理

- 搶到鎖後，啟一個背景 goroutine。
- 每隔 **TTL/3**（給足夠的重試餘裕；若一次續期失敗，還有兩次機會在 TTL 內補救）用 **`PEXPIRE`**（或帶 token 驗證的 Lua）把 TTL 重設回完整長度。
- **續期也要驗 token**：只有「這把鎖還是我的」才續，避免鎖已被別人搶走後我還幫別人續命。用 Lua：`if GET==token then PEXPIRE ...`。
- 業務結束（或呼叫 Release）時，透過 channel/context **通知 goroutine 停止**續期，讓鎖自然到期或主動釋放。
- watchdog 是 Redisson 的預設行為（預設 lease 30s、每 10s 續一次），我們在 5.8 手刻一個等效版本。

> watchdog 的代價：如果持鎖行程**卡住但沒死**（例如 GC 長暫停、被 OS 換出、網路分區到 Redis），watchdog 可能持續續期，或反之無法續期。它**降低**了業務超時掉鎖的機率，但**不能提供強一致保證**——這正是 fencing token 要補的洞。

### 5.4.1 等鎖：`SetNX` 不阻塞，怎麼處理「沒搶到」

`SetNX` / `SET NX` **立刻回傳** `true`（搶到）或 `false`（別人持有），**不會等**。Redis 沒有 blocking 版的鎖。所以「等鎖」要自己實作。三種策略：

| 策略 | 做法 | 適合 |
| --- | --- | --- |
| **Fail-fast** | 沒搶到直接回「忙碌，稍後再試」 | 多數場景，最簡單、不堆積 |
| **重試等待（polling）** | 迴圈重試 `SetNX` + 退避，直到拿到或逾時 | 一定要拿到才能做 |
| **Pub/Sub 通知** | 持有者釋放時 `PUBLISH`，等待者 `SUBSCRIBE` 被喚醒後再搶 | 高競爭、想省輪詢（複雜） |

主流做法是**重試 + 指數退避 + jitter + 逾時**：

```go
func acquireLock(ctx context.Context, rdb *redis.Client,
	key, token string, ttl, waitTimeout time.Duration) (bool, error) {

	deadline := time.Now().Add(waitTimeout)
	backoff := 5 * time.Millisecond
	for {
		ok, err := rdb.SetNX(ctx, key, token, ttl).Result() // token 唯一(uuid)供安全釋放
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil // 搶到
		}
		if time.Now().After(deadline) {
			return false, nil // 等太久，放棄（上層決定回錯或降級）
		}
		sleep := backoff + time.Duration(rand.Int63n(int64(backoff))) // +jitter 防驚群
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(sleep):
		}
		if backoff < 200*time.Millisecond {
			backoff *= 2 // 指數退避，封頂 200ms
		}
	}
}
```

高併發下的三條紀律：

1. **一定要退避 + jitter**：別「無 sleep 緊迴圈狂打 `SetNX`」會把 Redis 打爆；所有 waiter 同時醒來搶 = **驚群（thundering herd）**，加隨機 jitter 錯開。
2. **一定要 `waitTimeout`**：別無限等，否則請求全堆在這、拖垮上游（連鎖阻塞）。等不到就回錯 / 降級。
3. **`ttl` 與 `waitTimeout` 是兩回事**：`ttl` = 拿到後多久自動釋放（防死鎖）；`waitTimeout` = 沒拿到最多等多久放棄。

**想清楚「該不該等」**：一堆人排隊等同一把熱鎖 = 尾延遲爆炸 + Redis 壓力。這種常**該換設計**——用佇列（請求進 queue 序列處理）、fail-fast + 前端重試、或**根本不用鎖**改用原子操作（`INCR`、`UPDATE ... WHERE 條件`）。

**免輪詢版（Pub/Sub）**：持有者釋放時 `DEL lock; PUBLISH lock:release:key ""`，等待者 `SUBSCRIBE` 收到訊息就再搶一次——省掉盲目輪詢，但要管訂閱/超時/訊息遺失兜底，複雜。Java 的 Redisson 內建這套；Go 多數用退避輪詢就夠。生產別手刻，直接用 **redsync**（見 5.8），它把重試/退避/jitter/token/釋放都封好了。

### 5.5 fencing token：防「舊持有者」亂寫

即使有 watchdog，仍有一種情形：持有者 C1 因為 **GC 長暫停 / STW / 被排程換出**而「凍結」，這期間鎖過期、C2 搶到並開始寫共享資源；然後 C1 突然「醒來」，它**以為自己還持有鎖**，繼續往資源寫——造成資料損毀。TTL + watchdog 對這種「行程凍結超過 TTL」無能為力，因為凍結時 watchdog 也一起被凍住了。

**fencing token（柵欄令牌）** 是 Kleppmann 提出的解法：每次成功獲取鎖時，鎖服務回傳一個**單調遞增的序號**（token）。客戶端每次操作共享資源時**帶上這個 token**，而**被保護的資源端（DB、儲存服務）記住見過的最大 token，拒絕比它小的**：

```
C1 取得鎖，token = 33
C1 凍結（GC）...鎖過期
C2 取得鎖，token = 34
C2 寫資源，帶 token=34 → 資源端記錄 max=34，接受
C1 醒來，寫資源，帶 token=33 → 資源端看到 33 < 34 → 拒絕！
```

關鍵洞見：**鎖本身無法阻止一個「以為自己還持鎖」的凍結行程**；只有讓**下游資源**具備「拒絕過期令牌」的能力，才能真正防止亂寫。這也說明為什麼「單靠 Redis 鎖」在強一致場景不夠——你需要下游配合檢查 token（Redis 的 `INCR` 可產生單調 token，但資源端的檢查是不可或缺的另一半）。

### 5.6 Redlock：N 個獨立節點過半

Redlock 是 antirez 為了解決坑 (c)（主從切換丟鎖）提出的演算法。用 **N 個完全獨立**（無主從關係）的 Redis master（通常 N=5），要獲取鎖必須在**過半數（N/2+1，即 3 個）**節點上都搶到。

**獲取步驟：**

1. 記錄開始時間 `t0`。
2. 依序向 N 個節點用**相同的 token 和 TTL** 執行 `SET k token NX PX ttl`，每個節點設**很短的連線/回應 timeout**（如 5~50ms，遠小於 TTL），避免卡在掛掉的節點上。
3. 當客戶端在**過半數節點**搶到、且**總耗時 < TTL** 時，才算獲取成功。
4. 鎖的**有效時間 = 原始 TTL − 獲取所花的總時間**（因為時鐘在流逝，第一個節點的鎖已經開始倒數）。
5. 若**沒**搶到過半、或有效時間 ≤ 0，就對**所有**節點發送釋放（Lua 驗 token 的 DEL），然後可重試。

**釋放**：對所有 N 個節點都執行驗 token 的釋放腳本（即使當初沒搶到的節點也發，保險）。

**為什麼過半數就安全？** 因為兩把「都拿到過半」的鎖必然在至少一個節點上重疊，而單節點的 NX 保證同一 key 同時只有一個持有者 → 矛盾 → 不可能兩個客戶端同時各拿過半。N 個獨立節點也意味著「單一節點故障轉移丟鎖」不再致命（其他多數節點仍記得這把鎖）。

#### Kleppmann vs antirez 爭議（必知）

Martin Kleppmann（《DDIA》作者）2016 年發表〈How to do distributed locking〉批評 Redlock：

- **時鐘依賴**：Redlock 的正確性依賴各節點**時鐘以差不多速率前進**。若某節點時鐘跳變（NTP 校正、VM 遷移、管理員手動改時間），該節點上的 key 可能**提早過期**，導致過半數安全性被打破。分散式演算法不該依賴「時鐘同步」這種同步假設。
- **行程暫停 / 網路延遲**：GC 長暫停、頁面換出等會讓客戶端在「檢查鎖」與「使用資源」之間插入任意長的停頓，鎖早已過期而客戶端不自知。**任何**基於 TTL 到期的鎖（不只 Redlock）都擋不住這種情況——唯一的正解是 **fencing token**，而 Redlock 沒有提供單調遞增的 token。
- 結論：Redlock 既非「效率型」鎖（那用單節點就好，簡單），也不夠格當「正確性型」鎖（那該用有共識演算法、能給 fencing token 的系統，如 ZooKeeper）。

antirez 回應〈Is Redlock safe?〉：

- 時鐘跳變在實務上可透過運維（禁用大幅度 NTP step、用 slew 平滑校正）規避；把「時鐘完全不可信」當前提過於苛刻。
- fencing token 的單調性其實可以由 Redlock 搭配（例如各節點回傳的資訊組合），且很多需要 fencing 的場景，其下游本來就有辦法做唯一性檢查。
- Redlock 對「行程暫停」的處理和其他 TTL 鎖一樣——這是所有自動過期鎖的共性，非 Redlock 獨有缺陷。

**中立結論**：這場爭論的真正教訓是——**如果你的正確性「絕對不能」出錯（金流、庫存扣減到不能超賣一件），不要把最終的一致性責任交給任何 TTL 型分散式鎖，要靠資源端本身的原子性 / fencing / 交易**。Redis 鎖（含 Redlock）非常適合「效率型」互斥（避免重複做昂貴工作、降低衝突），不適合當「安全性唯一防線」。

### 5.7 何時該用 / 不該用

| 場景 | 建議 |
|------|------|
| 避免多個 worker 重複做同一件昂貴的事（去重、限流、定時任務單例） | **單節點鎖 + watchdog** 足夠，偶爾失效只是多做一次，可接受 |
| 一般業務互斥，允許極小機率重入但有**冪等兜底** | 單節點鎖 + 業務層冪等 |
| 跨多資料中心、要抵抗單節點故障轉移的互斥 | 可考慮 **Redlock**（N 獨立節點），但要理解其時鐘假設 |
| **金流、扣款、庫存絕不可超賣** | **不要只靠 Redis 鎖**。用 **DB 交易 + 唯一約束 / SELECT FOR UPDATE / 樂觀鎖版本號**，或 **fencing token 讓下游拒絕過期寫入**。Redis 鎖至多當第一道效率門檻 |

### 5.8 用 go-redsync/redsync 的範例

`redsync` 是 Redlock 演算法的 Go 實作，可用一個或多個 `redis.Client` 當 pool。它處理了 token、過半數獲取、驗 token 釋放，並支援自動延長（`ExtendContext`）。

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	goredislib "github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	// 生產環境 Redlock 應給多個「獨立」節點；這裡示範單一 pool（等同單節點鎖）。
	client := goredislib.NewClient(&goredislib.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	// 建立一把鎖：TTL 8s，最多重試 3 次。
	mutex := rs.NewMutex("lock:job:report",
		redsync.WithExpiry(8*time.Second),
		redsync.WithTries(3),
	)

	// 獲取。
	if err := mutex.LockContext(ctx); err != nil {
		log.Fatalf("acquire lock: %v", err)
	}
	fmt.Println("locked, doing work...")

	// 若業務可能超過 TTL，手動延長（redsync 的 watchdog 等價操作）。
	go func() {
		t := time.NewTicker(3 * time.Second) // 約 TTL/3
		defer t.Stop()
		for range t.C {
			ok, err := mutex.ExtendContext(ctx) // 只有仍持有時才延長成功
			if err != nil || !ok {
				return
			}
		}
	}()

	time.Sleep(5 * time.Second) // 模擬業務

	// 釋放：redsync 內部用驗 token 的 Lua 刪除。
	ok, err := mutex.UnlockContext(ctx)
	if err != nil || !ok {
		log.Fatalf("release lock failed: ok=%v err=%v", ok, err)
	}
	fmt.Println("unlocked")
}
```

### 5.9 完整可跑範例：自製 Lock（TryAcquire / Release / watchdog）+ 100 goroutine 搶鎖驗互斥

這是本節的壓軸：手刻一把帶 watchdog 的單節點鎖，並用 100 個 goroutine 搶同一把鎖、對一個計數器做「非原子的讀-加-寫」，若互斥正確，最終計數必然精確等於次數；若鎖失效，計數會小於預期。

```go
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ---- Lock ----

// 驗 token 才刪除（釋放）。
var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end`)

// 驗 token 才續期（watchdog）。
var renewScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
    return 0
end`)

type Lock struct {
	rdb   *redis.Client
	key   string
	token string        // 本次持有的唯一識別，釋放/續期都靠它
	ttl   time.Duration
	stop  chan struct{} // 通知 watchdog 停止
	once  sync.Once
}

func newToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// TryAcquire 嘗試搶鎖一次；搶到回 (lock, true)，沒搶到回 (nil, false)。
func TryAcquire(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration) (*Lock, bool, error) {
	token := newToken()
	// SET key token NX PX ttl —— 單一原子命令，互斥核心。
	ok, err := rdb.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil // 鎖被別人持有
	}
	l := &Lock{rdb: rdb, key: key, token: token, ttl: ttl, stop: make(chan struct{})}
	l.startWatchdog()
	return l, true, nil
}

// Acquire 帶自旋重試地搶鎖，直到成功或 ctx 取消。
func Acquire(ctx context.Context, rdb *redis.Client, key string, ttl, retry time.Duration) (*Lock, error) {
	for {
		l, ok, err := TryAcquire(ctx, rdb, key, ttl)
		if err != nil {
			return nil, err
		}
		if ok {
			return l, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retry): // 稍等再搶，避免忙等
		}
	}
}

// startWatchdog 每 ttl/3 續期一次，直到 Release 或續期失敗。
func (l *Lock) startWatchdog() {
	go func() {
		t := time.NewTicker(l.ttl / 3)
		defer t.Stop()
		ctx := context.Background()
		for {
			select {
			case <-l.stop:
				return
			case <-t.C:
				ms := int64(l.ttl / time.Millisecond)
				res, err := renewScript.Run(ctx, l.rdb, []string{l.key}, l.token, ms).Int64()
				if err != nil || res == 0 {
					// 續期失敗（鎖已丟失或已被他人持有）→ 停止 watchdog。
					return
				}
			}
		}
	}()
}

// Release 停止 watchdog 並用驗 token 的 Lua 釋放鎖。
func (l *Lock) Release(ctx context.Context) error {
	l.once.Do(func() { close(l.stop) }) // 冪等地停 watchdog
	res, err := releaseScript.Run(ctx, l.rdb, []string{l.key}, l.token).Int64()
	if err != nil {
		return err
	}
	if res == 0 {
		return errors.New("鎖已非本人持有（可能已過期被他人取得）")
	}
	return nil
}

// ---- 驗互斥的 main ----

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	const lockKey = "lock:counter"
	const counterKey = "app:counter"
	rdb.Del(ctx, lockKey, counterKey)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			// 每個 goroutine 搶鎖 → 做「非原子」讀改寫 → 釋放。
			lock, err := Acquire(ctx, rdb, lockKey, 3*time.Second, 20*time.Millisecond)
			if err != nil {
				log.Printf("acquire: %v", err)
				return
			}
			defer func() {
				if err := lock.Release(ctx); err != nil {
					log.Printf("release: %v", err)
				}
			}()

			// 臨界區：故意用「讀出→加一→寫回」這種非原子序列。
			// 只有鎖真正互斥，最終值才會精確等於 goroutines。
			cur, _ := rdb.Get(ctx, counterKey).Int()
			time.Sleep(time.Millisecond) // 放大競爭窗口
			rdb.Set(ctx, counterKey, cur+1, 0)
		}()
	}

	wg.Wait()
	final, _ := rdb.Get(ctx, counterKey).Int()
	fmt.Printf("final counter = %d (expect %d)\n", final, goroutines)
	if final == goroutines {
		fmt.Println("互斥成立 ✔")
	} else {
		fmt.Println("互斥被破壞（鎖有問題）！")
	}
}
```

> 這支程式若鎖正確，輸出必為 `final counter = 100`。你可以把 `Acquire` 換成「直接不加鎖的讀改寫」對照，會看到 final 明顯小於 100（丟失更新），直觀感受鎖的作用。

---

## 6. 選型建議

1. **大多數業務**：單節點 `SET NX PX` 鎖 + watchdog 續期 + **業務層冪等兜底**。鎖負責「大幅降低」重複執行機率，冪等負責「萬一鎖失效也不出錯」。這是性價比最高的組合。
2. **冪等怎麼做**：用唯一業務鍵（訂單號 + 操作類型）＋ DB 唯一約束 / `INSERT ... ON CONFLICT DO NOTHING` / 版本號樂觀鎖，讓「同一操作重複執行第二次」變成無害的 no-op。
3. **強一致（金流、庫存零超賣）**：**不要把互斥的最終責任交給 Redis**。用資料庫交易（`SELECT ... FOR UPDATE`、唯一約束、版本號 CAS）或 fencing token 讓下游拒絕過期寫入。Redis 鎖至多是「效率型第一道門」。
4. **需要抗單點故障轉移**：Redlock（多獨立節點）或 `redsync`；但要清楚它的時鐘假設與 fencing 侷限，別把它當萬靈丹。
5. **能用一條命令 / 一段 Lua 解決的，就別用鎖**。很多「需要鎖」的需求，其實是「需要一個原子操作」——`INCR`、`SET NX`、`HSETNX`、Lua CAS 往往就夠了，根本不必引入分散式鎖的複雜度。

決策樹（簡版）：

```
要互斥嗎？
├─ 能用單一原子命令/Lua 表達？ → 用它，不要鎖
└─ 真的需要跨操作互斥
   ├─ 出錯只是「多做一次」可接受？ → 單節點鎖 + watchdog
   └─ 出錯會造成資料錯誤（金流/庫存）？
      ├─ 下游能做唯一性/版本檢查？ → DB 交易 or fencing token（Redis 鎖選配）
      └─ 需抗節點故障轉移？ → Redlock/redsync + 仍搭配下游冪等
```

---

## 7. 踩坑清單彙整

| # | 坑 | 後果 | 正解 |
|---|----|------|------|
| 1 | 以為 Redis 事務會回滾 | 執行期出錯，前面的寫入不會撤銷 | 用 Lua 做條件邏輯；理解事務只保證「不被插隊」 |
| 2 | `MULTI` 裡讀值想拿去當後續參數 | 拿不到，命令只是入列 | 讀改寫改用 WATCH（應用層）或 Lua（伺服器端） |
| 3 | WATCH 失敗不重試 | 遺失更新 | 迴圈捕捉 `redis.TxFailedErr` 重讀重算 |
| 4 | 把 `Get` 放進 `TxPipelined` | 讀不到值、CAS 失效 | 讀放 Watch 的 fn 外層，寫才放 TxPipelined |
| 5 | Lua 腳本跑慢操作（`KEYS *`、大 range、長迴圈） | 阻塞整個實例、其他連線 `BUSY` | 腳本只做輕量原子邏輯；大掃描用 SCAN 在客戶端做 |
| 6 | 腳本內用隨機/時間又跑在舊版腳本複製 | master/replica 資料分歧 | Redis 5+ 預設效果複製；時間類參數從 ARGV 傳入 |
| 7 | 把 Pipeline 當事務用 | 中間被其他客戶端插隊，非原子 | 要原子用 TxPipeline / Lua |
| 8 | Pipeline 一次塞幾十萬條 | 緩衝區膨脹、被斷線、長尾延遲 | 每批 100~1000 條 |
| 9 | 鎖用 `SETNX` + 分開 `EXPIRE` 兩步 | 中間崩潰 → 無 TTL 死鎖 | 一律 `SET k token NX PX ttl` 單命令 |
| 10 | 鎖 value 不存 token | 釋放時刪到別人的鎖 | value 存唯一 token |
| 11 | 釋放用 `GET` 比對 + `DEL` 兩步 | 比對後鎖過期被搶，仍刪錯 | 驗 token + DEL 用 Lua 原子完成 |
| 12 | 只靠 TTL、不續期 | 業務超時掉鎖 → 多人同時持鎖 | watchdog 每 TTL/3 續期（且驗 token） |
| 13 | watchdog 續期不驗 token | 鎖已被他人搶走仍幫別人續命 | 續期腳本先 `GET==token` |
| 14 | 用 master-replica 單點鎖抗故障轉移 | 非同步複製 + 切換 → 丟鎖 | Redlock 多節點，或別靠 Redis 做強一致 |
| 15 | 拿 Redis 鎖當金流唯一防線 | 時鐘/暫停/切換任一出事就超賣 | DB 交易 + fencing token；Redis 只當效率門 |
| 16 | 忽視行程 GC 長暫停 | 凍結行程醒來後亂寫 | fencing token 讓下游拒絕過期序號 |

---

## 8. 練習題 + 檢查點 + 延伸閱讀

### 8.1 練習題

1. **事務語意**：寫一段 redis-cli 序列，證明「執行期錯誤那條回錯、其餘照跑」（提示：`SET x abc` 後在 `MULTI` 內 `INCR x` 與 `SET y ok`）。再寫一段證明「入列期錯誤（打錯命令）會讓 EXEC 整批放棄」。
2. **WATCH 重試**：把 5.4 的 `transfer` 改成兩個 goroutine 同時對同一帳戶轉出，觀察 `redis.TxFailedErr` 觸發與重試次數。把 `maxRetries` 設成 1，製造「重試耗盡」的失敗。
3. **Lua 原子性**：把第 3 節的 `incrByWithCap` 用「`GET` + 應用層判斷 + `SET`」的非原子版本重寫，用 50 個 goroutine 同時打，觀察是否會突破 cap（會）；再換回 Lua 版，驗證不會。
4. **Pipeline 效能**：分別用「逐條 `Set`」與「`Pipelined` 每批 500」寫入 10000 個 key，用 `time.Since` 比較耗時（跨機房差距更明顯）。
5. **watchdog 續命（重點）**：用 5.9 的 `Lock`，把 TTL 設 2s，臨界區 `time.Sleep(6*time.Second)`（模擬業務超時）。
   - (a) 先**關掉** watchdog（把 `startWatchdog` 註解掉），觀察第二個 goroutine 在鎖過期後搶到鎖 → 互斥被破壞、`Release` 回「非本人持有」。
   - (b) 再**開回** watchdog，觀察鎖被持續續期、6 秒內不掉鎖、互斥維持、`final counter` 正確。這一題最能體會 watchdog 的價值。
6. **釋放別人的鎖**：把 `releaseScript` 換成裸 `DEL`，重跑 (5) 的 (a) 情境，觀察 C1 直接刪掉 C2 的鎖造成的連鎖破壞。
7. **fencing 思考題**：設計一個「訂單狀態機更新」的下游檢查：DB 表存 `last_fence` 欄位，更新時 `WHERE last_fence < :token`，用一段 pseudo-SQL 說明它如何擋掉凍結後醒來的舊持有者。

### 8.2 檢查點（能答出就算過關）

- [ ] 能說清楚 Redis 事務保證什麼、不保證什麼；兩類錯誤（入列期 vs 執行期）行為差異。
- [ ] 能用 go-redis 的 `Watch` + `TxPipelined` 寫出正確、會重試的 CAS。
- [ ] 知道 `redis.NewScript` 如何自動 EVALSHA / NOSCRIPT fallback，以及為何 KEYS 要放 key。
- [ ] 能講出 Pipeline 與事務的核心差異，並說明「Pipeline 非原子」。
- [ ] 能默寫正確的單節點鎖：`SET k token NX PX ttl` + 驗 token 的 Lua 釋放。
- [ ] 能逐一講出三大坑（釋放別人的鎖 / 業務超 TTL / 主從切換丟鎖）的時序與解法。
- [ ] 能解釋 watchdog、fencing token、Redlock 各自解決什麼、代價/侷限是什麼。
- [ ] 能判斷一個需求該用哪種原語（含「金流別只靠 Redis 鎖」）。

### 8.3 延伸閱讀

- Redis 官方文件：Transactions（<https://redis.io/docs/latest/develop/interact/transactions/>）
- Redis 官方文件：Scripting with Lua / EVAL / functions（<https://redis.io/docs/latest/develop/interact/programmability/eval-intro/>）
- Redis 官方文件：Pipelining（<https://redis.io/docs/latest/develop/use/pipelining/>）
- Redis 官方文件：Distributed Locks with Redis / Redlock（<https://redis.io/docs/latest/develop/use/patterns/distributed-locks/>）
- antirez，"Is Redlock safe?"（Redlock 原文與對批評的回應：<http://antirez.com/news/101>）
- Martin Kleppmann，"How to do distributed locking"（對 Redlock 的反駁、fencing token：<https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html>）
- go-redis 文件：Lua scripting / Pipelines and transactions（<https://redis.uptrace.dev/guide/>）
- `go-redsync/redsync` 原始碼與 README（Redlock 的 Go 實作）
- 《Designing Data-Intensive Applications》第 8、9 章（分散式系統的時間、鎖與共識，Kleppmann 著）
