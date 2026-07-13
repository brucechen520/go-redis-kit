// Lab 01 — String 計數器
//
// 示範 String 當計數器的四個核心手法：
//  1. 併發 INCR   — 證明原子性（100 goroutine 同時 +1 不少算）
//  2. GETDEL 歸檔 — 原子取值並歸零，避免「讀後清空」競態
//  3. Lua 限流    — INCR + EXPIRE 綁成原子（INCR 沒有內建 EX）
//  4. 分片計數器  — 熱 key 打散到多個 shard，讀時加總
//
// 執行前先起 redis：make single-up
// 執行：           go run ./labs/01-string-counter
//
//	位址／密碼可用環境變數覆寫：REDIS_ADDR、REDIS_PASSWORD
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	// NewClient 不會馬上連線，只準備連線池設定；真正 TCP 連線在第一次執行指令時才建立。
	rdb := redis.NewClient(&redis.Options{
		Addr:     env("REDIS_ADDR", "127.0.0.1:6379"),
		Password: env("REDIS_PASSWORD", "devpass_change_me"), // 沒設密碼就留空
		DB:       0,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("連不上 redis：%v\n(先跑 make single-up；密碼用 REDIS_PASSWORD 覆寫)", err)
	}

	// 1. String
	demoConcurrentINCR(ctx, rdb)
	demoGetDelArchive(ctx, rdb)
	demoRateLimit(ctx, rdb)
	demoShardedCounter(ctx, rdb)

	// 2. Hash
	demoHash(ctx, rdb)
	demoHGetAllVsHscan(ctx, rdb)

	// 3. List
	demoListRecentN(ctx, rdb)
	demoListReliableQueue(ctx, rdb)

	// 4. Set
	demoSetTags(ctx, rdb)
	demoSetSocial(ctx, rdb)
	demoSetLottery(ctx, rdb)

	// 5. Rate Limit 三算法
	demoRLFixedWindow(ctx, rdb)
	demoRLSlidingWindow(ctx, rdb)
	demoRLTokenBucket(ctx, rdb)

	// 6. ZSet
	demoZSetLeaderboard(ctx, rdb)
	demoZSetDelayQueue(ctx, rdb)

	// 7. 分散式鎖：fencing token
	demoFencingToken(ctx, rdb)

	// 8. Stream：可靠訊息佇列
	demoStream(ctx, rdb)

	// 9. 位圖 / 機率結構 / 地理空間
	demoBitmap(ctx, rdb)
	demoHLL(ctx, rdb)
	demoGeo(ctx, rdb)
	demoBitfield(ctx, rdb)
}

// 1. 併發 INCR 證明原子性 ---------------------------------------------------
// 100 個 goroutine 各 +1 共 100 次 = 期望 10000。INCR 原子，不用鎖也不會少算。
func demoConcurrentINCR(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 1. 併發 INCR（原子性）===")
	const key = "lab:counter"
	const goroutines, perG = 100, 100

	rdb.Del(ctx, key)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				rdb.Incr(ctx, key) // 原子 +1
			}
		}()
	}
	wg.Wait()

	got, _ := rdb.Get(ctx, key).Int()
	fmt.Printf("期望 %d，實際 %d", goroutines*perG, got)
	if got == goroutines*perG {
		fmt.Println("  ✅ 一次不少（原子）")
	} else {
		fmt.Println("  ❌ 少算了（不該發生）")
	}
	enc, _ := rdb.Do(ctx, "OBJECT", "ENCODING", key).Text()
	fmt.Printf("OBJECT ENCODING = %s（整數走 int 編碼）\n", enc)
}

// 2. GETDEL 歸檔：原子取值並歸零 --------------------------------------------
// 每小時排程用 GETDEL 取出這段時間的增量並歸零，寫回 total。
// GETDEL 原子：取值當下就刪除，中間新增的 INCR 會進「下一桶」，不漏算。
func demoGetDelArchive(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 2. GETDEL 歸檔（避免讀後清空競態）===")
	const cur, total = "lab:pv:current", "lab:pv:total"
	rdb.Del(ctx, cur, total)

	// 模擬這一小時來了 250 次瀏覽
	for i := 0; i < 250; i++ {
		rdb.Incr(ctx, cur)
	}

	// 歸檔：原子取出增量並歸零
	delta, _ := rdb.GetDel(ctx, cur).Int()
	rdb.IncrBy(ctx, total, int64(delta)) // 累加到總數（或寫 DB）
	fmt.Printf("這桶增量 delta = %d，已累加到 total\n", delta)

	// 歸檔後 current 不存在，新的瀏覽從 0 重新累積（進下一桶）
	rdb.Incr(ctx, cur)
	cur2, _ := rdb.Get(ctx, cur).Int()
	tot, _ := rdb.Get(ctx, total).Int()
	fmt.Printf("歸檔後新瀏覽 current = %d（從 0 重算），total = %d\n", cur2, tot)
	fmt.Println("為何用 GETDEL 而非 GET+DEL：兩步之間的 INCR 會在 DEL 時被吃掉 → 漏算")
}

// 3. Lua 限流：INCR + EXPIRE 原子綁定 ---------------------------------------
// INCR 沒有內建 EX，「先 INCR 再 EXPIRE」兩步崩潰會留永久 key。用 Lua 綁成原子。
var rateScript = redis.NewScript(`
local c = redis.call('INCR', KEYS[1])
if c == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return c`)

func demoRateLimit(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 3. Lua 限流（固定窗，每窗上限 5）===")
	const key = "lab:rate:uid:9"
	const window, limit = 3, 5 // 3 秒窗、上限 5 次
	rdb.Del(ctx, key)

	for i := 1; i <= 8; i++ {
		c, err := rateScript.Run(ctx, rdb, []string{key}, window).Int()
		if err != nil {
			log.Fatal(err)
		}
		status := "允許"
		if c > limit {
			status = "擋下"
		}
		fmt.Printf("第 %d 次：count=%d → %s\n", i, c, status)
	}
	ttl, _ := rdb.TTL(ctx, key).Result()
	fmt.Printf("key 已自動帶 TTL = %v（第一次就在 Lua 內原子設好）\n", ttl)
}

// 4. 分片計數器：熱 key 打散 ------------------------------------------------
// 全站總 PV 這種超熱計數，單一 key 會把某 shard CPU 打滿。
// 寫入打散到 N 個 shard key，讀取時 MGET 加總（犧牲即時精確讀換寫入分散）。
func demoShardedCounter(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 4. 分片計數器（熱 key 打散）===")
	const shards = 10
	base := "lab:pv:total:shard:"

	keys := make([]string, shards)
	for i := 0; i < shards; i++ {
		keys[i] = fmt.Sprintf("%s%d", base, i)
	}
	rdb.Del(ctx, keys...)

	// 寫入：輪流打散到不同 shard（示範用；實務可隨機或 hash uid）
	for i := 0; i < 1000; i++ {
		rdb.Incr(ctx, keys[i%shards])
	}

	// 讀取：一次 MGET 全部 shard 再加總
	vals, _ := rdb.MGet(ctx, keys...).Result()
	var sum int64
	for _, v := range vals {
		if s, ok := v.(string); ok {
			var n int64
			n, _ = strconv.ParseInt(s, 10, 64)
			sum += n
		}
	}
	fmt.Printf("寫入 1000 次，分散到 %d 個 shard，加總 = %d\n", shards, sum)
	fmt.Printf("每 shard 約 %d，寫入壓力分散到 %d 個 key\n", sum/shards, shards)
}

func demoHash(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 5. hash（多欄位、局部更新、欄位級 TTL）===")

	// 情境 1：物件欄位儲存（user profile）— 固定 schema，欄位有界
	rdb.HSet(ctx, "user:1001", "name", "Alice", "age", 30, "city", "Taipei")
	rdb.HIncrBy(ctx, "user:1001", "age", 1)                // 只更新單一欄位
	rdb.HExpire(ctx, "user:1001", 60*time.Second, "email") // 欄位級 TTL（7.4+）

	// 情境 2：購物車 — 分出貨方式各一個 hash，field=商品 id、value=數量
	rdb.HIncrBy(ctx, "cart:uid9:24h", "prod123", 2) // 加 2 件
	rdb.HIncrBy(ctx, "cart:uid9:vendor", "prod789", 1)

	// 情境 3：計數聚合 — 一個 key 多子計數，原子加、一次讀整組
	rdb.HIncrBy(ctx, "order:stats:2026-07", "paid", 1)
	stats, _ := rdb.HGetAll(ctx, "order:stats:2026-07").Result() // 固定維度，安全
	_ = stats

	// 情境 4：配置字典 / feature flag
	rdb.HSet(ctx, "config:features", "new_checkout", "on", "dark_mode", "off")
	flag, _ := rdb.HGet(ctx, "config:features", "new_checkout").Result()
	_ = flag
}

// HGETALL vs HSCAN 效能差異 --------------------------------------------------
// 灌 10 萬 field 大 hash，用 SLOWLOG 量「server 端純執行時間」（不含 client
// 網路/渲染噪音）。HGETALL = 一次全撈 → server 被獨佔「一整段」；HSCAN =
// 游標分批 → 切成很多「小段」，批與批之間讓其他 client 插進來。
// 重點：HSCAN 總 CPU 反而更多（多次呼叫游標開銷），但單段阻塞很短，不獨佔 server。
func demoHGetAllVsHscan(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 6. HGETALL vs HSCAN（大 hash，SLOWLOG server 阻塞時間）===")
	const key = "lab:bighash"
	const nFields = 100000

	// 灌大 hash（pipeline 加速）
	rdb.Del(ctx, key)
	pipe := rdb.Pipeline()
	for i := 0; i < nFields; i++ {
		pipe.HSet(ctx, key, fmt.Sprintf("f%d", i), i)
		if i%10000 == 9999 {
			if _, err := pipe.Exec(ctx); err != nil {
				log.Fatal(err)
			}
			pipe = rdb.Pipeline()
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Fatal(err)
	}
	hlen, _ := rdb.HLen(ctx, key).Result()
	enc, _ := rdb.Do(ctx, "OBJECT", "ENCODING", key).Text()
	fmt.Printf("HLEN=%d ENCODING=%s\n", hlen, enc)

	warmPool(ctx, rdb) // 先建好連線，避免 AUTH 混進 slowlog
	// 全記 slowlog（門檻 0），量 server 純執行時間
	rdb.ConfigSet(ctx, "slowlog-max-len", "2000")
	rdb.ConfigSet(ctx, "slowlog-log-slower-than", "0")
	defer rdb.ConfigSet(ctx, "slowlog-log-slower-than", "10000")

	// HGETALL：一次全撈
	rdb.Do(ctx, "SLOWLOG", "RESET")
	if _, err := rdb.HGetAll(ctx, key).Result(); err != nil {
		log.Fatal(err)
	}
	_, hgetallMax, _ := statSlowlog(ctx, rdb, "hgetall")

	// HSCAN：游標分批全掃
	rdb.Do(ctx, "SLOWLOG", "RESET")
	var cursor uint64
	batches := 0
	for {
		_, next, err := rdb.HScan(ctx, key, cursor, "*", 1000).Result()
		if err != nil {
			log.Fatal(err)
		}
		batches++
		cursor = next
		if cursor == 0 {
			break
		}
	}
	hscanTotal, hscanMax, hscanCnt := statSlowlog(ctx, rdb, "hscan")
	avg := hscanTotal / int64(max(hscanCnt, 1))

	fmt.Printf("HGETALL：一次全撈，server 阻塞 %d µs ← 這段期間所有 client 都得等\n", hgetallMax)
	fmt.Printf("HSCAN  ：%d 批，server 加總 %d µs、單批最大 %d µs、平均 %d µs/批\n",
		batches, hscanTotal, hscanMax, avg)
	fmt.Printf("→ 注意 HSCAN 總 CPU 反而「更多」（%d vs %d µs，多次呼叫有游標開銷）。\n", hscanTotal, hgetallMax)
	fmt.Printf("  但關鍵是「單段阻塞」：HGETALL 一次卡 %d µs，HSCAN 最多才 %d µs/段（平均 %d µs）——\n",
		hgetallMax, hscanMax, avg)
	fmt.Printf("  批間讓其他 client 插進來，別人最多只等一小段，server 不被獨佔。這才是 HSCAN 的價值。\n")

	rdb.Del(ctx, key)
}

// statSlowlog 掃最近 slowlog，回傳指定指令的（耗時加總 µs, 單筆最大 µs, 筆數）。
func statSlowlog(ctx context.Context, rdb *redis.Client, cmd string) (int64, int64, int) {
	logs, err := rdb.SlowLogGet(ctx, 3000).Result()
	if err != nil {
		log.Fatal(err)
	}
	var total, maxOne int64
	var cnt int
	for _, l := range logs {
		if len(l.Args) > 0 && strings.EqualFold(l.Args[0], cmd) {
			us := l.Duration.Microseconds()
			total += us
			if us > maxOne {
				maxOne = us
			}
			cnt++
		}
	}
	return total, maxOne, cnt
}

// warmPool 預熱連線池，避免量測含建立連線的時間。
func warmPool(ctx context.Context, rdb *redis.Client) {
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); rdb.Ping(ctx) }()
	}
	wg.Wait()
}

// 7. List：LPUSH + LTRIM 保留「最新 N 筆」------------------------------------
// 情境：使用者最近瀏覽紀錄、最近搜尋、最新 N 則通知。推到頭 + 砍到剩 N，
// list 永遠是最新 N 筆、有界、自動淘汰舊的。
func demoListRecentN(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 7. List：LPUSH+LTRIM 保留最新 N 筆（最近瀏覽）===")
	const key = "recent:views:uid9"
	const keepN = 5

	rdb.Del(ctx, key)
	pages := []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"}
	for _, p := range pages {
		rdb.LPush(ctx, key, p)          // 推到頭：最新永遠在 index 0
		rdb.LTrim(ctx, key, 0, keepN-1) // 只留最新 N 筆，其餘丟掉
	}

	list, _ := rdb.LRange(ctx, key, 0, -1).Result()
	fmt.Printf("瀏覽 %d 頁後，只留最新 %d 筆：%v\n", len(pages), keepN, list)
	fmt.Println("（最新在最前；舊的 p1/p2/p3 已被 LTRIM 淘汰，記憶體有界）")
}

// 8. List：BLMOVE 可靠佇列（崩潰可回收）--------------------------------------
// 情境：背景任務佇列要「不掉訊息」。BRPOP 一 pop 就消失，crash 即掉；
// BLMOVE 原子搬到 processing 清單（備份），處理完 LREM(=ack)，
// crash 卡在 processing 可被回收程序搬回重試。要多消費組+重放請用 Stream。
func demoListReliableQueue(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 8. List：BLMOVE 可靠佇列（崩潰可回收 vs BRPOP 掉訊息）===")
	const queue, processing = "jobs:queue", "jobs:processing"
	rdb.Del(ctx, queue, processing)

	// 生產：RPUSH 進尾、消費從頭取 = FIFO
	rdb.RPush(ctx, queue, "job-1", "job-2", "job-3")

	// 取一個：BLMOVE 原子搬到 processing（不是刪掉）
	job, _ := rdb.BLMove(ctx, queue, processing, "LEFT", "RIGHT", time.Second).Result()
	fmt.Printf("取出 %s → 此刻在 processing（備份）：%v\n", job, lrange(ctx, rdb, processing))

	// 處理成功 → LREM 移除（= ack）
	rdb.LRem(ctx, processing, 1, job)
	fmt.Printf("處理成功，LREM(=ack) → processing：%v\n", lrange(ctx, rdb, processing))

	// 模擬崩潰：再取一個但「不 ack」
	job2, _ := rdb.BLMove(ctx, queue, processing, "LEFT", "RIGHT", time.Second).Result()
	fmt.Printf("取出 %s 後 consumer 崩潰（沒 ack）→ 卡在 processing：%v\n", job2, lrange(ctx, rdb, processing))

	// 回收程序：把 processing 卡住的搬回 queue 重試
	back, _ := rdb.LMove(ctx, processing, queue, "LEFT", "RIGHT").Result()
	fmt.Printf("回收把 %s 搬回 queue 重試 → queue：%v；processing：%v\n",
		back, lrange(ctx, rdb, queue), lrange(ctx, rdb, processing))
	fmt.Println("→ 崩潰任務沒遺失（對比 BRPOP 一 pop 就消失、crash 即掉）")

	rdb.Del(ctx, queue, processing)
}

func lrange(ctx context.Context, rdb *redis.Client, key string) []string {
	v, _ := rdb.LRange(ctx, key, 0, -1).Result()
	return v
}

// 9. Set：標籤 / 去重 / 成員檢查 ---------------------------------------------
// 情境：文章標籤、用戶興趣、黑白名單。SADD 天生去重；SISMEMBER O(1) 查在不在
//（權限檢查、IP 黑名單、去重都靠這個）。
func demoSetTags(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 9. Set：標籤 / 去重 / 成員檢查 ===")
	const key = "article:42:tags"
	rdb.Del(ctx, key)

	// SADD 去重：加 5 次（含重複）只會留不重複的
	added, _ := rdb.SAdd(ctx, key, "go", "redis", "go", "db", "redis").Result()
	fmt.Printf("加 5 次(含重複)，實際新增 %d 個 → %v（自動去重）\n", added, smembers(ctx, rdb, key))

	// SISMEMBER：O(1) 查在不在
	yes, _ := rdb.SIsMember(ctx, key, "go").Result()
	no, _ := rdb.SIsMember(ctx, key, "rust").Result()
	fmt.Printf("有 go? %v；有 rust? %v\n", yes, no)

	// SCARD 數量、SREM 移除
	cnt, _ := rdb.SCard(ctx, key).Result()
	rdb.SRem(ctx, key, "db")
	fmt.Printf("標籤數=%d，移除 db 後=%v\n", cnt, smembers(ctx, rdb, key))
}

// 10. Set：共同好友 / 好友推薦（交集、差集）----------------------------------
// 情境：社群「共同好友」= 兩人好友集合的交集（SINTER）；
//「可能認識的人」= 差集（SDIFF，他有你沒有）。
func demoSetSocial(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 10. Set：共同好友 SINTER / 好友推薦 SDIFF ===")
	const alice, bob = "friends:alice", "friends:bob"
	rdb.Del(ctx, alice, bob)
	rdb.SAdd(ctx, alice, "u1", "u2", "u3", "u4")
	rdb.SAdd(ctx, bob, "u3", "u4", "u5", "u6")

	common, _ := rdb.SInter(ctx, alice, bob).Result() // 交集 = 共同好友
	fmt.Printf("alice ∩ bob 共同好友 = %v\n", common)

	recommend, _ := rdb.SDiff(ctx, bob, alice).Result() // bob 有、alice 沒有 → 推薦
	fmt.Printf("推薦給 alice（bob 有她沒有）= %v\n", recommend)
	fmt.Println("（⚠️ 大 set 的 SINTER/SDIFF 是 O(N)，熱點要小心或改離線算）")
}

// 11. Set：抽獎（SPOP 不重複中獎 / SRANDMEMBER 抽樣不移除）--------------------
// 情境：抽獎池。SPOP 隨機取出「並移除」→ 同一人不會中兩次；
// SRANDMEMBER 隨機取但「不移除」→ 適合抽樣/展示不消耗池子。
func demoSetLottery(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 11. Set：抽獎 SPOP（不重複中獎）/ SRANDMEMBER（抽樣不移除）===")
	const pool = "lottery:pool"
	rdb.Del(ctx, pool)
	for i := 1; i <= 10; i++ {
		rdb.SAdd(ctx, pool, fmt.Sprintf("user%d", i))
	}

	preview, _ := rdb.SRandMemberN(ctx, pool, 3).Result() // 不移除
	fmt.Printf("SRANDMEMBER 預覽 3 個（不移除）= %v，池子還剩 %d\n", preview, scard(ctx, rdb, pool))

	winners, _ := rdb.SPopN(ctx, pool, 2).Result() // 移除 → 不重複中獎
	fmt.Printf("SPOP 抽 2 位中獎（移除）= %v，池子剩 %d\n", winners, scard(ctx, rdb, pool))
	fmt.Println("→ SPOP 中獎即移除，同一人不會中兩次；SRANDMEMBER 只抽樣不動池子")

	rdb.Del(ctx, pool)
}

func smembers(ctx context.Context, rdb *redis.Client, key string) []string {
	v, _ := rdb.SMembers(ctx, key).Result()
	return v
}

func scard(ctx context.Context, rdb *redis.Client, key string) int64 {
	n, _ := rdb.SCard(ctx, key).Result()
	return n
}

// ── 限流三算法（固定窗 / 滑動窗 / 令牌桶）────────────────────────────────────
// 三種都「8 次請求、上限 5」→ 前 5 放行、後 3 擋下。差別在時間拉長後的行為
// （固定窗整窗重置、滑動窗逐筆滑出、令牌桶按 rate 勻速補回）。

// 12. 固定窗口：INCR + 首次 EXPIRE，超過上限就擋（有邊界雙倍突發問題）
var rlFixedScript = redis.NewScript(`
local c = redis.call('INCR', KEYS[1])
if c == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
return c`)

func demoRLFixedWindow(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 12. 限流：固定窗口（每窗上限 5）===")
	const key, window, limit = "rl:fixed:u1", 60, 5
	rdb.Del(ctx, key)
	for i := 1; i <= 8; i++ {
		c, _ := rlFixedScript.Run(ctx, rdb, []string{key}, window).Int()
		fmt.Printf("第 %d 次：count=%d → %s\n", i, c, allowStr(c <= limit))
	}
}

// 13. 滑動窗口（ZSet）：清窗外 → 數當前 → 未滿才加入。member 唯一（now_ms:i）
var rlSlidingScript = redis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1]-ARGV[2])  -- 清掉 window 之前的
local n = redis.call('ZCARD', KEYS[1])
if n < tonumber(ARGV[3]) then
  redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0`)

func demoRLSlidingWindow(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 13. 限流：滑動窗口 ZSet（60s 內上限 5）===")
	const key = "rl:slide:u1"
	const windowMs, limit = 60000, 5
	rdb.Del(ctx, key)
	for i := 1; i <= 8; i++ {
		now := time.Now().UnixMilli()
		member := fmt.Sprintf("%d:%d", now, i) // 唯一 member，避免同毫秒覆蓋
		ok, _ := rlSlidingScript.Run(ctx, rdb, []string{key}, now, windowMs, limit, member).Int()
		fmt.Printf("第 %d 次 → %s\n", i, allowStr(ok == 1))
	}
}

// 14. 令牌桶：惰性補充（容量 5、每秒補 1），先吃掉攢的突發、之後回歸勻速
var rlTokenScript = redis.NewScript(`
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1]) or capacity
local ts     = tonumber(b[2]) or now
tokens = math.min(capacity, tokens + (now - ts) / 1000 * rate)  -- 依經過時間補充
local allowed = 0
if tokens >= 1 then tokens = tokens - 1; allowed = 1 end
redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000))
return allowed`)

func demoRLTokenBucket(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 14. 限流：令牌桶（容量 5、每秒補 1）===")
	const key, capacity, rate = "rl:tb:u1", 5, 1
	rdb.Del(ctx, key)
	for i := 1; i <= 8; i++ {
		now := time.Now().UnixMilli()
		ok, _ := rlTokenScript.Run(ctx, rdb, []string{key}, capacity, rate, now).Int()
		fmt.Printf("第 %d 次 → %s\n", i, allowStr(ok == 1))
	}
}

func allowStr(ok bool) string {
	if ok {
		return "允許"
	}
	return "擋下"
}

// 15. ZSet：排行榜（ZADD/ZINCRBY/ZREVRANGE/ZREVRANK）--------------------------
// 情境：遊戲排行榜、熱門排序。member=玩家、score=分數，按 score 排序。
func demoZSetLeaderboard(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 15. ZSet：排行榜 ===")
	const key = "game:leaderboard"
	rdb.Del(ctx, key)
	rdb.ZAdd(ctx, key,
		redis.Z{Score: 1500, Member: "alice"},
		redis.Z{Score: 2300, Member: "bob"},
		redis.Z{Score: 1800, Member: "carol"},
		redis.Z{Score: 900, Member: "dave"},
	)
	rdb.ZIncrBy(ctx, key, 500, "alice") // alice 加 500 → 2000

	// Top 3（分數高到低，帶分數）
	top, _ := rdb.ZRevRangeWithScores(ctx, key, 0, 2).Result()
	fmt.Println("Top 3：")
	for i, z := range top {
		fmt.Printf("  #%d %s = %.0f\n", i+1, z.Member, z.Score)
	}

	// 查名次（ZRevRank 由高到低，0-based）+ 分數
	rank, _ := rdb.ZRevRank(ctx, key, "alice").Result()
	score, _ := rdb.ZScore(ctx, key, "alice").Result()
	fmt.Printf("alice 第 %d 名（分數 %.0f）\n", rank+1, score)

	// 分頁：第 2 頁、每頁 2 筆 = index 2~3
	page2, _ := rdb.ZRevRange(ctx, key, 2, 3).Result()
	fmt.Printf("第 2 頁（每頁 2 筆）= %v\n", page2)
}

// 16. ZSet：延遲隊列（score=執行時戳，撈到期任務）----------------------------
// 情境：延遲任務、定時重試、訂單超時關閉。score=該執行的 unix 秒，
// 週期性用 ZRANGEBYSCORE 0..now 撈到期的處理。
func demoZSetDelayQueue(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 16. ZSet：延遲隊列（撈到期任務）===")
	const key = "delay:queue"
	rdb.Del(ctx, key)
	now := time.Now()
	rdb.ZAdd(ctx, key,
		redis.Z{Score: float64(now.Add(-10 * time.Second).Unix()), Member: "job-past-1"},
		redis.Z{Score: float64(now.Add(-2 * time.Second).Unix()), Member: "job-past-2"},
		redis.Z{Score: float64(now.Add(30 * time.Second).Unix()), Member: "job-future"},
	)

	// 撈到期：score <= now
	due, _ := rdb.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: "0", Max: fmt.Sprintf("%d", now.Unix()),
	}).Result()
	fmt.Printf("到期任務（該執行）= %v\n", due)

	// 執行完移除
	if len(due) > 0 {
		rdb.ZRem(ctx, key, due[0])
		rest, _ := rdb.ZRange(ctx, key, 0, -1).Result()
		fmt.Printf("執行並移除 %s；剩下 = %v\n", due[0], rest)
	}
	fmt.Println("（job-future 還沒到期不會被撈；多 worker 要用 Lua 原子撈+刪或 ZPOPMIN 防重複取）")
}

// 17. Fencing token：防「過期持鎖者」亂寫共享資源 -----------------------------
// 鎖過期後，舊持有者（GC 凍結後醒來）以為自己還持鎖、繼續寫 → 資料損毀。
// 解法：發鎖時給單調遞增序號（INCR），資源端只收「比看過的最大值更大」的 token，
// 舊 token 一律拒絕。擋住舊持有者的不是鎖，是資源端的序號檢查。
//
// 對照 docs/05 §5.5：fencing token = 分散式鎖的「版本號」，跟 token bucket（限流的
// 「額度桶」）完全無關，只是名字都有 token。
var fenceGuardScript = redis.NewScript(`
local seen = tonumber(redis.call('GET', KEYS[1]) or '0')
if tonumber(ARGV[1]) > seen then
  redis.call('SET', KEYS[1], ARGV[1])   -- 記住新的最大 token
  return 1                              -- 接受寫入
end
return 0                                -- 舊 token，拒絕
`)

func demoFencingToken(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 17. Fencing token（防過期持鎖者亂寫）===")
	const seqKey = "lock:sla:fence:seq" // 發號器（INCR 產生單調遞增 token）
	const guardKey = "resource:sla:max" // 資源端記錄「看過的最大 token」
	rdb.Del(ctx, seqKey, guardKey)

	// A 取得鎖 → 領一個 fencing token
	tokenA, _ := rdb.Incr(ctx, seqKey).Result()
	fmt.Printf("A 取得鎖，fencing token = %d\n", tokenA)

	// A 業務卡住（GC 長暫停 / STW）→ 鎖 TTL 過期（此處直接讓 B 也能拿鎖）
	fmt.Println("A 凍結中（GC pause）… 鎖 TTL 過期，watchdog 也一起被凍住")

	// B 取得鎖 → 領到更大的 token
	tokenB, _ := rdb.Incr(ctx, seqKey).Result()
	fmt.Printf("B 取得鎖，fencing token = %d\n", tokenB)

	// B 寫資源，帶新 token → 資源端記錄 max=B，接受
	okB, _ := fenceGuardScript.Run(ctx, rdb, []string{guardKey}, tokenB).Int()
	fmt.Printf("B 寫資源（token=%d）→ %s\n", tokenB, acceptStr(okB == 1))

	// A 醒來，仍以為自己持鎖，帶舊 token 寫資源 → 被資源端拒絕
	okA, _ := fenceGuardScript.Run(ctx, rdb, []string{guardKey}, tokenA).Int()
	fmt.Printf("A 醒來寫資源（token=%d，舊）→ %s\n", tokenA, acceptStr(okA == 1))

	fmt.Println("（關鍵：鎖擋不住已凍結的 A；擋住 A 的是資源端『只收更大 token』的檢查）")
	fmt.Println("（實務多用冪等 / DB 唯一約束替代 fencing——下游能擋重複就不用改寫序號）")
}

func acceptStr(ok bool) string {
	if ok {
		return "接受寫入"
	}
	return "拒絕（舊 token）"
}

// 18. Stream：訂單事件可靠處理管線 ------------------------------------------
// 真實情境：下單服務 XADD 訂單事件 → 計費(billing)、通知(notify) 兩個消費組
// 各自收到全部事件（fan-out）。處理成功才 XACK；未 ack 的留在 PEL，consumer
// 崩潰後由 XAUTOCLAIM 轉給健康的 worker 補做。業務用 order_id 去重（冪等）。
//
// 對照 docs/01 §5「List 佇列三級」：BRPOP 無 ack（崩潰掉訊息）< BLMOVE 可回收
// < Stream（ack/PEL/XCLAIM/consumer group 全原生內建）。要可靠又省事就用 Stream。
func demoStream(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 18. Stream：訂單事件可靠處理管線（ack + 崩潰回收 + fan-out）===")
	const stream, dedupKey = "demo:orders", "demo:orders:dedup"
	rdb.Del(ctx, stream, dedupKey)

	// 兩個消費組：billing / notify → 各自收到全部事件（fan-out，互不影響）
	rdb.XGroupCreateMkStream(ctx, stream, "billing", "0")
	rdb.XGroupCreateMkStream(ctx, stream, "notify", "0")

	// 下單服務寫 3 筆訂單事件（MAXLEN ~ 近似修剪，控制長度）
	orders := []struct {
		id     string
		amount int
	}{{"1001", 250}, {"1002", 990}, {"1003", 130}}
	for _, o := range orders {
		rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream, MaxLen: 100000, Approx: true,
			Values: map[string]any{"order_id": o.id, "amount": o.amount},
		})
	}
	fmt.Printf("下單服務寫入 3 筆訂單事件（XLEN=%d）\n", rdb.XLen(ctx, stream).Val())

	// 計費 worker-1 消費新訊息 > → 冪等處理；故意漏 ack 最後一筆（模擬崩潰）
	msgs, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: "billing", Consumer: "worker-1",
		Streams: []string{stream, ">"}, Count: 10,
	}).Result()
	if err != nil || len(msgs) == 0 {
		fmt.Printf("XReadGroup 無訊息或出錯：%v\n", err)
		return
	}
	list := msgs[0].Messages
	for i, m := range list {
		orderID, _ := m.Values["order_id"].(string)
		if !billProcessed(ctx, rdb, dedupKey, orderID) { // order_id 去重
			fmt.Printf("  billing/worker-1 計費 order=%s amount=%v\n", orderID, m.Values["amount"])
		}
		if i < len(list)-1 {
			rdb.XAck(ctx, stream, "billing", m.ID) // 前兩筆正常 ack
		} else {
			fmt.Printf("  ✗ worker-1 處理 order=%s 後崩潰，來不及 XACK\n", orderID)
		}
	}

	// 監控：XPENDING 看堆積（那筆沒 ack 的留在 PEL）
	pend, _ := rdb.XPending(ctx, stream, "billing").Result()
	fmt.Printf("  billing 待確認(PEL)= %d 筆\n", pend.Count)

	// worker-2 回收 idle 訊息（示範 MinIdle=0 立即認領；生產設 60s）
	claimed, _, _ := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: stream, Group: "billing", Consumer: "worker-2",
		MinIdle: 0, Start: "0", Count: 10,
	}).Result()
	for _, m := range claimed {
		fmt.Printf("  ✓ worker-2 認領並補計費 order=%s\n", m.Values["order_id"])
		rdb.XAck(ctx, stream, "billing", m.ID)
	}

	// fan-out：notify 組獨立收到全部 3 筆（跟 billing 各拿全部、互不影響）
	nmsgs, _ := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: "notify", Consumer: "n1",
		Streams: []string{stream, ">"}, Count: 10,
	}).Result()
	if len(nmsgs) > 0 {
		fmt.Printf("  notify 組獨立收到 %d 筆（fan-out）\n", len(nmsgs[0].Messages))
		for _, m := range nmsgs[0].Messages {
			rdb.XAck(ctx, stream, "notify", m.ID)
		}
	}
	fmt.Println("（重點：ack 才移出 PEL；崩潰未 ack 由 XAUTOCLAIM 回收；多組=fan-out）")
}

// billProcessed 用 Set 對 order_id 去重，回傳「之前是否已處理過」（冪等）。
func billProcessed(ctx context.Context, rdb *redis.Client, dedupKey, orderID string) bool {
	added, _ := rdb.SAdd(ctx, dedupKey, orderID).Result()
	return added == 0 // 0 = 已存在（處理過）
}

// 19. Bitmap：日活 DAU + 連續活躍 + 月簽到卡 ---------------------------------
// 真實情境：一個 user 一個 bit，海量布林狀態用極少記憶體（千萬 uid 日活 ≈ 1.25MB）。
// 前提：uid 必須是密集自增整數，不能拿手機號/UUID/雪花 ID 當 offset（會撐爆記憶體）。
func demoBitmap(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 19. Bitmap：日活 DAU + 連續活躍 + 月簽到卡 ===")
	const d1, d2, both = "active:20260713", "active:20260714", "active:both"
	const sign = "sign:u100:202607"
	rdb.Del(ctx, d1, d2, both, sign)

	// (1) 日活：uid 100/105/233/512 今天活躍 → 一個 bit 一人
	for _, uid := range []int64{100, 105, 233, 512} {
		rdb.SetBit(ctx, d1, uid, 1)
	}
	dau, _ := rdb.BitCount(ctx, d1, nil).Result()
	fmt.Printf("7/13 DAU = %d\n", dau)

	// (2) 連續兩天都活躍：BITOP AND 交集
	for _, uid := range []int64{100, 233, 999} {
		rdb.SetBit(ctx, d2, uid, 1)
	}
	rdb.BitOpAnd(ctx, both, d1, d2)
	n, _ := rdb.BitCount(ctx, both, nil).Result()
	fmt.Printf("連兩天都活躍 = %d 人（100、233）\n", n)

	// (3) 月簽到卡：第 D 天簽到 → offset D-1（別差一）
	for _, day := range []int64{1, 3, 7, 13} {
		rdb.SetBit(ctx, sign, day-1, 1)
	}
	days, _ := rdb.BitCount(ctx, sign, nil).Result()
	signedToday, _ := rdb.GetBit(ctx, sign, 13-1).Result()
	fmt.Printf("u100 本月簽到 %d 天；今天(13號)已簽=%v\n", days, signedToday == 1)
	fmt.Println("（記憶體由最大 offset 決定，非設了幾個 1；uid 稀疏就改用 Set）")
}

// 20. HyperLogLog：每日/每週 UV 估算 -----------------------------------------
// 真實情境：只要「不重複數量」不要「名單」時，固定 ~12KB 估算任意規模基數，
// 誤差 ±0.81%。1 億去重元素：Set 要數 GB，HLL 固定 12KB。
func demoHLL(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 20. HyperLogLog：每日/每週 UV 估算 ===")
	const d1, d2, week = "uv:20260713", "uv:20260714", "uv:week28"
	rdb.Del(ctx, d1, d2, week)

	// 當日訪客（含重複，自動去重）
	rdb.PFAdd(ctx, d1, "u1", "u2", "u3", "u1", "u2")
	uv1, _ := rdb.PFCount(ctx, d1).Result()
	fmt.Printf("7/13 UV ≈ %d（送了 5 次含重複，去重估 3）\n", uv1)

	rdb.PFAdd(ctx, d2, "u3", "u4", "u5")
	// 週 UV：多 key 聯集去重（u3 跨日只算一次）
	wk, _ := rdb.PFCount(ctx, d1, d2).Result()
	fmt.Printf("兩日聯集 UV ≈ %d（u3 跨日只算一次）\n", wk)

	// PFMERGE 成週 key 便於快取
	rdb.PFMerge(ctx, week, d1, d2)
	merged, _ := rdb.PFCount(ctx, week).Result()
	fmt.Printf("PFMERGE 週 key ≈ %d（固定 ~12KB，不隨訪客數長）\n", merged)
	fmt.Println("（只能算數量與聯集，不能列名單、不能算交集；估算值不可用於計費對帳）")
}

// 21. Geo：附近的騎手匹配 ----------------------------------------------------
// 真實情境：底層是 ZSet（GeoHash 當 score），所以 ZSet 指令都能用。
// 外送派單、附近的人、地理圍欄都是這招。
func demoGeo(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 21. Geo：附近的騎手匹配 ===")
	const key = "drivers:online"
	rdb.Del(ctx, key)

	// 騎手上線更新位置（注意順序：先經度 lon 後緯度 lat，寫反會跑到地球另一邊）
	rdb.GeoAdd(ctx, key,
		&redis.GeoLocation{Name: "driver-A", Longitude: 121.5654, Latitude: 25.0330},
		&redis.GeoLocation{Name: "driver-B", Longitude: 121.5170, Latitude: 25.0478},
		&redis.GeoLocation{Name: "driver-C", Longitude: 121.6000, Latitude: 25.1000},
	)

	// 餐廳座標 3km 內、最近的候選（ASC + COUNT，別漏 COUNT 否則回全部）
	res, _ := rdb.GeoSearchLocation(ctx, key, &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude: 121.55, Latitude: 25.04,
			Radius: 3, RadiusUnit: "km", Sort: "ASC", Count: 20,
		},
		WithCoord: true, WithDist: true,
	}).Result()
	fmt.Printf("餐廳 3km 內候選 %d 名：\n", len(res))
	for _, l := range res {
		fmt.Printf("  %s  %.2fkm\n", l.Name, l.Dist)
	}

	// 派單給最近的 → 移出可派池（Geo 無 GEODEL，用 ZSet 的 ZREM）
	if len(res) > 0 {
		picked := res[0].Name
		rdb.ZRem(ctx, key, picked)
		left, _ := rdb.ZCard(ctx, key).Result()
		fmt.Printf("派單給 %s，移出可派池；剩 %d 名在線\n", picked, left)
	}
	fmt.Println("（lon,lat 順序別寫反；刪點用 ZREM；全國規模按城市分 key 防 big key）")
}

// 22. Bitfield：用戶功能使用次數面板（記憶體極限優化）------------------------
// 真實情境：多個小計數器打包在一個 key 的不同 bit 段，比多個 String key 省
// key metadata；OVERFLOW SAT 讓計數封頂不溢位。只有記憶體真是瓶頸才值得（可讀性差）。
func demoBitfield(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== 22. Bitfield：用戶功能使用面板（8 功能打包一 key）===")
	const key = "feat:u100:20260713"
	rdb.Del(ctx, key)

	// 功能 #0 點 3 次、#3 點 1 次（每個 u8 計數，SAT 防溢位）
	for i := 0; i < 3; i++ {
		rdb.BitField(ctx, key, "OVERFLOW", "SAT", "INCRBY", "u8", "#0", 1)
	}
	rdb.BitField(ctx, key, "OVERFLOW", "SAT", "INCRBY", "u8", "#3", 1)

	// 飽和示範：#0 再加 300 → u8 封頂 255，不回繞
	rdb.BitField(ctx, key, "OVERFLOW", "SAT", "INCRBY", "u8", "#0", 300)

	// 一條指令原子取回全部欄位（#0~#3）
	vals, _ := rdb.BitField(ctx, key,
		"GET", "u8", "#0", "GET", "u8", "#1", "GET", "u8", "#2", "GET", "u8", "#3").Result()
	fmt.Printf("功能使用面板 #0~#3 = %v（#0 飽和封頂 255）\n", vals)
	fmt.Println("（8 功能打包一 key=8byte，省 key metadata；語意穩+記憶體瓶頸才用，否則 Hash 更好維運）")
}
