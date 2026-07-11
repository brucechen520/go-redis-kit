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
