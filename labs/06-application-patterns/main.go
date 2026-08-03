// Lab 06 — 應用層模式：快取穿透的兩道防線
//
//  1. 空值快取（cache null）：DB 也查無 → 快取一個短 TTL 的哨兵，擋住「反覆查不存在的 key」。
//  2. 布隆過濾器前置攔截（bloom filter）：查 DB / 快取「之前」先問布隆——說「一定沒有」就直接擋，
//     連 Redis 快取都不用碰。用 Redis bitmap（SETBIT/GETBIT）自己實作，免 RedisBloom 模組。
//  3. 快取擊穿：互斥鎖重建（SET NX PX + Lua release），只讓一個請求查 DB 重建。
//  4. 快取雪崩：TTL 抖動，讓大量 key 的到期時間錯開，不擠在同一時刻一起過期。
//
// 對照 docs/06-application-patterns.md 快取三大問題 + 一致性。
//
// 執行前先起 redis：make single-up
// 執行：           go run ./labs/06-application-patterns
//
//	位址／密碼可用環境變數覆寫：REDIS_ADDR、REDIS_PASSWORD
package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const nullSentinel = "__NULL__" // 哨兵：代表 DB 確認不存在

var ErrNotFound = errors.New("resource not found")

// 假 DB：只有 "1"、"2" 存在。
func loadUserFromDB(_ context.Context, id string) (string, error) {
	db := map[string]string{"1": "Bill", "2": "Amy"}
	if name, ok := db[id]; ok {
		return name, nil
	}
	return "", ErrNotFound
}

// ────────────────────────── 1. 空值快取 ──────────────────────────

// GetUser 讀 user，帶空值快取防穿透。
func GetUser(ctx context.Context, rdb *redis.Client, id string) (string, error) {
	key := "user:" + id

	val, err := rdb.Get(ctx, key).Result()
	switch {
	case err == nil && val == nullSentinel:
		// 命中「不存在」標記，直接擋掉，不打 DB
		return "", ErrNotFound
	case err == nil:
		return val, nil // 正常命中
	case !errors.Is(err, redis.Nil):
		return "", err // Redis 真的壞了
	}

	// 快取 miss，回源 DB
	user, err := loadUserFromDB(ctx, id)
	if errors.Is(err, ErrNotFound) {
		// 關鍵：DB 也沒有 → 寫短 TTL 空值快取（TTL 短，避免之後真的新增了還被擋太久）
		rdb.Set(ctx, key, nullSentinel, 60*time.Second)
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}

	rdb.Set(ctx, key, user, 10*time.Minute)
	return user, nil
}

// ────────────────────── 2. 布隆過濾器（Redis bitmap 版）──────────────────────

// BloomFilter 用 Redis 的一個 bitmap key 當底層儲存。
// 特性：沒有 false negative（說「沒有」就一定沒有）；有 false positive（說「有」可能其實沒有）。
// 所以拿來做「前置攔截」：說沒有 → 安全直接擋；說有 → 放行，後面再靠空值快取/DB 兜底。
type BloomFilter struct {
	rdb *redis.Client
	key string
	m   uint64 // bitmap 位元數
	k   uint   // 每個元素用幾個 hash 位置
}

func NewBloomFilter(rdb *redis.Client, key string, m uint64, k uint) *BloomFilter {
	return &BloomFilter{rdb: rdb, key: key, m: m, k: k}
}

// positions 用「雙重雜湊」由兩個 hash 推出 k 個位置：pos_i = (h1 + i*h2) mod m。
// 只算兩次 hash 就能產生 k 個位置，是布隆常見手法（Kirsch-Mitzenmacher）。
func (b *BloomFilter) positions(item string) []int64 {
	h1 := fnv.New64a()
	_, _ = h1.Write([]byte(item))
	a := h1.Sum64()

	h2 := fnv.New64a()
	_, _ = h2.Write([]byte("salt:" + item)) // 加 salt 得到獨立的第二個 hash
	c := h2.Sum64()

	pos := make([]int64, b.k)
	for i := uint(0); i < b.k; i++ {
		pos[i] = int64((a + uint64(i)*c) % b.m)
	}
	return pos
}

// Add 把 item 的 k 個位置設為 1（pipeline 一次送）。
func (b *BloomFilter) Add(ctx context.Context, item string) error {
	pipe := b.rdb.Pipeline()
	for _, p := range b.positions(item) {
		pipe.SetBit(ctx, b.key, p, 1)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// MightContain 查 k 個位置：任一為 0 → 一定不存在（回 false）；全為 1 → 可能存在（回 true）。
func (b *BloomFilter) MightContain(ctx context.Context, item string) (bool, error) {
	pipe := b.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, 0, b.k)
	for _, p := range b.positions(item) {
		cmds = append(cmds, pipe.GetBit(ctx, b.key, p))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	for _, c := range cmds {
		if c.Val() == 0 {
			return false, nil // 有一位是 0 → 布隆保證：一定沒加過
		}
	}
	return true, nil
}

// GetUserGuarded 前置攔截版：先問布隆，說「一定沒有」直接擋，連快取都不碰；說「可能有」才走空值快取那條。
func GetUserGuarded(ctx context.Context, rdb *redis.Client, bf *BloomFilter, id string) (string, error) {
	might, err := bf.MightContain(ctx, id)
	if err != nil {
		return "", err
	}
	if !might {
		// 布隆說一定沒有 → 最前面就擋掉，省下 Redis 快取查詢 + DB 查詢
		return "", ErrNotFound
	}
	// 可能有（含 false positive）→ 交給空值快取那條處理
	return GetUser(ctx, rdb, id)
}

// ────────────────────── 3. 快取擊穿：互斥鎖重建 ──────────────────────
//
// 擊穿 = 單一熱 key 過期瞬間，大量請求同時 miss 湧向 DB。
// 對策：miss 時搶「重建鎖」（Stage 5 的 SET NX PX + Lua release），只讓一個請求查 DB 回填，
// 其餘 backoff 後重讀快取。對照 docs/06 §2.2（生產版再加 singleflight 擋單機 + TTL 抖動）。

// breakdownReleaseScript：Stage 5 的安全解鎖——比對 token 才 DEL，不刪到別人的鎖。
var breakdownReleaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end`)

func newToken() string {
	b := make([]byte, 16)
	_, _ = crand.Read(b)
	return hex.EncodeToString(b)
}

// rebuildHotKey：讀快取，miss 時用分散式鎖確保「全叢集只有一個」查 DB 重建。
func rebuildHotKey(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration, load func() (string, error)) (string, error) {
	// 1) 先讀快取
	if v, err := rdb.Get(ctx, key).Result(); err == nil {
		return v, nil
	} else if !errors.Is(err, redis.Nil) {
		return "", err
	}

	// 2) miss：搶重建鎖
	lockKey := "lock:rebuild:" + key
	token := newToken()
	got, err := rdb.SetNX(ctx, lockKey, token, 5*time.Second).Result()
	if err != nil {
		return "", err
	}

	if !got {
		// 沒搶到：別人正在重建 → backoff 再讀一次（此時通常已被填好）
		time.Sleep(50 * time.Millisecond)
		if v, err := rdb.Get(ctx, key).Result(); err == nil {
			return v, nil
		}
		// 還沒好也別死等：這次退化直接回源（偶發多打一次 DB，可接受）
		return load()
	}
	// 搶到鎖：離開前安全釋放
	defer breakdownReleaseScript.Run(ctx, rdb, []string{lockKey}, token)

	// 3) double-check：搶鎖期間別人可能已填好
	if v, err := rdb.Get(ctx, key).Result(); err == nil {
		return v, nil
	}

	// 4) 回源 + 回填（TTL 加抖動防雪崩）
	v, err := load()
	if err != nil {
		return "", err
	}
	rdb.Set(ctx, key, v, ttl+time.Duration(rand.Int63n(int64(ttl/5))))
	return v, nil
}

// ────────────────────── 4. 快取雪崩：TTL 抖動 ──────────────────────
//
// 雪崩 = 大量 key「同時」過期 → 那一瞬間全部 miss 一起打 DB，DB 被瞬間打爆。
// 對策：TTL 加隨機抖動（base + rand），讓到期時間錯開，不再擠在同一時刻。
// 本 demo 用 PTTL 分佈證明：固定 TTL 全擠同一時間桶；抖動後分散到多個桶。

// warmKeys 灌 n 把 key，TTL 由 ttlFn 決定（pipeline 一次送）。
func warmKeys(ctx context.Context, rdb *redis.Client, prefix string, n int, ttlFn func() time.Duration) {
	pipe := rdb.Pipeline()
	for i := 0; i < n; i++ {
		pipe.Set(ctx, prefix+strconv.Itoa(i), "v", ttlFn())
	}
	_, _ = pipe.Exec(ctx)
}

// ttlSpread 讀 n 把 key 的剩餘 TTL，依「10 秒一桶」歸類，回傳 (不同桶數, 單一桶最多幾把)。
// 不同桶數越多 = 過期越分散；單一桶最多幾把 = 最壞情況同一時刻多少 key 一起過期（雪崩規模）。
func ttlSpread(ctx context.Context, rdb *redis.Client, prefix string, n int) (buckets, maxInBucket int) {
	const bucket = 10 * time.Second
	pipe := rdb.Pipeline()
	cmds := make([]*redis.DurationCmd, n)
	for i := 0; i < n; i++ {
		cmds[i] = pipe.PTTL(ctx, prefix+strconv.Itoa(i))
	}
	_, _ = pipe.Exec(ctx)

	hist := map[int64]int{}
	for _, c := range cmds {
		if d := c.Val(); d > 0 {
			hist[int64(d/bucket)]++ // 落在第幾個 10 秒桶
		}
	}
	for _, cnt := range hist {
		if cnt > maxInBucket {
			maxInBucket = cnt
		}
	}
	return len(hist), maxInBucket
}

func demoAvalanche(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n==== 4. 快取雪崩：TTL 抖動 ====")
	const n = 1000
	baseTTL := 3600 * time.Second
	jitter := 600 * time.Second // 抖動幅度：0~600 秒

	// 策略 A：固定 TTL → 全部同一時刻過期
	warmKeys(ctx, rdb, "av:fixed:", n, func() time.Duration { return baseTTL })
	// 策略 B：抖動 TTL → 到期時間錯開
	warmKeys(ctx, rdb, "av:jitter:", n, func() time.Duration {
		return baseTTL + time.Duration(rand.Int63n(int64(jitter)))
	})

	fBuckets, fMax := ttlSpread(ctx, rdb, "av:fixed:", n)
	jBuckets, jMax := ttlSpread(ctx, rdb, "av:jitter:", n)

	fmt.Printf("固定 TTL：%d 把 key 到期時間落在 %d 個時間桶，單一時刻最多 %d 把一起過期 → 雪崩\n",
		n, fBuckets, fMax)
	fmt.Printf("抖動 TTL：%d 把 key 到期時間落在 %d 個時間桶，單一時刻最多 %d 把一起過期 → 削平\n",
		n, jBuckets, jMax)
	fmt.Println("關鍵：固定 TTL 最壞情況所有 key 同秒過期，DB 瞬間被打 1000 次；")
	fmt.Println("      抖動把過期攤平到一段區間，任一時刻只有少量 miss，DB 扛得住。")
}

// ────────────────────────────── main ──────────────────────────────

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     env("REDIS_ADDR", "127.0.0.1:6379"),
		Password: env("REDIS_PASSWORD", "devpass_change_me"),
		DB:       0,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("連不上 redis：%v\n(先跑 make single-up；密碼用 REDIS_PASSWORD 覆寫)", err)
	}
	// 清掉上次殘留，每次乾淨開始
	rdb.Del(ctx, "user:1", "user:2", "user:999999", "user:404", "bloom:users")

	// ===== 1. 空值快取 =====
	fmt.Println("==== 1. 空值快取 ====")
	// 第一次查不存在的 id → 回源 DB（miss），寫入空值哨兵
	_, err := GetUser(ctx, rdb, "999999")
	fmt.Printf("查 999999（首次，回源 DB）→ %v\n", err)
	// 確認哨兵已寫入
	v, _ := rdb.Get(ctx, "user:999999").Result()
	fmt.Printf("  快取內容 = %q（下次同 id 直接被擋，不再打 DB）\n", v)
	// 第二次查同一個 → 命中哨兵，不打 DB
	_, err = GetUser(ctx, rdb, "999999")
	fmt.Printf("查 999999（再次，命中空值快取）→ %v\n\n", err)

	// ===== 2. 布隆過濾器前置攔截 =====
	fmt.Println("==== 2. 布隆過濾器前置攔截 ====")
	// m=65536 bits（8KB），k=7；把「DB 裡真的存在的 id」全加進布隆
	bf := NewBloomFilter(rdb, "bloom:users", 1<<16, 7)
	for _, id := range []string{"1", "2"} {
		if err := bf.Add(ctx, id); err != nil {
			log.Fatalf("bloom add: %v", err)
		}
	}
	fmt.Println("已把存在的 id {1,2} 加入布隆")

	// 存在的 → 布隆說可能有 → 放行 → 命中 DB
	name, err := GetUserGuarded(ctx, rdb, bf, "1")
	fmt.Printf("查 1   → 布隆放行, 結果 name=%q err=%v\n", name, err)

	// 不存在的 → 布隆極大機率說「一定沒有」→ 最前面就擋，省下快取+DB 查詢
	might, _ := bf.MightContain(ctx, "404")
	_, err = GetUserGuarded(ctx, rdb, bf, "404")
	fmt.Printf("查 404 → 布隆 MightContain=%v（false 代表一定沒有 → 直接擋）, err=%v\n", might, err)

	fmt.Println("\n── 兩道防線分工 ──")
	fmt.Println("布隆：擋『從沒存在過』的 id（如亂猜/掃描攻擊），最前置、最省，但有 false positive。")
	fmt.Println("空值快取：擋『曾查過、DB 確認不存在』的 id（含布隆放行的 false positive），兜底。")
	fmt.Println("實務：布隆前置攔掉大量亂打，漏網的 false positive 再由空值快取擋 → 兩層防穿透。")

	// ===== 3. 快取擊穿：互斥鎖重建 =====
	fmt.Println("\n==== 3. 快取擊穿：互斥鎖重建 ====")
	hotKey := "hot:banner"
	rdb.Del(ctx, hotKey, "lock:rebuild:"+hotKey) // 模擬熱 key 剛過期（不存在）

	var dbHits int64
	load := func() (string, error) {
		atomic.AddInt64(&dbHits, 1)       // 記 DB 真的被查幾次
		time.Sleep(30 * time.Millisecond) // 模擬 DB 查詢耗時
		return "BANNER_v1", nil
	}

	// N 個並發請求同時擊穿同一個剛過期的熱 key
	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if _, err := rebuildHotKey(ctx, rdb, hotKey, 10*time.Minute, load); err != nil {
				log.Printf("rebuild: %v", err)
			}
		}()
	}
	wg.Wait()

	fmt.Printf("%d 個並發請求同時 miss → DB 實際被查 %d 次（互斥鎖重建，理想 ≈1）\n",
		n, atomic.LoadInt64(&dbHits))
	fmt.Println("沒有鎖的話 DB 會被查 ~50 次；有鎖 → 只有搶到的那個查，其餘讀回填好的快取。")
	fmt.Println("註：這是單機示範。生產再加 singleflight（擋單機並發）+ TTL 抖動（防雪崩），見 docs/06 §2.2。")

	// ===== 4. 快取雪崩：TTL 抖動 =====
	demoAvalanche(ctx, rdb)
}
