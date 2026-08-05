// Lab 06 — 應用層模式：快取穿透的兩道防線
//
//  1. 空值快取（cache null）：DB 也查無 → 快取一個短 TTL 的哨兵，擋住「反覆查不存在的 key」。
//  2. 布隆過濾器前置攔截（bloom filter）：查 DB / 快取「之前」先問布隆——說「一定沒有」就直接擋，
//     連 Redis 快取都不用碰。用 Redis bitmap（SETBIT/GETBIT）自己實作，免 RedisBloom 模組。
//  3. 快取擊穿：互斥鎖重建（SET NX PX + Lua release），只讓一個請求查 DB 重建。
//  4. 快取雪崩：TTL 抖動，讓大量 key 的到期時間錯開，不擠在同一時刻一起過期。
//  5. 限流四算法：固定窗口 / 滑動窗口 / 令牌桶 / 漏桶（全用 Lua 原子化）。
//  6. Token / 認證：session token / JWT 黑名單 / refresh 輪換 + 重放偵測。
//  7. 熔斷 + 降級：Redis 共享的分散式熔斷器（三態）+ fail fast 回退降級值。
//
// 對照 docs/06-application-patterns.md 快取 + 限流 + 認證 + 穩定性四件套。
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

// ────────────────────── 5. 限流四算法（Lua 原子）──────────────────────
//
// 限流 = 進入系統的第一道閘門，超額直接「拒絕」（非排隊）。四種算法各有取捨。
// 全部用 Lua 讓「判斷 + 計數」原子化，避免併發下的競態。對照 docs/06 §3。

// 5.1 固定窗口：INCR 計數，第一次設 EXPIRE 當窗口；超過 limit 拒絕。
// 坑：窗口交界瞬間可過 2 倍量（邊界突刺）。
var fixedWindowScript = redis.NewScript(`
local c = redis.call("INCR", KEYS[1])
if c == 1 then redis.call("EXPIRE", KEYS[1], ARGV[2]) end
if c > tonumber(ARGV[1]) then return 0 else return 1 end`)

func allowFixed(ctx context.Context, rdb *redis.Client, key string, limit, windowSec int) bool {
	n, _ := fixedWindowScript.Run(ctx, rdb, []string{key}, limit, windowSec).Int()
	return n == 1
}

// 5.2 滑動窗口（ZSet）：移除窗口外的時間戳，數剩幾筆；未滿才加入。精確、無邊界突刺。
// 坑：存每筆請求的時間戳，較耗記憶體。
var slidingWindowScript = redis.NewScript(`
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, tonumber(ARGV[1]) - tonumber(ARGV[2]))
if redis.call("ZCARD", KEYS[1]) < tonumber(ARGV[3]) then
	redis.call("ZADD", KEYS[1], ARGV[1], ARGV[4])
	redis.call("PEXPIRE", KEYS[1], ARGV[2])
	return 1
end
return 0`)

func allowSliding(ctx context.Context, rdb *redis.Client, key string, limit int, window time.Duration, member string) bool {
	now := time.Now().UnixMilli()
	n, _ := slidingWindowScript.Run(ctx, rdb, []string{key},
		now, window.Milliseconds(), limit, member).Int()
	return n == 1
}

// 5.3 令牌桶（★最推薦）：桶固定速率補 token，來請求拿一個。惰性補充——取的時候依時間差算該補多少。
// 特點：容忍突發（桶有存量能一次噴一批）+ 平均限速。
var tokenBucketScript = redis.NewScript(`
local rate = tonumber(ARGV[1])   -- 每秒補幾個
local cap  = tonumber(ARGV[2])   -- 桶容量
local now  = tonumber(ARGV[3])   -- 現在(ms)
local req  = tonumber(ARGV[4])   -- 這次要幾個
local ttl  = tonumber(ARGV[5])
local d = redis.call("HMGET", KEYS[1], "tokens", "ts")
local tokens = tonumber(d[1])
local ts = tonumber(d[2])
if tokens == nil then tokens = cap; ts = now end       -- 第一次：滿桶
local elapsed = (now - ts) / 1000
tokens = math.min(cap, tokens + elapsed * rate)         -- 依時間差惰性補充
local allowed = 0
if tokens >= req then tokens = tokens - req; allowed = 1 end
redis.call("HSET", KEYS[1], "tokens", tokens, "ts", now)
redis.call("PEXPIRE", KEYS[1], ttl)
return allowed`)

func allowToken(ctx context.Context, rdb *redis.Client, key string, rate, capacity int) bool {
	now := time.Now().UnixMilli()
	n, _ := tokenBucketScript.Run(ctx, rdb, []string{key},
		rate, capacity, now, 1, 3600000).Int()
	return n == 1
}

// 5.4 漏桶（惰性水位）：請求進桶、固定速率漏出。水位到頂就拒。強制平滑輸出（不容突發）。
var leakyBucketScript = redis.NewScript(`
local leak = tonumber(ARGV[1])   -- 每秒漏幾個
local cap  = tonumber(ARGV[2])   -- 桶容量
local now  = tonumber(ARGV[3])
local ttl  = tonumber(ARGV[4])
local d = redis.call("HMGET", KEYS[1], "water", "ts")
local water = tonumber(d[1])
local ts = tonumber(d[2])
if water == nil then water = 0; ts = now end
local leaked = (now - ts) / 1000 * leak                 -- 依時間差惰性漏水
water = math.max(0, water - leaked)
local allowed = 0
if water + 1 <= cap then water = water + 1; allowed = 1 end
redis.call("HSET", KEYS[1], "water", water, "ts", now)
redis.call("PEXPIRE", KEYS[1], ttl)
return allowed`)

func allowLeaky(ctx context.Context, rdb *redis.Client, key string, leak, capacity int) bool {
	now := time.Now().UnixMilli()
	n, _ := leakyBucketScript.Run(ctx, rdb, []string{key},
		leak, capacity, now, 3600000).Int()
	return n == 1
}

func demoRateLimit(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n==== 5. 限流四算法 ====")
	rdb.Del(ctx, "rl:fixed", "rl:sliding", "rl:token", "rl:leaky")

	// 5.1 固定窗口：limit 5/60s，瞬間打 8 次
	pass := 0
	for i := 0; i < 8; i++ {
		if allowFixed(ctx, rdb, "rl:fixed", 5, 60) {
			pass++
		}
	}
	fmt.Printf("5.1 固定窗口 limit=5/60s，打 8 次 → 通過 %d、拒絕 %d\n", pass, 8-pass)

	// 5.2 滑動窗口：limit 5/60s，member 需唯一
	pass = 0
	for i := 0; i < 8; i++ {
		member := strconv.Itoa(int(time.Now().UnixNano())) + "-" + strconv.Itoa(i)
		if allowSliding(ctx, rdb, "rl:sliding", 5, 60*time.Second, member) {
			pass++
		}
	}
	fmt.Printf("5.2 滑動窗口 limit=5/60s，打 8 次 → 通過 %d、拒絕 %d（精確、無邊界突刺）\n", pass, 8-pass)

	// 5.3 令牌桶：cap 5, rate 1/s。瞬間打 8 次 → 桶存量 5 一次噴出
	pass = 0
	for i := 0; i < 8; i++ {
		if allowToken(ctx, rdb, "rl:token", 1, 5) {
			pass++
		}
	}
	fmt.Printf("5.3 令牌桶 cap=5 rate=1/s，瞬間打 8 次 → 通過 %d（容忍突發，桶存量一次放）\n", pass)
	time.Sleep(2 * time.Second) // 等 2 秒補 2 個 token
	pass = 0
	for i := 0; i < 3; i++ {
		if allowToken(ctx, rdb, "rl:token", 1, 5) {
			pass++
		}
	}
	fmt.Printf("    等 2 秒補 token 後打 3 次 → 通過 %d（惰性補充後可再過）\n", pass)

	// 5.4 漏桶：cap 5, leak 1/s。瞬間打 8 次 → 水位到頂就拒
	pass = 0
	for i := 0; i < 8; i++ {
		if allowLeaky(ctx, rdb, "rl:leaky", 1, 5) {
			pass++
		}
	}
	fmt.Printf("5.4 漏桶 cap=5 leak=1/s，瞬間打 8 次 → 通過 %d（水位到頂即拒，強制平滑）\n", pass)

	fmt.Println("選型：通用首選令牌桶（容忍突發）；要平穩輸出對下游友善用漏桶；要精確用滑動窗口；最簡固定窗口。")
}

// ────────────────────── 6. Token / 認證 ──────────────────────
//
// 三種模式：有狀態 session（可即時撤銷）、JWT 黑名單（補「JWT 簽發後無法作廢」）、
// refresh 輪換 + 重放偵測（防 refresh token 被盜）。對照 docs/06 §4。

// 6.1 Session token：token → userID 存 Redis + TTL。登入寫、驗證查、登出刪。
func login(ctx context.Context, rdb *redis.Client, token, userID string) {
	rdb.Set(ctx, "session:"+token, userID, 7*24*time.Hour)
}

func verifySession(ctx context.Context, rdb *redis.Client, token string) (string, bool) {
	v, err := rdb.Get(ctx, "session:"+token).Result()
	if err != nil {
		return "", false // redis.Nil（已登出/過期）或故障
	}
	return v, true
}

func logout(ctx context.Context, rdb *redis.Client, token string) {
	rdb.Del(ctx, "session:"+token)
}

// 6.2 JWT 黑名單：JWT 無狀態不能撤銷 → 撤銷時把 jti 加進黑名單，TTL = token 剩餘壽命。
func revokeJWT(ctx context.Context, rdb *redis.Client, jti string, remaining time.Duration) {
	rdb.Set(ctx, "jwt:blacklist:"+jti, 1, remaining) // TTL=剩餘壽命，過期自動清
}

func isJWTRevoked(ctx context.Context, rdb *redis.Client, jti string) bool {
	n, _ := rdb.Exists(ctx, "jwt:blacklist:"+jti).Result()
	return n == 1
}

// 6.3 Refresh 輪換 + 重放偵測：每次刷新「刪舊發新」（原子）。舊 token 又被用 → 偵測到重放（被盜）。
// GET 舊 token == userID → 刪舊、發新、回 1；否則（已被用過/無效）回 0 = 重放訊號。
var rotateRefreshScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	redis.call("DEL", KEYS[1])
	redis.call("SET", KEYS[2], ARGV[1], "EX", ARGV[2])
	return 1
else
	return 0
end`)

func rotateRefresh(ctx context.Context, rdb *redis.Client, oldTok, newTok, userID string, ttlSec int) bool {
	n, _ := rotateRefreshScript.Run(ctx, rdb,
		[]string{"refresh:" + oldTok, "refresh:" + newTok}, userID, ttlSec).Int()
	return n == 1
}

func demoAuth(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n==== 6. Token / 認證 ====")
	rdb.Del(ctx, "session:tk-abc", "jwt:blacklist:jti-1",
		"refresh:rt-aaa", "refresh:rt-bbb", "refresh:rt-ccc")

	// 6.1 Session token
	login(ctx, rdb, "tk-abc", "user-7")
	_, ok := verifySession(ctx, rdb, "tk-abc")
	fmt.Printf("6.1 session：登入後驗證 → 有效=%v\n", ok)
	logout(ctx, rdb, "tk-abc")
	_, ok = verifySession(ctx, rdb, "tk-abc")
	fmt.Printf("    登出後驗證 → 有效=%v（有狀態 session 可即時撤銷）\n", ok)

	// 6.2 JWT 黑名單
	fmt.Printf("6.2 JWT：撤銷前 jti-1 被撤銷=%v\n", isJWTRevoked(ctx, rdb, "jti-1"))
	revokeJWT(ctx, rdb, "jti-1", time.Hour) // 假設 token 還剩 1 小時
	fmt.Printf("    撤銷後 jti-1 被撤銷=%v（無狀態 JWT 靠黑名單補撤銷能力）\n", isJWTRevoked(ctx, rdb, "jti-1"))

	// 6.3 Refresh 輪換 + 重放偵測
	rdb.Set(ctx, "refresh:rt-aaa", "user-7", 7*24*time.Hour) // 發 refresh token rt-aaa
	ok1 := rotateRefresh(ctx, rdb, "rt-aaa", "rt-bbb", "user-7", 604800)
	fmt.Printf("6.3 refresh：rt-aaa 輪換成 rt-bbb → 成功=%v（舊的已刪）\n", ok1)
	replay := rotateRefresh(ctx, rdb, "rt-aaa", "rt-ccc", "user-7", 604800)
	fmt.Printf("    攻擊者拿舊 rt-aaa 再輪換 → 成功=%v（false = 偵測到重放，該 user 全部 token 應作廢告警）\n", replay)
}

// ────────────────────── 7. 熔斷 + 降級 ──────────────────────
//
// 穩定性四件套的另兩件（限流的隊友）。熔斷/降級本質是應用層邏輯，單機常用 in-process
// (gobreaker)；這裡做 Redis 共享版，讓「多實例共用同一個熔斷狀態」。
//
// 熔斷三態：Closed(正常放行) → 連續失敗達門檻 → Open(斷開，直接 fail fast) →
//           冷卻期過 → Half-Open(放一個試探) → 成功回 Closed、失敗再 Open。
// 降級 = 熔斷中或呼叫失敗時，回退到預設值(fallback)，不讓錯誤往上炸、也不再打掛掉的下游。

// cbFailScript：記一次失敗；連續失敗達門檻 → 開熔斷(SET open EX cooldown)並清計數。
var cbFailScript = redis.NewScript(`
local f = redis.call("INCR", KEYS[1])
redis.call("EXPIRE", KEYS[1], ARGV[2])
if f >= tonumber(ARGV[1]) then
	redis.call("SET", KEYS[2], 1, "EX", ARGV[3])
	redis.call("DEL", KEYS[1])
	return 1
end
return 0`)

type CircuitBreaker struct {
	rdb       *redis.Client
	name      string
	threshold int // 連續失敗幾次熔斷
	failWin   int // 失敗計數窗口(秒)
	cooldown  int // 熔斷持續(秒)
}

func (cb *CircuitBreaker) openKey() string { return "cb:" + cb.name + ":open" }
func (cb *CircuitBreaker) failKey() string { return "cb:" + cb.name + ":failures" }

// allow：open key 不存在才放行（存在 = 熔斷中）。
func (cb *CircuitBreaker) allow(ctx context.Context) bool {
	n, _ := cb.rdb.Exists(ctx, cb.openKey()).Result()
	return n == 0
}
func (cb *CircuitBreaker) onSuccess(ctx context.Context) { cb.rdb.Del(ctx, cb.failKey()) }
func (cb *CircuitBreaker) onFailure(ctx context.Context) {
	cbFailScript.Run(ctx, cb.rdb, []string{cb.failKey(), cb.openKey()},
		cb.threshold, cb.failWin, cb.cooldown)
}

// Call：熔斷中直接降級（連 fn 都不呼叫）；否則呼叫 fn，失敗也降級並記一次失敗。
func (cb *CircuitBreaker) Call(ctx context.Context, fn func() (string, error), fallback string) (string, bool) {
	if !cb.allow(ctx) {
		return fallback, false // 熔斷中 → fail fast，保護下游
	}
	v, err := fn()
	if err != nil {
		cb.onFailure(ctx)
		return fallback, false // 這次失敗 → 降級
	}
	cb.onSuccess(ctx)
	return v, true
}

func demoCircuitBreaker(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n==== 7. 熔斷 + 降級 ====")
	rdb.Del(ctx, "cb:payment:open", "cb:payment:failures")
	cb := &CircuitBreaker{rdb: rdb, name: "payment", threshold: 3, failWin: 10, cooldown: 2}

	var calls int64
	failing := func() (string, error) {
		atomic.AddInt64(&calls, 1) // 記下游「真的被呼叫」幾次
		return "", errors.New("downstream down")
	}

	// 打 6 次：前 3 次真呼叫(失敗)，第 3 次觸發熔斷 → 後面 fail fast 不再呼叫下游
	for i := 1; i <= 6; i++ {
		_, ok := cb.Call(ctx, failing, "DEFAULT")
		fmt.Printf("  第 %d 次：ok=%v（下游已被打 %d 次）\n", i, ok, atomic.LoadInt64(&calls))
	}
	fmt.Printf("→ 下游掛掉，只被打 %d 次就熔斷，其餘 fail fast 回降級值 DEFAULT，保護下游不被繼續打\n",
		atomic.LoadInt64(&calls))

	// 冷卻 2 秒後 half-open：下游恢復
	time.Sleep(2100 * time.Millisecond)
	recovered := func() (string, error) { atomic.AddInt64(&calls, 1); return "REAL", nil }
	v, ok := cb.Call(ctx, recovered, "DEFAULT")
	fmt.Printf("  冷卻後試探：ok=%v 回傳=%q（half-open 放行，成功 → 熔斷關閉、恢復正常）\n", ok, v)
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

	// ===== 5. 限流四算法 =====
	demoRateLimit(ctx, rdb)

	// ===== 6. Token / 認證 =====
	demoAuth(ctx, rdb)

	// ===== 7. 熔斷 + 降級 =====
	demoCircuitBreaker(ctx, rdb)
}
