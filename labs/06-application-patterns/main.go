// Lab 06 — 應用層模式：快取穿透的兩道防線
//
//  1. 空值快取（cache null）：DB 也查無 → 快取一個短 TTL 的哨兵，擋住「反覆查不存在的 key」。
//  2. 布隆過濾器前置攔截（bloom filter）：查 DB / 快取「之前」先問布隆——說「一定沒有」就直接擋，
//     連 Redis 快取都不用碰。用 Redis bitmap（SETBIT/GETBIT）自己實作，免 RedisBloom 模組。
//
// 兩者互補（見 main 末尾的分工說明）。對照 docs/06-application-patterns.md 快取三大問題。
//
// 執行前先起 redis：make single-up
// 執行：           go run ./labs/06-application-patterns
//
//	位址／密碼可用環境變數覆寫：REDIS_ADDR、REDIS_PASSWORD
package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
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
}
