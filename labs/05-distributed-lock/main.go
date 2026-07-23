// 執行前先起 redis：make single-up
// 執行：           go run ./labs/05-distributed-lock
//
//	位址／密碼可用環境變數覆寫：REDIS_ADDR、REDIS_PASSWORD
package main

import (
	"context"
	"fmt"
	"log"
	"os"
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
	rdb := redis.NewClient(&redis.Options{
		Addr:     env("REDIS_ADDR", "127.0.0.1:6379"),
		Password: env("REDIS_PASSWORD", "devpass_change_me"),
		DB:       0,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("連不上 redis：%v\n(先跑 make single-up；密碼用 REDIS_PASSWORD 覆寫)", err)
	}

	key := "lock:demo"
	rdb.Del(ctx, key, "fence:demo")

	// 1. 互斥：A 拿到，B 拿不到
	a, ok, err := Acquire(ctx, rdb, key, 10*time.Second)
	must(err)
	fmt.Printf("A 取鎖: %v (token=%s...)\n", ok, a.Token()[:8])

	_, ok2, err := Acquire(ctx, rdb, key, 10*time.Second)
	must(err)
	fmt.Printf("B 取鎖: %v （預期 false，已被 A 持有）\n", ok2)

	// 2. holder-safe 釋放：偽造的 token 釋放不掉
	fake := &Lock{rdb: rdb, key: key, token: "not-a-real-token", ttl: 10 * time.Second}
	deleted, err := fake.Release(ctx)
	must(err)
	fmt.Printf("假 token 釋放: %v （預期 false，Lua 比對擋掉）\n", deleted)

	// 3. Refresh 續命
	extended, err := a.Refresh(ctx)
	must(err)
	fmt.Printf("A 續命: %v\n", extended)

	// 4. A 正常釋放，之後 B 才拿得到
	deleted, err = a.Release(ctx)
	must(err)
	fmt.Printf("A 釋放: %v\n", deleted)

	c, ok3, err := Acquire(ctx, rdb, key, 10*time.Second)
	must(err)
	fmt.Printf("C 取鎖: %v （A 釋放後可拿）\n", ok3)
	_, _ = c.Release(ctx)

	// 5. fencing token：每次取鎖序號嚴格遞增
	l1, f1, _, _ := AcquireWithFence(ctx, rdb, key, "fence:demo", 5*time.Second)
	_, _ = l1.Release(ctx)
	l2, f2, _, _ := AcquireWithFence(ctx, rdb, key, "fence:demo", 5*time.Second)
	_, _ = l2.Release(ctx)
	fmt.Printf("fencing token: %d → %d （嚴格遞增，下游用它擋舊請求）\n", f1, f2)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
