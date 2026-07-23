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

	// 6. §5.9 壓軸：100 goroutine 搶鎖驗互斥（+ watchdog 續命）
	demoMutex100(ctx, rdb)
}

// demoMutex100 對應 docs/05 §5.9：100 個 goroutine 搶同一把鎖，對 counter 做「非原子的
// 讀→加一→寫回」。鎖若真互斥，final 必精確等於 100；鎖失效則 < 100（丟失更新）。
// 每把鎖配一個 watchdog（ticker ttl/3 呼叫 Refresh 續命），示範長任務不怕 TTL 到期。
func demoMutex100(ctx context.Context, rdb *redis.Client) {
	fmt.Println("\n=== §5.9 100 goroutine 搶鎖驗互斥 ===")
	const lockKey, counterKey = "lock:counter", "app:counter"
	rdb.Del(ctx, lockKey, counterKey)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			// 每個 goroutine：自旋搶鎖 → watchdog 續命 → 非原子讀改寫 → 停 watchdog + 釋放
			lock, err := spinAcquire(ctx, rdb, lockKey, 3*time.Second, 20*time.Millisecond)
			if err != nil {
				log.Printf("acquire: %v", err)
				return
			}
			stop := startWatchdog(lock)
			defer func() {
				close(stop) // 先停 watchdog，才釋放（避免釋放後又續命）
				if _, err := lock.Release(ctx); err != nil {
					log.Printf("release: %v", err)
				}
			}()

			// 臨界區：故意用「讀出→加一→寫回」的非原子序列；只有鎖真互斥，final 才會精確 100。
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

// spinAcquire 自旋搶鎖，直到成功或 ctx 取消。復用 lock.go 的 try-once Acquire，
// 沒搶到就 select 退避（睡 retry，但 ctx 一取消立刻醒）—— 即「可取消的等待」。
func spinAcquire(ctx context.Context, rdb *redis.Client, key string, ttl, retry time.Duration) (*Lock, error) {
	for {
		l, ok, err := Acquire(ctx, rdb, key, ttl)
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

// startWatchdog 每 ttl/3 呼叫 Refresh 續命，回傳 stop channel；close(stop) 停止。
// 續命失敗（鎖已丟失或被他人持有）也會自動停。
func startWatchdog(l *Lock) chan struct{} {
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(l.ttl / 3)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if ok, err := l.Refresh(context.Background()); err != nil || !ok {
					return // 鎖已非本人持有 → 停止續命
				}
			}
		}
	}()
	return stop
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
