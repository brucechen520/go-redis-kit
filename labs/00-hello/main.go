package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	// 1. 建立 client。NewClient 不會馬上連線，只是準備好連線池設定。
	//    真正的 TCP 連線是「第一次執行指令時」才從連線池按需建立。
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379", // Redis 位址
		Password: "",               // 沒設密碼就留空 - devpass_change_me
		DB:       0,                // 用 DB 0
		// --- 連線池相關（先用預設，這裡列出讓你知道有這些旋鈕）---
		// PoolSize:     10 * runtime.GOMAXPROCS, // 預設：每個 CPU 10 條連線
		// MinIdleConns: 0,                        // 預設不預熱閒置連線
		// DialTimeout:  5 * time.Second,          // 建立連線逾時
		// ReadTimeout:  3 * time.Second,          // 讀取逾時
		// WriteTimeout: 3 * time.Second,          // 寫入逾時
	})
	// 程式結束前關掉連線池，釋放所有連線。
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Printf("close redis: %v", err)
		}
	}()

	// 2. 每個操作都要帶 context，用來控制逾時 / 取消。
	//    這是 v9 的核心慣例：所有指令方法第一個參數都是 ctx。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 3. PING 確認連得到。錯誤處理一定要做，別忽略。
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("cannot reach redis: %v", err)
	}
	fmt.Println("PING ok, connected to Redis")

	// 4. 寫一個 key，設 60 秒 TTL。
	if err := rdb.Set(ctx, "greeting", "hello from go", 60*time.Second).Err(); err != nil {
		log.Fatalf("SET failed: %v", err)
	}

	// 5. 讀回來。
	val, err := rdb.Get(ctx, "greeting").Result()
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	fmt.Printf("GET greeting = %q\n", val)

	// 6. 處理「key 不存在」——go-redis 用 redis.Nil 這個哨兵錯誤表示。
	//    這是新手最常踩的坑：key 不存在不是「空字串」，是一個 error。
	_, err = rdb.Get(ctx, "does-not-exist").Result()
	if errors.Is(err, redis.Nil) {
		fmt.Println("key does-not-exist: not found (redis.Nil), 這是正常情況不是故障")
	} else if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
}
