// Lab 05 — 分散式鎖（Distributed Lock）
//
// 對應 docs/05 併發原語。示範「三道防線」的前兩道 + fencing token：
//  1. holder-safe 釋放：Lua 比對 token 才 DEL（避免刪到別人的鎖）
//  2. watchdog 續命：Lua 比對 token 才 PEXPIRE（只有持有者能續）
//  3. fencing token：INCR 拿單調遞增序號，下游用它擋「過期後才到」的舊請求
//
// Lua 用 redis.NewScript 定義一次（套件層 var），內部走 EVALSHA→NOSCRIPT→EVAL，
// 值全走 KEYS/ARGV，不動態拼字串（見 docs/05 的腳本快取鐵律）。
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/redis/go-redis/v9"
)

// releaseScript：持有者安全釋放。GET==token 才 DEL，回 1；否則回 0（不刪別人的）。
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end`)

// refreshScript：watchdog 續命。GET==token 才 PEXPIRE 延長 TTL，回 1；否則回 0（鎖已不是你的）。
var refreshScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
	return 0
end`)

// Lock 是一把分散式鎖的持有憑證。token 是本次持有的隨機識別，用來保證「只有我能釋放/續命」。
type Lock struct {
	rdb   *redis.Client
	key   string
	token string
	ttl   time.Duration
}

// newToken 產生 16 bytes 隨機 token（hex）。每次 Acquire 都不同 → 分辨「這把鎖是不是我拿的」。
func newToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Acquire 嘗試取鎖：SET key token NX PX ttl。
// NX = 只在 key 不存在時設（互斥）；PX = 設毫秒 TTL（持有者 crash 也會自動釋放，避免死鎖）。
// 回傳 (lock, true) 代表拿到；(nil, false) 代表已被別人持有。
func Acquire(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration) (*Lock, bool, error) {
	token := newToken()
	ok, err := rdb.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return &Lock{rdb: rdb, key: key, token: token, ttl: ttl}, true, nil
}

// Release 釋放鎖：Lua 比對 token 才刪。回傳是否真的刪掉（false = 鎖已不是你的，可能已過期被別人拿走）。
//
// 為什麼要 Lua：「GET 比對」和「DEL」若分兩條指令，中間鎖可能過期被別人拿走，
// 你的 DEL 就刪掉別人的鎖。Lua 讓比對 + 刪除原子化（docs/05 三道防線第一道）。
func (l *Lock) Release(ctx context.Context) (bool, error) {
	res, err := releaseScript.Run(ctx, l.rdb, []string{l.key}, l.token).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// Refresh 續命：Lua 比對 token 才把 TTL 延長回 ttl。watchdog 定期呼叫，防止長任務做到一半鎖過期。
// 回傳 false = 鎖已不是你的（別再續，任務該中止）。
func (l *Lock) Refresh(ctx context.Context) (bool, error) {
	ms := l.ttl.Milliseconds()
	res, err := refreshScript.Run(ctx, l.rdb, []string{l.key}, l.token, ms).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// Token 回傳本鎖的 token（測試 / 除錯用）。
func (l *Lock) Token() string { return l.token }

// AcquireWithFence 取鎖並回傳一個單調遞增的 fencing token（第三道防線）。
// fenceKey 是一個獨立計數器；每次成功取鎖 INCR 一次 → 拿到嚴格遞增的序號。
// 下游（DB / 外部資源）記住看過的最大 fence，拒絕比它小的請求 →
// 就算「持有者 A 鎖過期、B 拿到新鎖」，A 遲來的寫入帶舊 fence 會被下游擋掉。
func AcquireWithFence(ctx context.Context, rdb *redis.Client, key, fenceKey string, ttl time.Duration) (*Lock, int64, bool, error) {
	lock, ok, err := Acquire(ctx, rdb, key, ttl)
	if err != nil || !ok {
		return nil, 0, ok, err
	}
	fence, err := rdb.Incr(ctx, fenceKey).Result()
	if err != nil {
		// 拿到鎖但 fence 失敗 → 釋放鎖避免懸空
		_, _ = lock.Release(ctx)
		return nil, 0, false, err
	}
	return lock, fence, true, nil
}
