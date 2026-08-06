package rediskit

import (
	"github.com/redis/go-redis/v9"
)

// 所有 Lua 腳本集中在這個檔案，方便一次 review 原子性。
//
// 鐵律（docs/05 腳本快取）：
//   - redis.NewScript 內部走 EVALSHA → NOSCRIPT 時自動 fallback EVAL 並快取 SHA，
//     不用自己管腳本載入。
//   - 值全走 KEYS/ARGV，不動態拼字串進腳本本文。
//   - 腳本必須是決定性的：時間由 Go 端經 ARGV 傳入，腳本內不呼叫 TIME/RANDOM
//     （副作用複寫下 replica 才不會分岔，測試也才能注入假時鐘）。

// lockReleaseScript：持有者安全釋放。GET==token 才 DEL。
// 「比對」與「刪除」必須原子：分兩條指令的話，中間鎖可能過期被別人拿走，
// 你的 DEL 就刪掉別人的鎖。回 1 = 刪了自己的鎖；0 = 鎖已不是你的。
var lockReleaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end`)

// lockRefreshScript：watchdog 續命。GET==token 才 PEXPIRE。
// 同樣要原子：只有持有者能延長 TTL。回 0 = 鎖已不是你的，別再續。
var lockRefreshScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
	return 0
end`)

// rateLimitScript：令牌桶。判斷 + 扣減一步原子，桶滿時允許 burst 到容量上限。
//
// 用「毫令牌」（millitoken，1 token = 1000）做整數尺度運算，避免補充速率
// 在整數下被無條件捨去（例如 100 次/分 = 1.666 mt/ms）。
//
// KEYS[1] 桶（hash：t = 剩餘毫令牌，ts = 上次補充時刻 ms）
// ARGV[1] 容量（毫令牌） ARGV[2] 補充速率（毫令牌/ms）
// ARGV[3] 現在時刻（ms，Go 端傳入） ARGV[4] 本次成本（毫令牌） ARGV[5] 桶 TTL（ms）
// 回 1 = 放行；0 = 超限。
var rateLimitScript = redis.NewScript(`
local tokens = tonumber(redis.call("HGET", KEYS[1], "t"))
local ts     = tonumber(redis.call("HGET", KEYS[1], "ts"))
local cap    = tonumber(ARGV[1])
local rate   = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])
local cost   = tonumber(ARGV[4])

if tokens == nil then
	tokens = cap
	ts = now
end

local elapsed = now - ts
if elapsed < 0 then
	elapsed = 0
end
tokens = tokens + elapsed * rate
if tokens > cap then
	tokens = cap
end

local allowed = 0
if tokens >= cost then
	tokens = tokens - cost
	allowed = 1
end

redis.call("HSET", KEYS[1], "t", tokens, "ts", now)
redis.call("PEXPIRE", KEYS[1], ARGV[5])
return allowed`)

// tokenRotateScript：token 原子輪替（CAS）。現值==舊 token 才寫入新 token。
// 「檢查舊值」與「寫入新值」必須原子，否則兩個併發 Rotate 都會成功，
// 重放偵測就失效了。回 1 = 輪替成功；0 = 現值不符（不存在、已過期、或已被輪替——
// 後者可能是舊 token 被重放，呼叫端可據此撤銷整個 session）。
var tokenRotateScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
	return 1
else
	return 0
end`)
