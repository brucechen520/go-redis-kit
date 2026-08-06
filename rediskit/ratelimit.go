package rediskit

import (
	"context"
	"time"
)

// RateLimiter 是令牌桶限流器：每 per 時間補滿 limit 個令牌，桶容量 = limit
// （允許短時 burst 到 limit，之後按速率放行）。判斷 + 扣減走 Lua，原子。
//
// 選令牌桶不選其他三種（固定窗口/滑動窗口/漏桶）的理由：一段 Lua 同時控制
// 平均速率與 burst 上限，是四種裡覆蓋面最大的；有別的需求再加，別預先做全。
//
// 降級語意：Redis 掛掉時 Allow 回傳底層錯誤（不是 ErrRateLimited）。
// fail-open 還是 fail-close 是業務決定——登入防爆破該 close，一般 API 可 open——
// 所以 lib 不替你決定，呼叫端自己接。
type RateLimiter struct {
	c     *Client
	limit int
	per   time.Duration
}

// Allow 問「key 這個對象現在能不能過一次」。
// 放行回 nil；超限回 ErrRateLimited；其他錯誤 = Redis 有問題。
func (r *RateLimiter) Allow(ctx context.Context, key string) error {
	return r.AllowN(ctx, key, 1)
}

// AllowN 一次索取 n 個令牌（批量操作、加權成本用）。原子：要嘛全拿、要嘛不拿。
func (r *RateLimiter) AllowN(ctx context.Context, key string, n int) error {
	perMs := r.per.Milliseconds()
	if perMs <= 0 {
		perMs = 1
	}

	// 全用「毫令牌」尺度（1 令牌 = 1000），補充速率才不會被整數除法捨成 0。
	capM := float64(r.limit) * 1000
	rateM := capM / float64(perMs)          // 毫令牌 / ms
	nowMs := r.c.now().UnixMilli()          // 時間由 Go 端傳入，腳本保持決定性
	ttlMs := perMs * 2                      // 桶閒置超過兩個週期就回收

	res, err := rateLimitScript.Run(ctx, r.c.rdb,
		[]string{r.c.kb.Qualify("rl:" + key)},
		capM, rateM, nowMs, float64(n)*1000, ttlMs,
	).Int()
	if err != nil {
		return MapError(err)
	}
	if res == 0 {
		return ErrRateLimited
	}
	return nil
}
