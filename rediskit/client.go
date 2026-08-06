package rediskit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client 是 rediskit 的唯一入口，也是整個 lib 裡唯一「碰得到」go-redis 的公開型別。
// 業務型別（Cache / Locker / RateLimiter / TokenStore）都從它生出來，
// 內部共享同一個連線池、序列化器、KeyBuilder 與指標記錄器。
type Client struct {
	rdb   redis.UniversalClient
	kb    KeyBuilder
	ser   Serializer
	rec   MetricsRecorder
	now   func() time.Time
	cache *Cache
}

// New 建立 Client。連線是惰性的：這裡不會真的碰網路，
// 要驗證連得上請呼叫 Ping。
func New(opts ...Option) (*Client, error) {
	o := defaultOptions()
	for _, fn := range opts {
		fn(&o)
	}

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        []string{o.addr},
		Password:     o.password,
		DB:           o.db,
		PoolSize:     o.poolSize,
		MinIdleConns: o.minIdle,
		PoolTimeout:  o.poolTimeout,
		DialTimeout:  o.dialTO,
		ReadTimeout:  o.readTO,
		WriteTimeout: o.writeTO,
		MaxRetries:   o.maxRetries,
	})

	// 觀測掛在 Hook 鏈上，之後所有指令（含 Lua）自動被記錄，業務碼零感知。
	if _, isNop := o.metrics.(nopRecorder); !isNop {
		rdb.AddHook(metricsHook{rec: o.metrics})
	}

	c := &Client{
		rdb: rdb,
		kb:  NewKeyBuilder(o.namespace),
		ser: o.serializer,
		rec: o.metrics,
		now: o.now,
	}
	c.cache = newCache(c, o.loadTimeout)
	return c, nil
}

// Ping 驗證連線可用。建議在服務啟動時呼叫一次 fail fast。
func (c *Client) Ping(ctx context.Context) error {
	return MapError(c.rdb.Ping(ctx).Err())
}

// Close 關閉連線池。之後所有操作回 ErrClosed。
func (c *Client) Close() error {
	return MapError(c.rdb.Close())
}

// Cache 回傳共享的 Cache 實例。
// 共享是刻意的：singleflight 的合併範圍 = 一個 Cache 實例，
// 每次都拿到同一個才能把全 process 的併發回源合併起來。
func (c *Client) Cache() *Cache { return c.cache }

// Locker 回傳分散式鎖的入口。
func (c *Client) Locker() *Locker { return &Locker{c: c} }

// RateLimiter 建一個限流器：每 per 時間最多 limit 次（令牌桶，容量 = limit，
// 允許短時 burst 到 limit）。不同用途（登入、寄信…）各建各的，用 key 區分對象。
func (c *Client) RateLimiter(limit int, per time.Duration) *RateLimiter {
	return &RateLimiter{c: c, limit: limit, per: per}
}

// TokenStore 回傳 token 儲存的入口。
func (c *Client) TokenStore() *TokenStore { return &TokenStore{c: c} }
