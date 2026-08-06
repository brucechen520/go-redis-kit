package rediskit

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"golang.org/x/sync/singleflight"
)

// Cache 提供 cache-aside 的意圖 API。
// 從 Client.Cache() 取得；全 process 共用一個實例，singleflight 才有合併效果。
type Cache struct {
	c           *Client
	sf          singleflight.Group
	loadTimeout time.Duration
}

func newCache(c *Client, loadTimeout time.Duration) *Cache {
	return &Cache{c: c, loadTimeout: loadTimeout}
}

// Get 讀取 key 並反序列化進 dst。key 不存在回 ErrCacheMiss。
func (c *Cache) Get(ctx context.Context, key string, dst any) error {
	return c.get(ctx, key, dst, true)
}

// get 是內部實作。countMetrics=false 供 GetOrLoad 的 flight 內重查使用，
// 避免同一個邏輯請求把 miss 記兩次、污染命中率。
func (c *Cache) get(ctx context.Context, key string, dst any, countMetrics bool) error {
	b, err := c.c.rdb.Get(ctx, c.c.kb.Qualify(key)).Bytes()
	if err != nil {
		err = MapError(err)
		if countMetrics {
			if errors.Is(err, ErrCacheMiss) {
				c.c.rec.IncMiss()
			}
		}
		return err
	}
	if countMetrics {
		c.c.rec.IncHit()
	}
	if err := c.c.ser.Unmarshal(b, dst); err != nil {
		return fmt.Errorf("rediskit: unmarshal %q: %w", key, err)
	}
	return nil
}

// Set 序列化 val 並寫入。ttl > 0 時會加上 ±10% 抖動，
// 打散大量 key 的到期時刻避免雪崩——這是「忘記做會出事」的橫切關注點，
// 所以收在 lib 內不給呼叫端忘記的機會。ttl <= 0 表示不過期。
func (c *Cache) Set(ctx context.Context, key string, val any, ttl time.Duration) error {
	b, err := c.c.ser.Marshal(val)
	if err != nil {
		return fmt.Errorf("rediskit: marshal %q: %w", key, err)
	}
	return MapError(c.c.rdb.Set(ctx, c.c.kb.Qualify(key), b, jitterTTL(ttl)).Err())
}

// Delete 刪除一或多把 key。key 本來就不存在不算錯誤。
func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = c.c.kb.Qualify(k)
	}
	return MapError(c.c.rdb.Del(ctx, full...).Err())
}

// GetOrLoad 是 cache-aside 的一次呼叫版：讀快取 → miss 就回源 → 回填。
// 內建 singleflight（同 key 併發回源只跑一次 loader）與 TTL 抖動。
//
// 是 package-level 泛型函式而非 Cache 的方法（Go 方法不能有型別參數）。
// 換得的是編譯期型別安全：loader 回傳型別接錯，編譯就擋下，
// 不必等 runtime 的 reflect 賦值炸掉。
//
// loader 的執行與任何單一呼叫端的壽命脫鉤（flight 的結果是所有等待者共享的，
// 不能因為第一個呼叫端斷線就害其他人陪葬），超時由 WithLoadTimeout 獨立控制；
// 呼叫端自己的 ctx 只決定「自己要等多久」。
//
// 回填失敗不會讓請求失敗（資料已經拿到了），但會記進 metrics——
// 靜默吞掉的話，Redis 掛了你只會看到命中率莫名下降。
func GetOrLoad[T any](ctx context.Context, c *Cache, key string, ttl time.Duration,
	loader func(context.Context) (T, error)) (T, error) {

	var zero T

	// 1. 先讀快取。
	var got T
	err := c.Get(ctx, key, &got)
	if err == nil {
		return got, nil
	}
	if !errors.Is(err, ErrCacheMiss) {
		return zero, err // Redis 真的壞了：回錯誤，降級（直讀 DB）是呼叫端的決定
	}

	// 2. miss → singleflight 合併回源。
	ch := c.sf.DoChan(c.c.kb.Qualify(key), func() (any, error) {
		// flight 內先重查一次：排在後面的 flight 可能剛好接在
		// 「前一輪已回填」之後，重查省掉一次 loader。
		var v T
		if err := c.get(ctx, key, &v, false); err == nil {
			return v, nil
		}

		// 回源用獨立壽命：WithoutCancel 切斷與呼叫端的取消連動，再配自己的超時。
		lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.loadTimeout)
		defer cancel()

		v, err := loader(lctx)
		if err != nil {
			return zero, err
		}
		if err := c.Set(lctx, key, v, ttl); err != nil {
			c.c.rec.IncError("cache_backfill") // 回填失敗：不擋請求，但要看得見
		}
		return v, nil
	})

	// 3. 用自己的 ctx 等結果：自己取消就自己先走，flight 照跑、結果照回填。
	select {
	case res := <-ch:
		if res.Err != nil {
			return zero, res.Err
		}
		v, ok := res.Val.(T)
		if !ok {
			// 同一 key 被不同型別的 GetOrLoad 併發呼叫才會走到這裡——是呼叫端 bug。
			return zero, fmt.Errorf("rediskit: GetOrLoad type mismatch for key %q: got %T", key, res.Val)
		}
		return v, nil
	case <-ctx.Done():
		return zero, MapError(ctx.Err())
	}
}

// jitterTTL 把 ttl 打散成 [90%, 110%) 的均勻分布。
func jitterTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	// 0.9 + [0, 0.2) → [0.9, 1.1)
	return time.Duration(float64(ttl) * (0.9 + rand.Float64()*0.2))
}
