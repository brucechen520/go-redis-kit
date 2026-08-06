package rediskit_test

// 量 lib 本身的開銷（序列化、singleflight、key 組裝、Lua 派發）。
// 底層是 miniredis（in-process），所以數字不含真實網路——
// 要量 Redis server + 網路的天花板用 redis-benchmark，兩邊對照找 lib 佔比
// （見 docs/07 §6.3）。重點指標：ns/op、B/op、allocs/op。

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/brucechen520/go-redis-kit/rediskit"
)

var benchUser = user{ID: "1", Name: "Ada", Age: 36}

func BenchmarkKeyBuilder_Build(b *testing.B) {
	kb := rediskit.NewKeyBuilder("app")

	b.ReportAllocs()
	for b.Loop() {
		_ = kb.Build("user", "12345")
	}
}

func BenchmarkCache_Get_Hit(b *testing.B) {
	_, client := newTestClient(b)
	cache := client.Cache()
	ctx := context.Background()
	if err := cache.Set(ctx, "user:1", benchUser, time.Hour); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		var u user
		_ = cache.Get(ctx, "user:1", &u)
	}
}

func BenchmarkCache_Set(b *testing.B) {
	_, client := newTestClient(b)
	cache := client.Cache()
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		_ = cache.Set(ctx, "user:1", benchUser, time.Hour)
	}
}

func BenchmarkGetOrLoad_Hit(b *testing.B) {
	_, client := newTestClient(b)
	cache := client.Cache()
	ctx := context.Background()
	loader := func(context.Context) (user, error) { return benchUser, nil }
	if _, err := rediskit.GetOrLoad(ctx, cache, "user:1", time.Hour, loader); err != nil {
		b.Fatal(err) // 先暖 key，之後全是 hit 路徑
	}

	b.ReportAllocs()
	for b.Loop() {
		_, _ = rediskit.GetOrLoad(ctx, cache, "user:1", time.Hour, loader)
	}
}

func BenchmarkGetOrLoad_Miss(b *testing.B) {
	_, client := newTestClient(b)
	cache := client.Cache()
	ctx := context.Background()
	loader := func(context.Context) (user, error) { return benchUser, nil }

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		i++ // 每輪新 key → 走完整 miss → singleflight → loader → 回填路徑
		_, _ = rediskit.GetOrLoad(ctx, cache, "user:"+strconv.Itoa(i), time.Hour, loader)
	}
}

func BenchmarkRateLimiter_Allow(b *testing.B) {
	_, client := newTestClient(b)
	rl := client.RateLimiter(1<<30, time.Second) // 額度大到打不完，量的是 Lua 派發成本
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		_ = rl.Allow(ctx, "bench")
	}
}

func BenchmarkLock_ObtainRelease(b *testing.B) {
	_, client := newTestClient(b)
	locker := client.Locker()
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		lock, err := locker.Obtain(ctx, "bench", time.Minute)
		if err != nil {
			b.Fatal(err)
		}
		if err := lock.Release(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
