package rediskit_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twteam/go-redis-kit/rediskit"
)

// fakeClock 是可手動快轉的時間來源。限流的令牌補充以它為準，
// 測試不必 sleep 就能驗「時間過了、額度回來了」。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	// 固定起點，測試決定性
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func TestAllow_PermitsUpToLimitWithinWindow(t *testing.T) {
	_, client := newTestClient(t, rediskit.WithTimeSource(newFakeClock().Now))
	rl := client.RateLimiter(3, time.Second)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		if err := rl.Allow(ctx, "login:user:1"); err != nil {
			t.Fatalf("第 %d 次 Allow = %v, want nil（額度內）", i, err)
		}
	}
}

func TestAllow_RejectsWhenBucketEmpty(t *testing.T) {
	_, client := newTestClient(t, rediskit.WithTimeSource(newFakeClock().Now))
	rl := client.RateLimiter(3, time.Second)
	ctx := context.Background()
	for range 3 {
		_ = rl.Allow(ctx, "login:user:1") // 額度用光
	}

	err := rl.Allow(ctx, "login:user:1")

	if !errors.Is(err, rediskit.ErrRateLimited) {
		t.Errorf("第 4 次 Allow = %v, want ErrRateLimited", err)
	}
}

func TestAllow_RefillsAfterTimePasses(t *testing.T) {
	clock := newFakeClock()
	_, client := newTestClient(t, rediskit.WithTimeSource(clock.Now))
	rl := client.RateLimiter(3, time.Second)
	ctx := context.Background()
	for range 3 {
		_ = rl.Allow(ctx, "login:user:1")
	}

	clock.Advance(time.Second) // 一個完整週期 → 桶補滿

	if err := rl.Allow(ctx, "login:user:1"); err != nil {
		t.Errorf("補滿後 Allow = %v, want nil", err)
	}
}

// 令牌桶是漸進補充：過 1/3 週期只回 1 個令牌，不是整窗重置。
func TestAllow_RefillsGraduallyNotPerWindow(t *testing.T) {
	clock := newFakeClock()
	_, client := newTestClient(t, rediskit.WithTimeSource(clock.Now))
	rl := client.RateLimiter(3, time.Second)
	ctx := context.Background()
	for range 3 {
		_ = rl.Allow(ctx, "login:user:1")
	}

	// 340ms × 3 令牌/s = 1.02 個 → 剛好補回 1 個
	// （不用 time.Second/3：毫秒截斷成 333ms × 3 = 0.999 個，差一毫令牌補不滿）
	clock.Advance(340 * time.Millisecond)

	if err := rl.Allow(ctx, "login:user:1"); err != nil {
		t.Fatalf("補 1 個後第 1 次 Allow = %v, want nil", err)
	}
	if err := rl.Allow(ctx, "login:user:1"); !errors.Is(err, rediskit.ErrRateLimited) {
		t.Errorf("補 1 個後第 2 次 Allow = %v, want ErrRateLimited", err)
	}
}

func TestAllow_TracksKeysIndependently(t *testing.T) {
	_, client := newTestClient(t, rediskit.WithTimeSource(newFakeClock().Now))
	rl := client.RateLimiter(3, time.Second)
	ctx := context.Background()
	for range 3 {
		_ = rl.Allow(ctx, "login:user:1") // user:1 用光
	}

	err := rl.Allow(ctx, "login:user:2")

	if err != nil {
		t.Errorf("user:2 首次 Allow = %v, want nil（各 key 額度獨立）", err)
	}
}

func TestAllowN_TakesAllOrNothing(t *testing.T) {
	_, client := newTestClient(t, rediskit.WithTimeSource(newFakeClock().Now))
	rl := client.RateLimiter(3, time.Second)
	ctx := context.Background()

	if err := rl.AllowN(ctx, "batch:1", 2); err != nil {
		t.Fatalf("AllowN(2) 桶剩 3 = %v, want nil", err)
	}
	// 剩 1 個，要 2 個 → 整批拒絕，且不能扣掉那 1 個
	if err := rl.AllowN(ctx, "batch:1", 2); !errors.Is(err, rediskit.ErrRateLimited) {
		t.Fatalf("AllowN(2) 桶剩 1 = %v, want ErrRateLimited", err)
	}
	if err := rl.AllowN(ctx, "batch:1", 1); err != nil {
		t.Errorf("AllowN(1) 桶剩 1 = %v, want nil（被拒的那次不該扣令牌）", err)
	}
}
