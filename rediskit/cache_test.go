package rediskit_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twteam/go-redis-kit/rediskit"
)

func TestCacheSetGet_RoundTripsStruct(t *testing.T) {
	_, client := newTestClient(t)
	cache := client.Cache()
	ctx := context.Background()
	want := user{ID: "1", Name: "Ada", Age: 36}

	if err := cache.Set(ctx, "user:1", want, time.Minute); err != nil {
		t.Fatalf("Set 回傳非預期錯誤: %v", err)
	}
	var got user
	if err := cache.Get(ctx, "user:1", &got); err != nil {
		t.Fatalf("Get 回傳非預期錯誤: %v", err)
	}

	if got != want {
		t.Errorf("Get = %+v, want %+v", got, want)
	}
}

func TestCacheGet_ReturnsCacheMissForUnknownKey(t *testing.T) {
	_, client := newTestClient(t)

	var got user
	err := client.Cache().Get(context.Background(), "user:nope", &got)

	if !errors.Is(err, rediskit.ErrCacheMiss) {
		t.Errorf("Get(未知 key) = %v, want ErrCacheMiss", err)
	}
}

func TestCacheSet_StoresKeyUnderNamespace(t *testing.T) {
	mr, client := newTestClient(t, rediskit.WithNamespace("app"))

	_ = client.Cache().Set(context.Background(), "user:1", "v", time.Minute)

	if !mr.Exists("app:user:1") {
		t.Errorf("Set(\"user:1\") 後 miniredis keys = %v, want 含 \"app:user:1\"", mr.Keys())
	}
}

// 抖動範圍 [90%, 110%)，實際 TTL 必須落在裡面——太短提早過期、沒抖動會雪崩。
func TestCacheSet_AppliesBoundedTTLJitter(t *testing.T) {
	mr, client := newTestClient(t)

	_ = client.Cache().Set(context.Background(), "user:1", "v", 100*time.Second)

	got := mr.TTL("user:1")
	if got < 90*time.Second || got >= 110*time.Second {
		t.Errorf("TTL = %v, want [90s, 110s)", got)
	}
}

func TestCacheGet_ReturnsCacheMissAfterTTLExpires(t *testing.T) {
	mr, client := newTestClient(t)
	cache := client.Cache()
	ctx := context.Background()
	_ = cache.Set(ctx, "user:1", "v", time.Minute)

	// 抖動上限 110% → 快轉 67s 保證所有可能的 TTL 都已過期
	mr.FastForward(67 * time.Second)

	var got string
	err := cache.Get(ctx, "user:1", &got)
	if !errors.Is(err, rediskit.ErrCacheMiss) {
		t.Errorf("Get(過期 key) = %v, want ErrCacheMiss", err)
	}
}

func TestCacheDelete_RemovesKey(t *testing.T) {
	_, client := newTestClient(t)
	cache := client.Cache()
	ctx := context.Background()
	_ = cache.Set(ctx, "user:1", "v", time.Minute)

	if err := cache.Delete(ctx, "user:1"); err != nil {
		t.Fatalf("Delete 回傳非預期錯誤: %v", err)
	}

	var got string
	if err := cache.Get(ctx, "user:1", &got); !errors.Is(err, rediskit.ErrCacheMiss) {
		t.Errorf("Get(已刪 key) = %v, want ErrCacheMiss", err)
	}
}

func TestGetOrLoad_CallsLoaderOnMissAndReturnsValue(t *testing.T) {
	_, client := newTestClient(t)
	calls := 0
	loader := func(context.Context) (user, error) {
		calls++
		return user{ID: "1", Name: "Ada"}, nil
	}

	got, err := rediskit.GetOrLoad(context.Background(), client.Cache(), "user:1", time.Minute, loader)

	if err != nil {
		t.Fatalf("GetOrLoad 回傳非預期錯誤: %v", err)
	}
	if want := (user{ID: "1", Name: "Ada"}); got != want {
		t.Errorf("GetOrLoad = %+v, want %+v", got, want)
	}
	if calls != 1 {
		t.Errorf("loader 被呼叫 %d 次, want 1", calls)
	}
}

func TestGetOrLoad_SkipsLoaderOnHit(t *testing.T) {
	_, client := newTestClient(t)
	ctx := context.Background()
	calls := 0
	loader := func(context.Context) (user, error) {
		calls++
		return user{ID: "1", Name: "Ada"}, nil
	}
	_, _ = rediskit.GetOrLoad(ctx, client.Cache(), "user:1", time.Minute, loader) // 第一次回填

	_, err := rediskit.GetOrLoad(ctx, client.Cache(), "user:1", time.Minute, loader)

	if err != nil {
		t.Fatalf("GetOrLoad 第二次回傳非預期錯誤: %v", err)
	}
	if calls != 1 {
		t.Errorf("loader 被呼叫 %d 次, want 1（第二次應命中快取）", calls)
	}
}

func TestGetOrLoad_PropagatesLoaderError(t *testing.T) {
	_, client := newTestClient(t)
	wantErr := errors.New("db down")
	loader := func(context.Context) (user, error) { return user{}, wantErr }

	_, err := rediskit.GetOrLoad(context.Background(), client.Cache(), "user:1", time.Minute, loader)

	if !errors.Is(err, wantErr) {
		t.Errorf("GetOrLoad = %v, want %v", err, wantErr)
	}
}

// 擊穿防線：N 個併發 miss 同一 key，loader 只准跑一次（singleflight）。
func TestGetOrLoad_MergesConcurrentLoadsIntoOne(t *testing.T) {
	_, client := newTestClient(t)
	cache := client.Cache()

	const n = 50
	var calls atomic.Int64
	gate := make(chan struct{}) // 擋住 loader，確保所有人擠進同一個 flight
	loader := func(context.Context) (user, error) {
		calls.Add(1)
		<-gate
		return user{ID: "1", Name: "Ada"}, nil
	}

	var started, done sync.WaitGroup
	errs := make([]error, n)
	vals := make([]user, n)
	for i := range n {
		started.Add(1)
		done.Add(1)
		go func() {
			started.Done() // 就位
			defer done.Done()
			vals[i], errs[i] = rediskit.GetOrLoad(context.Background(), cache, "user:1", time.Minute, loader)
		}()
	}
	started.Wait() // 全員就位後才放行 loader
	close(gate)
	done.Wait()

	for i := range n {
		if errs[i] != nil {
			t.Fatalf("goroutine %d 回傳非預期錯誤: %v", i, errs[i])
		}
		if want := (user{ID: "1", Name: "Ada"}); vals[i] != want {
			t.Errorf("goroutine %d 拿到 %+v, want %+v", i, vals[i], want)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("loader 被呼叫 %d 次, want 1（singleflight 應合併）", got)
	}
}

// 呼叫端取消只影響自己：等待者拿 ErrCanceled 走人，flight 不陪葬。
func TestGetOrLoad_ReturnsCanceledWhenCallerGivesUp(t *testing.T) {
	_, client := newTestClient(t)

	entered := make(chan struct{})
	gate := make(chan struct{})
	t.Cleanup(func() { close(gate) }) // 收尾放行，別漏 goroutine
	loader := func(context.Context) (user, error) {
		close(entered)
		<-gate
		return user{ID: "1"}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := rediskit.GetOrLoad(ctx, client.Cache(), "user:1", time.Minute, loader)
		errCh <- err
	}()

	<-entered // loader 已在跑（flight 開著）
	cancel()  // 呼叫端不等了

	select {
	case err := <-errCh:
		if !errors.Is(err, rediskit.ErrCanceled) {
			t.Errorf("GetOrLoad(取消的 ctx) = %v, want ErrCanceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("取消後 1s 內 GetOrLoad 仍未返回, want 立即返回 ErrCanceled")
	}
}
