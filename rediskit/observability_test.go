package rediskit_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twteam/go-redis-kit/rediskit"
)

// fakeRecorder 只「記」狀態，斷言全部拉回 test body 主線做。
type fakeRecorder struct {
	mu        sync.Mutex
	latencies map[string]int // cmd → 觀測次數
	errors    map[string]int
	hits      int
	misses    int
}

func newFakeRecorder() *fakeRecorder {
	return &fakeRecorder{latencies: map[string]int{}, errors: map[string]int{}}
}

func (f *fakeRecorder) ObserveLatency(cmd string, _ time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latencies[cmd]++
}

func (f *fakeRecorder) IncError(cmd string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors[cmd]++
}

func (f *fakeRecorder) IncHit() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits++
}

func (f *fakeRecorder) IncMiss() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.misses++
}

func (f *fakeRecorder) snapshot() (hits, misses, getLatencies, getErrors int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits, f.misses, f.latencies["get"], f.errors["get"]
}

func TestMetrics_CountsHitAndMiss(t *testing.T) {
	rec := newFakeRecorder()
	_, client := newTestClient(t, rediskit.WithMetrics(rec))
	cache := client.Cache()
	ctx := context.Background()
	_ = cache.Set(ctx, "user:1", "v", time.Minute)

	var v string
	_ = cache.Get(ctx, "user:1", &v)    // hit
	_ = cache.Get(ctx, "user:nope", &v) // miss

	hits, misses, _, _ := rec.snapshot()
	if hits != 1 {
		t.Errorf("hits = %d, want 1", hits)
	}
	if misses != 1 {
		t.Errorf("misses = %d, want 1", misses)
	}
}

func TestMetrics_ObservesCommandLatency(t *testing.T) {
	rec := newFakeRecorder()
	_, client := newTestClient(t, rediskit.WithMetrics(rec))

	var v string
	_ = client.Cache().Get(context.Background(), "user:1", &v)

	_, _, getLatencies, _ := rec.snapshot()
	if getLatencies != 1 {
		t.Errorf("GET 延遲觀測次數 = %d, want 1", getLatencies)
	}
}

// redis.Nil（key 不存在）是預期結果，算進錯誤率會污染告警。
func TestMetrics_DoesNotCountMissAsError(t *testing.T) {
	rec := newFakeRecorder()
	_, client := newTestClient(t, rediskit.WithMetrics(rec))

	var v string
	_ = client.Cache().Get(context.Background(), "user:nope", &v) // miss

	_, _, _, getErrors := rec.snapshot()
	if getErrors != 0 {
		t.Errorf("GET 錯誤數 = %d, want 0（miss 不是錯誤）", getErrors)
	}
}
