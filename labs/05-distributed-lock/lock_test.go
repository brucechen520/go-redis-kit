package main

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestClient 起一個純記憶體 miniredis（免 docker），回傳連上它的 client 與 server（用來快轉時間）。
func newTestClient(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t) // t 結束自動關閉
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb, mr
}

// 互斥：A 拿到鎖後，B 拿不到；A 釋放後 C 才拿得到。
func TestAcquireMutex(t *testing.T) {
	ctx := context.Background()
	rdb, _ := newTestClient(t)
	const key = "lock:k"

	a, ok, err := Acquire(ctx, rdb, key, time.Minute)
	if err != nil || !ok {
		t.Fatalf("A 應取得鎖, got ok=%v err=%v", ok, err)
	}

	if _, ok, err := Acquire(ctx, rdb, key, time.Minute); err != nil || ok {
		t.Fatalf("B 不該取得鎖(已被 A 持有), got ok=%v err=%v", ok, err)
	}

	if deleted, err := a.Release(ctx); err != nil || !deleted {
		t.Fatalf("A 釋放應成功, got deleted=%v err=%v", deleted, err)
	}

	if _, ok, err := Acquire(ctx, rdb, key, time.Minute); err != nil || !ok {
		t.Fatalf("A 釋放後 C 應取得鎖, got ok=%v err=%v", ok, err)
	}
}

// holder-safe：拿錯 token 的 Release 不該刪掉鎖（Lua 比對擋掉）。
func TestReleaseWrongTokenNoop(t *testing.T) {
	ctx := context.Background()
	rdb, _ := newTestClient(t)
	const key = "lock:k"

	a, ok, err := Acquire(ctx, rdb, key, time.Minute)
	if err != nil || !ok {
		t.Fatalf("A 應取得鎖")
	}

	fake := &Lock{rdb: rdb, key: key, token: "wrong-token", ttl: time.Minute}
	deleted, err := fake.Release(ctx)
	if err != nil {
		t.Fatalf("Release err: %v", err)
	}
	if deleted {
		t.Fatal("錯 token 不該刪掉鎖")
	}

	// 鎖仍在, 且 token 還是 A 的
	got, err := rdb.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("鎖應仍存在: %v", err)
	}
	if got != a.Token() {
		t.Fatalf("token 應仍是 A 的, got %s want %s", got, a.Token())
	}
}

// 核心競態：A 的鎖過期被 B 拿走後，A 遲來的 Release 不能刪掉 B 的鎖。
func TestReleaseAfterExpiryDoesNotDeleteOthersLock(t *testing.T) {
	ctx := context.Background()
	rdb, mr := newTestClient(t)
	const key = "lock:k"

	a, ok, err := Acquire(ctx, rdb, key, 100*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("A 應取得鎖")
	}

	mr.FastForward(200 * time.Millisecond) // A 的鎖過期

	b, ok, err := Acquire(ctx, rdb, key, time.Minute)
	if err != nil || !ok {
		t.Fatalf("A 過期後 B 應取得鎖, got ok=%v err=%v", ok, err)
	}

	// A 現在才來釋放 —— 不能刪到 B 的鎖
	deleted, err := a.Release(ctx)
	if err != nil {
		t.Fatalf("Release err: %v", err)
	}
	if deleted {
		t.Fatal("A 遲到的 Release 刪掉了 B 的鎖 —— 這正是 Lua 比對要防的災難")
	}

	got, err := rdb.Get(ctx, key).Result()
	if err != nil || got != b.Token() {
		t.Fatalf("B 的鎖應完好, got %q err %v", got, err)
	}
}

// Refresh：只有持有者能續命；鎖過期後續命失敗。
func TestRefresh(t *testing.T) {
	ctx := context.Background()
	rdb, mr := newTestClient(t)
	const key = "lock:k"

	a, ok, err := Acquire(ctx, rdb, key, 100*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("A 應取得鎖")
	}

	// 快轉 80ms(未過期), 續命成功並把 TTL 拉回 100ms
	mr.FastForward(80 * time.Millisecond)
	if extended, err := a.Refresh(ctx); err != nil || !extended {
		t.Fatalf("持有者續命應成功, got %v err %v", extended, err)
	}

	// 續命後 TTL 應接近 100ms(> 剩餘的原始 TTL)
	ttl, err := rdb.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("PTTL err: %v", err)
	}
	if ttl <= 20*time.Millisecond {
		t.Fatalf("續命後 TTL 應被拉長, got %v", ttl)
	}

	// 錯 token 續命失敗
	fake := &Lock{rdb: rdb, key: key, token: "wrong", ttl: 100 * time.Millisecond}
	if extended, err := fake.Refresh(ctx); err != nil || extended {
		t.Fatalf("非持有者不該續命成功, got %v err %v", extended, err)
	}

	// 鎖真的過期後, 持有者也續不了
	mr.FastForward(200 * time.Millisecond)
	if extended, err := a.Refresh(ctx); err != nil || extended {
		t.Fatalf("鎖已過期不該續命成功, got %v err %v", extended, err)
	}
}

// fencing token：每次成功取鎖拿到嚴格遞增的序號。
func TestFencingTokenMonotonic(t *testing.T) {
	ctx := context.Background()
	rdb, _ := newTestClient(t)
	const key, fence = "lock:k", "fence:k"

	var prev int64
	for i := 0; i < 5; i++ {
		lock, f, ok, err := AcquireWithFence(ctx, rdb, key, fence, time.Minute)
		if err != nil || !ok {
			t.Fatalf("第 %d 次應取得鎖, got ok=%v err=%v", i, ok, err)
		}
		if f <= prev {
			t.Fatalf("fence 應嚴格遞增: prev=%d got=%d", prev, f)
		}
		prev = f
		if _, err := lock.Release(ctx); err != nil {
			t.Fatalf("釋放失敗: %v", err)
		}
	}
}
