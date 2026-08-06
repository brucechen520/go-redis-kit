package rediskit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/brucechen520/go-redis-kit/rediskit"
)

func TestObtain_SucceedsWhenLockFree(t *testing.T) {
	_, client := newTestClient(t)

	lock, err := client.Locker().Obtain(context.Background(), "job:report", 10*time.Second)

	if err != nil {
		t.Fatalf("Obtain = %v, want nil", err)
	}
	if lock == nil {
		t.Fatal("Obtain 回傳 nil lock, want 持有憑證")
	}
}

func TestObtain_ReturnsNotObtainedWhenHeldByOther(t *testing.T) {
	_, client := newTestClient(t)
	locker := client.Locker()
	ctx := context.Background()
	_, _ = locker.Obtain(ctx, "job:report", 10*time.Second) // 別人先拿走

	_, err := locker.Obtain(ctx, "job:report", 10*time.Second)

	if !errors.Is(err, rediskit.ErrLockNotObtained) {
		t.Errorf("Obtain(已被持有) = %v, want ErrLockNotObtained", err)
	}
}

func TestRelease_FreesLockForNextObtainer(t *testing.T) {
	_, client := newTestClient(t)
	locker := client.Locker()
	ctx := context.Background()
	lock, _ := locker.Obtain(ctx, "job:report", 10*time.Second)

	if err := lock.Release(ctx); err != nil {
		t.Fatalf("Release = %v, want nil", err)
	}

	if _, err := locker.Obtain(ctx, "job:report", 10*time.Second); err != nil {
		t.Errorf("Release 後重新 Obtain = %v, want nil", err)
	}
}

func TestRelease_ReturnsLockLostAfterExpiry(t *testing.T) {
	mr, client := newTestClient(t)
	ctx := context.Background()
	lock, _ := client.Locker().Obtain(ctx, "job:report", 5*time.Second)

	mr.FastForward(6 * time.Second) // TTL 過了，鎖已自動釋放

	if err := lock.Release(ctx); !errors.Is(err, rediskit.ErrLockLost) {
		t.Errorf("Release(過期鎖) = %v, want ErrLockLost", err)
	}
}

// 過期後別人拿到新鎖，舊持有者的 Release 不准刪掉它（Lua CAS 防線）。
func TestRelease_DoesNotFreeLockNowHeldByOther(t *testing.T) {
	mr, client := newTestClient(t)
	locker := client.Locker()
	ctx := context.Background()
	oldLock, _ := locker.Obtain(ctx, "job:report", 5*time.Second)
	mr.FastForward(6 * time.Second)
	_, _ = locker.Obtain(ctx, "job:report", 10*time.Second) // B 拿到新鎖

	_ = oldLock.Release(ctx) // A 遲來的釋放

	// B 的鎖必須還在：第三者 Obtain 仍該失敗
	if _, err := locker.Obtain(ctx, "job:report", 10*time.Second); !errors.Is(err, rediskit.ErrLockNotObtained) {
		t.Errorf("舊持有者 Release 後 Obtain = %v, want ErrLockNotObtained（B 的鎖不該被 A 刪掉）", err)
	}
}

func TestRefresh_ExtendsLockLifetime(t *testing.T) {
	mr, client := newTestClient(t)
	locker := client.Locker()
	ctx := context.Background()
	lock, _ := locker.Obtain(ctx, "job:report", 10*time.Second)

	mr.FastForward(6 * time.Second) // 沒續的話再 6s 就過期
	if err := lock.Refresh(ctx); err != nil {
		t.Fatalf("Refresh = %v, want nil", err)
	}
	mr.FastForward(9 * time.Second) // 未續命總計 15s 早就過期；續過的話 TTL 重設 10s 還剩 1s

	if _, err := locker.Obtain(ctx, "job:report", 10*time.Second); !errors.Is(err, rediskit.ErrLockNotObtained) {
		t.Errorf("Refresh 後 9s Obtain = %v, want ErrLockNotObtained（鎖應仍被持有）", err)
	}
}

func TestRefresh_ReturnsLockLostAfterExpiry(t *testing.T) {
	mr, client := newTestClient(t)
	ctx := context.Background()
	lock, _ := client.Locker().Obtain(ctx, "job:report", 5*time.Second)

	mr.FastForward(6 * time.Second)

	if err := lock.Refresh(ctx); !errors.Is(err, rediskit.ErrLockLost) {
		t.Errorf("Refresh(過期鎖) = %v, want ErrLockLost", err)
	}
}

func TestObtainWithFence_IssuesMonotonicallyIncreasingFences(t *testing.T) {
	_, client := newTestClient(t)
	locker := client.Locker()
	ctx := context.Background()

	lock1, err := locker.ObtainWithFence(ctx, "job:report", 10*time.Second)
	if err != nil {
		t.Fatalf("第一次 ObtainWithFence = %v, want nil", err)
	}
	_ = lock1.Release(ctx)
	lock2, err := locker.ObtainWithFence(ctx, "job:report", 10*time.Second)
	if err != nil {
		t.Fatalf("第二次 ObtainWithFence = %v, want nil", err)
	}

	if lock2.Fence() <= lock1.Fence() {
		t.Errorf("fence 序列 = %d 之後 %d, want 嚴格遞增", lock1.Fence(), lock2.Fence())
	}
}
