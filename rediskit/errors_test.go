package rediskit_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/twteam/go-redis-kit/rediskit"
)

func TestMapError_ReturnsNilForNil(t *testing.T) {
	got := rediskit.MapError(nil)

	if got != nil {
		t.Errorf("MapError(nil) = %v, want nil", got)
	}
}

func TestMapError_MapsRedisNilToCacheMiss(t *testing.T) {
	got := rediskit.MapError(redis.Nil)

	if !errors.Is(got, rediskit.ErrCacheMiss) {
		t.Errorf("MapError(redis.Nil) = %v, want ErrCacheMiss", got)
	}
}

// 業務碼不該看得到傳輸層細節，映射後 redis.Nil 必須斷鏈。
func TestMapError_HidesRedisNilFromResult(t *testing.T) {
	got := rediskit.MapError(redis.Nil)

	if errors.Is(got, redis.Nil) {
		t.Errorf("MapError(redis.Nil) = %v, 仍可 errors.Is 到 redis.Nil, want 已隱藏", got)
	}
}

func TestMapNotFound_MapsRedisNilToCallerSentinel(t *testing.T) {
	got := rediskit.MapNotFound(redis.Nil, rediskit.ErrTokenNotFound)

	if !errors.Is(got, rediskit.ErrTokenNotFound) {
		t.Errorf("MapNotFound(redis.Nil, ErrTokenNotFound) = %v, want ErrTokenNotFound", got)
	}
}

func TestMapError_MapsDeadlineExceededToTimeout(t *testing.T) {
	got := rediskit.MapError(context.DeadlineExceeded)

	if !errors.Is(got, rediskit.ErrTimeout) {
		t.Errorf("MapError(context.DeadlineExceeded) = %v, want ErrTimeout", got)
	}
}

func TestMapError_MapsCanceledToCanceled(t *testing.T) {
	got := rediskit.MapError(context.Canceled)

	if !errors.Is(got, rediskit.ErrCanceled) {
		t.Errorf("MapError(context.Canceled) = %v, want ErrCanceled", got)
	}
}

// 取消不是逾時。混在一起會讓 client 斷線灌爆逾時指標。
func TestMapError_DoesNotReportCanceledAsTimeout(t *testing.T) {
	got := rediskit.MapError(context.Canceled)

	if errors.Is(got, rediskit.ErrTimeout) {
		t.Errorf("MapError(context.Canceled) = %v, 同時符合 ErrTimeout, want 只符合 ErrCanceled", got)
	}
}

func TestMapError_DoesNotReportDeadlineExceededAsCanceled(t *testing.T) {
	got := rediskit.MapError(context.DeadlineExceeded)

	if errors.Is(got, rediskit.ErrCanceled) {
		t.Errorf("MapError(context.DeadlineExceeded) = %v, 同時符合 ErrCanceled, want 只符合 ErrTimeout", got)
	}
}

// 映射不能吃掉原因，不然線上只看得到「timeout」，查不出是 ctx 到期還是網路。
func TestMapError_KeepsCauseReachableAfterMapping(t *testing.T) {
	got := rediskit.MapError(context.DeadlineExceeded)

	if !errors.Is(got, context.DeadlineExceeded) {
		t.Errorf("MapError(context.DeadlineExceeded) = %v, 原因已遺失, want 仍可 errors.Is 到 context.DeadlineExceeded", got)
	}
}

func TestMapError_MapsNetworkTimeoutToTimeout(t *testing.T) {
	got := rediskit.MapError(os.ErrDeadlineExceeded)

	if !errors.Is(got, rediskit.ErrTimeout) {
		t.Errorf("MapError(os.ErrDeadlineExceeded) = %v, want ErrTimeout", got)
	}
}

// go-redis 的 read timeout 是包在 *net.OpError 裡回來的，映射要能穿過包裝。
func TestMapError_MapsWrappedNetworkTimeoutToTimeout(t *testing.T) {
	wrapped := fmt.Errorf("redis: read tcp: %w", os.ErrDeadlineExceeded)

	got := rediskit.MapError(wrapped)

	if !errors.Is(got, rediskit.ErrTimeout) {
		t.Errorf("MapError(wrapped net timeout) = %v, want ErrTimeout", got)
	}
}

func TestMapError_MapsClientClosedToErrClosed(t *testing.T) {
	got := rediskit.MapError(redis.ErrClosed)

	if !errors.Is(got, rediskit.ErrClosed) {
		t.Errorf("MapError(redis.ErrClosed) = %v, want ErrClosed", got)
	}
}

func TestMapError_PassesThroughUnrecognizedError(t *testing.T) {
	serverErr := errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

	got := rediskit.MapError(serverErr)

	if !errors.Is(got, serverErr) {
		t.Errorf("MapError(server error) = %v, want %v", got, serverErr)
	}
}
