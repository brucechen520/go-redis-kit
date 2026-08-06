package rediskit_test

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/twteam/go-redis-kit/rediskit"
)

// newTestClient 起一個 miniredis + 連上去的 rediskit client。
// 額外的 opts 疊加在 addr 之後。
func newTestClient(t testing.TB, opts ...rediskit.Option) (*miniredis.Miniredis, *rediskit.Client) {
	t.Helper()
	mr := miniredis.RunT(t) // t.Cleanup 自動關

	client, err := rediskit.New(append([]rediskit.Option{rediskit.WithAddr(mr.Addr())}, opts...)...)
	if err != nil {
		t.Fatalf("New(%q) 回傳非預期錯誤: %v", mr.Addr(), err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

func TestPing_SucceedsAgainstRunningRedis(t *testing.T) {
	_, client := newTestClient(t)

	got := client.Ping(context.Background())

	if got != nil {
		t.Errorf("Ping() = %v, want nil", got)
	}
}

func TestPing_ReturnsErrClosedAfterClose(t *testing.T) {
	_, client := newTestClient(t)
	_ = client.Close()

	got := client.Ping(context.Background())

	if !errors.Is(got, rediskit.ErrClosed) {
		t.Errorf("Ping() after Close = %v, want ErrClosed", got)
	}
}

func TestRaw_ExposesUnderlyingClient(t *testing.T) {
	_, client := newTestClient(t)

	raw := client.Raw()

	if raw == nil {
		t.Fatal("Raw() = nil, want 底層 client")
	}
	// 逃生艙走的是同一個連線池：能直接下 rediskit 沒封的指令
	if err := raw.SetBit(context.Background(), "bloom", 42, 1).Err(); err != nil {
		t.Errorf("Raw().SetBit() = %v, want nil", err)
	}
}
