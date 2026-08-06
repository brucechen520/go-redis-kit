package rediskit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twteam/go-redis-kit/rediskit"
)

func TestTokenSaveLoad_RoundTrips(t *testing.T) {
	_, client := newTestClient(t)
	ts := client.TokenStore()
	ctx := context.Background()

	if err := ts.Save(ctx, "sess:abc", "refresh-xyz", time.Hour); err != nil {
		t.Fatalf("Save 回傳非預期錯誤: %v", err)
	}
	got, err := ts.Load(ctx, "sess:abc")

	if err != nil {
		t.Fatalf("Load 回傳非預期錯誤: %v", err)
	}
	if want := "refresh-xyz"; got != want {
		t.Errorf("Load = %q, want %q", got, want)
	}
}

func TestTokenLoad_ReturnsNotFoundForUnknownKey(t *testing.T) {
	_, client := newTestClient(t)

	_, err := client.TokenStore().Load(context.Background(), "sess:nope")

	if !errors.Is(err, rediskit.ErrTokenNotFound) {
		t.Errorf("Load(未知 key) = %v, want ErrTokenNotFound", err)
	}
}

func TestTokenLoad_ReturnsNotFoundAfterTTLExpires(t *testing.T) {
	mr, client := newTestClient(t)
	ts := client.TokenStore()
	ctx := context.Background()
	_ = ts.Save(ctx, "sess:abc", "refresh-xyz", time.Hour)

	mr.FastForward(time.Hour + time.Second)

	if _, err := ts.Load(ctx, "sess:abc"); !errors.Is(err, rediskit.ErrTokenNotFound) {
		t.Errorf("Load(過期 token) = %v, want ErrTokenNotFound", err)
	}
}

func TestTokenSave_RejectsNonPositiveTTL(t *testing.T) {
	_, client := newTestClient(t)

	err := client.TokenStore().Save(context.Background(), "sess:abc", "tok", 0)

	if err == nil {
		t.Error("Save(ttl=0) = nil, want 錯誤（永不過期的憑證是安全事故）")
	}
}

func TestTokenRotate_SwapsValueWhenOldTokenMatches(t *testing.T) {
	_, client := newTestClient(t)
	ts := client.TokenStore()
	ctx := context.Background()
	_ = ts.Save(ctx, "sess:abc", "old-tok", time.Hour)

	if err := ts.Rotate(ctx, "sess:abc", "old-tok", "new-tok", time.Hour); err != nil {
		t.Fatalf("Rotate 回傳非預期錯誤: %v", err)
	}

	got, _ := ts.Load(ctx, "sess:abc")
	if want := "new-tok"; got != want {
		t.Errorf("Rotate 後 Load = %q, want %q", got, want)
	}
}

// 重放場景：舊 token 已被輪替過，再拿來 Rotate 必須失敗且不動現值。
func TestTokenRotate_RejectsStaleOldToken(t *testing.T) {
	_, client := newTestClient(t)
	ts := client.TokenStore()
	ctx := context.Background()
	_ = ts.Save(ctx, "sess:abc", "old-tok", time.Hour)
	_ = ts.Rotate(ctx, "sess:abc", "old-tok", "new-tok", time.Hour) // 正常輪替一次

	err := ts.Rotate(ctx, "sess:abc", "old-tok", "evil-tok", time.Hour) // 舊 token 重放

	if !errors.Is(err, rediskit.ErrTokenNotFound) {
		t.Fatalf("Rotate(重放的舊 token) = %v, want ErrTokenNotFound", err)
	}
	got, _ := ts.Load(ctx, "sess:abc")
	if want := "new-tok"; got != want {
		t.Errorf("重放失敗後 Load = %q, want %q（現值不得被動到）", got, want)
	}
}

func TestTokenRevoke_RemovesToken(t *testing.T) {
	_, client := newTestClient(t)
	ts := client.TokenStore()
	ctx := context.Background()
	_ = ts.Save(ctx, "sess:abc", "tok", time.Hour)

	if err := ts.Revoke(ctx, "sess:abc"); err != nil {
		t.Fatalf("Revoke 回傳非預期錯誤: %v", err)
	}

	if _, err := ts.Load(ctx, "sess:abc"); !errors.Is(err, rediskit.ErrTokenNotFound) {
		t.Errorf("Revoke 後 Load = %v, want ErrTokenNotFound", err)
	}
}

func TestTokenRevoke_IsIdempotent(t *testing.T) {
	_, client := newTestClient(t)

	err := client.TokenStore().Revoke(context.Background(), "sess:never-existed")

	if err != nil {
		t.Errorf("Revoke(不存在的 key) = %v, want nil（冪等）", err)
	}
}
