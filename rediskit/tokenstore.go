package rediskit

import (
	"context"
	"time"
)

// TokenStore 儲存不透明 token（refresh token、session id…）。
//
// 刻意做薄：它只懂「帶 TTL 的字串 + 原子輪替（CAS）」，不懂 session、
// 不懂 JWT、不懂重放策略——那些是業務層（A 層）的判斷，照 docs/07 §2 的
// 邊界表不該下沉到 lib。
//
// 降級語意：認證是安全型，一律 fail-close——Redis 掛了就是驗不了，
// 別把「查不到」當成「可以放行」。
type TokenStore struct {
	c *Client
}

// Save 寫入 token，ttl 到期自動失效。ttl 必須 > 0：
// 永不過期的憑證是安全事故，不提供這個選項。
func (t *TokenStore) Save(ctx context.Context, key, token string, ttl time.Duration) error {
	if ttl <= 0 {
		return errTokenTTLRequired
	}
	return MapError(t.c.rdb.Set(ctx, t.c.kb.Qualify("token:"+key), token, ttl).Err())
}

// Load 讀出 token。不存在（從沒存過、已過期、已撤銷）回 ErrTokenNotFound，
// 呼叫端一律當「要求重新登入」處理。
func (t *TokenStore) Load(ctx context.Context, key string) (string, error) {
	v, err := t.c.rdb.Get(ctx, t.c.kb.Qualify("token:"+key)).Result()
	if err != nil {
		return "", MapNotFound(err, ErrTokenNotFound)
	}
	return v, nil
}

// Rotate 原子輪替：現值 == oldToken 才寫入 newToken（舊失效 + 新生效一步完成）。
//
// 現值不符回 ErrTokenNotFound。「不符」有兩種可能：token 已過期，或
// 已被輪替過——後者代表舊 token 被重放（可能外洩），呼叫端收到這個錯誤時
// 保守的做法是撤銷整個 session 強制重新登入。
func (t *TokenStore) Rotate(ctx context.Context, key, oldToken, newToken string, ttl time.Duration) error {
	if ttl <= 0 {
		return errTokenTTLRequired
	}
	res, err := tokenRotateScript.Run(ctx, t.c.rdb,
		[]string{t.c.kb.Qualify("token:" + key)},
		oldToken, newToken, ttl.Milliseconds(),
	).Int()
	if err != nil {
		return MapError(err)
	}
	if res == 0 {
		return ErrTokenNotFound
	}
	return nil
}

// Revoke 撤銷 token（登出、強制下線）。token 本來就不存在也算成功——
// 撤銷要的是「之後查不到」這個狀態，冪等。
func (t *TokenStore) Revoke(ctx context.Context, keys ...string) error {
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = t.c.kb.Qualify("token:" + k)
	}
	return MapError(t.c.rdb.Del(ctx, full...).Err())
}
