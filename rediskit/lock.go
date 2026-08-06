package rediskit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Locker 是分散式鎖的入口，收編自 labs/05 的自刻實作（SET NX PX + Lua CAS）。
//
// 不用 redsync 的理由：本 repo 的部署形態（single/sentinel/cluster）都是
// 單一邏輯 Redis，redlock 的多節點 quorum 用不上；自刻版還保得住
// fencing token（redsync 沒有），且 Lua 全集中 script.go 可 review。
//
// 降級語意：鎖是安全型原語，一律 fail-close——拿不到、續不了、Redis 掛了，
// 都當作「沒有鎖」處理，寧可不做也不要破壞互斥。
type Locker struct {
	c *Client
}

// Lock 是一把鎖的持有憑證。token 是本次持有的隨機識別，
// 保證「只有我能釋放/續命」——不會誤刪別人在我過期後拿到的鎖。
type Lock struct {
	c     *Client
	key   string // 完整 key（已含 namespace 與 lock: 前綴）
	token string
	ttl   time.Duration
	fence int64 // 0 = 未啟用 fencing
}

// newLockToken 產生 16 bytes 隨機 token（hex）。
func newLockToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// Obtain 嘗試取鎖：SET key token NX PX ttl。
// NX = 只在 key 不存在時設定（互斥）；PX = 毫秒 TTL（持有者 crash 也會自動釋放）。
// 鎖被別人持有時回 ErrLockNotObtained——這是預期路徑，用 errors.Is 判斷後跳過即可。
func (l *Locker) Obtain(ctx context.Context, name string, ttl time.Duration) (*Lock, error) {
	token, err := newLockToken()
	if err != nil {
		return nil, err
	}
	key := l.c.kb.Qualify("lock:" + name)
	ok, err := l.c.rdb.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return nil, MapError(err)
	}
	if !ok {
		return nil, ErrLockNotObtained
	}
	return &Lock{c: l.c, key: key, token: token, ttl: ttl}, nil
}

// ObtainWithFence 取鎖並附上單調遞增的 fencing token（docs/05 第三道防線）。
// 下游（DB / 外部資源）記住看過的最大 fence、拒絕更小的請求，就能擋掉
// 「A 的鎖過期、B 已拿到新鎖，A 遲來的寫入」這種 TTL 鎖的固有破口。
func (l *Locker) ObtainWithFence(ctx context.Context, name string, ttl time.Duration) (*Lock, error) {
	lock, err := l.Obtain(ctx, name, ttl)
	if err != nil {
		return nil, err
	}
	fence, err := l.c.rdb.Incr(ctx, l.c.kb.Qualify("lock:fence:"+name)).Result()
	if err != nil {
		// 拿到鎖但 fence 失敗 → 釋放，別讓鎖懸空到 TTL 才消失
		_ = lock.Release(ctx)
		return nil, MapError(err)
	}
	lock.fence = fence
	return lock, nil
}

// Fence 回傳本次持有的 fencing token；未用 ObtainWithFence 取得時為 0。
func (l *Lock) Fence() int64 { return l.fence }

// Release 釋放鎖（Lua 比對 token 才 DEL）。
// 回 ErrLockLost = 鎖已不是你的（TTL 過期、可能已被別人拿走）——這是正確性信號：
// 你剛做完的工作可能與別人並行了，長任務收到它應該告警而不是靜默忽略。
func (l *Lock) Release(ctx context.Context) error {
	res, err := lockReleaseScript.Run(ctx, l.c.rdb, []string{l.key}, l.token).Int()
	if err != nil {
		return MapError(err)
	}
	if res == 0 {
		return ErrLockLost
	}
	return nil
}

// Refresh 續命：Lua 比對 token 才把 TTL 重設回取鎖時的長度。
// 長任務的 watchdog 定期呼叫；回 ErrLockLost = 鎖已不是你的，任務該中止。
func (l *Lock) Refresh(ctx context.Context) error {
	ms := l.ttl.Milliseconds()
	res, err := lockRefreshScript.Run(ctx, l.c.rdb, []string{l.key}, l.token, ms).Int()
	if err != nil {
		return MapError(err)
	}
	if res == 0 {
		return ErrLockLost
	}
	return nil
}
