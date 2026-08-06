// Package rediskit 是一層「意圖 API」：呼叫端說「我要快取這個」「我要拿這把鎖」，
// 而不需要知道底層是 GET/SET/EVALSHA、序列化用什麼、key 長什麼樣。
//
// 收斂是否成功的判準：呼叫端的 import 裡不應該出現 github.com/redis/go-redis/v9。
package rediskit

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/redis/go-redis/v9"
)

// 哨兵 error。呼叫端一律用 errors.Is 判斷，不要比對字串。
var (
	// ErrCacheMiss 表示 key 不存在。這是預期內的正常路徑，不是故障。
	ErrCacheMiss = errors.New("rediskit: cache miss")
	// ErrLockNotObtained 表示鎖被別人持有。安全型操作應 fail-close（放棄，別硬做）。
	ErrLockNotObtained = errors.New("rediskit: lock not obtained")
	// ErrLockLost 表示 Release/Refresh 時鎖已經不是你的（TTL 過期、可能已被別人取得）。
	// 這是正確性信號，不是技術細節：收到它代表你的臨界區可能正與別人並行，
	// 長任務應該中止而不是繼續寫入。
	ErrLockLost = errors.New("rediskit: lock lost")
	// ErrRateLimited 表示配額用罄，應回 429 之類給上層。
	ErrRateLimited = errors.New("rediskit: rate limited")
	// ErrTokenNotFound 表示 token/session 不存在或已被撤銷，應要求重新登入。
	ErrTokenNotFound = errors.New("rediskit: token not found")
	// ErrTimeout 表示操作逾時（ctx deadline 到期、socket read/write timeout）。
	// 這代表 Redis 或網路真的慢，是該進告警的訊號。
	ErrTimeout = errors.New("rediskit: operation timeout")
	// ErrCanceled 表示呼叫端主動取消（ctx 被 cancel，例如 HTTP client 斷線）。
	//
	// 刻意與 ErrTimeout 分開：取消是「上游不要了」，不是「Redis 慢」。
	// 混在一起會讓 client 斷線灌爆逾時指標，把真正的延遲問題蓋掉。
	// 降級判斷也不同——取消不該觸發熔斷或 fallback，請求已經沒人要了。
	ErrCanceled = errors.New("rediskit: operation canceled")
	// ErrClosed 表示 client 已關閉，後續操作都不會成功。
	ErrClosed = errors.New("rediskit: client closed")
)

// errTokenTTLRequired：TokenStore 拒絕永不過期的憑證（安全紅線），
// 屬使用方式錯誤而非執行期狀態，故不列入哨兵、不匯出。
var errTokenTTLRequired = errors.New("rediskit: token ttl must be > 0")

// MapError 把 go-redis 的傳輸層錯誤映射成 rediskit 的語意化 error，
// 讓 redis.Nil 這種協定細節不會漏到業務碼。key 不存在映射成 ErrCacheMiss。
//
// 除了 redis.Nil，回傳值都會用 %w 包住原始錯誤，errors.Is 對兩邊都成立：
// errors.Is(err, ErrTimeout) 與 errors.Is(err, context.DeadlineExceeded) 同時為 true。
func MapError(err error) error {
	return MapNotFound(err, ErrCacheMiss)
}

// MapNotFound 同 MapError，但由呼叫端指定 key 不存在時要回哪個哨兵 error。
// Cache 用 ErrCacheMiss、TokenStore 用 ErrTokenNotFound，其餘映射邏輯共用。
func MapNotFound(err error, notFound error) error {
	switch {
	case err == nil:
		return nil

	// redis.Nil = key 不存在。這是熱路徑且屬預期結果，回裸哨兵不包裝、不配置記憶體。
	case errors.Is(err, redis.Nil):
		return notFound

	// Canceled 要在 DeadlineExceeded 之前判：ctx 到期後常被層層包裝，
	// 先分出「上游主動取消」才不會誤記成 Redis 變慢。
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("%w: %w", ErrCanceled, err)

	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %w", ErrTimeout, err)

	case errors.Is(err, redis.ErrClosed):
		return fmt.Errorf("%w: %w", ErrClosed, err)

	default:
		// go-redis 的 read/write timeout 是 *net.OpError，不帶 context 語意，
		// 要靠 net.Error.Timeout() 認出來。
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return fmt.Errorf("%w: %w", ErrTimeout, err)
		}
		return err
	}
}
