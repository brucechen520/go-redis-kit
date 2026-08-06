package rediskit

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// MetricsRecorder 是觀測的注入點。lib 不綁定 Prometheus 或任何指標系統，
// 呼叫端實作這個 interface 接到自己的後端（見 docs/07 §5.1 的核心指標表）。
//
// 實作必須是併發安全且快速的——這些方法在每個 Redis 指令的熱路徑上被呼叫。
type MetricsRecorder interface {
	// ObserveLatency 記錄單一指令的耗時（histogram，抓 p99 / 慢命令）。
	ObserveLatency(cmd string, d time.Duration)
	// IncError 記錄指令錯誤（redis.Nil 這種「key 不存在」不會被算進來）。
	IncError(cmd string)
	// IncHit / IncMiss 記錄快取命中與未中，命中率 = hit / (hit + miss)。
	IncHit()
	IncMiss()
}

// nopRecorder 是預設實作：沒設 WithMetrics 時全部空轉，熱路徑零成本。
type nopRecorder struct{}

func (nopRecorder) ObserveLatency(string, time.Duration) {}
func (nopRecorder) IncError(string)                      {}
func (nopRecorder) IncHit()                              {}
func (nopRecorder) IncMiss()                             {}

// metricsHook 掛在 go-redis 的 Hook 鏈上，攔每個指令記延遲與錯誤。
// 業務碼零感知——這正是「觀測放中間層」的意義。
type metricsHook struct {
	rec MetricsRecorder
}

var _ redis.Hook = metricsHook{}

func (h metricsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmd)
		h.rec.ObserveLatency(cmd.Name(), time.Since(start))
		// redis.Nil = key 不存在，是預期結果不是故障，算進錯誤率會污染告警。
		if err != nil && !errors.Is(err, redis.Nil) {
			h.rec.IncError(cmd.Name())
		}
		return err
	}
}

func (h metricsHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h metricsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
