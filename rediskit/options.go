package rediskit

import (
	"time"
)

// options 集中所有可調參數。呼叫端不直接碰這個 struct，
// 一律走 With* functional options——新增設定不動 New 的簽名。
type options struct {
	addr     string
	password string
	db       int

	namespace  string
	serializer Serializer

	poolSize    int
	minIdle     int
	poolTimeout time.Duration
	dialTO      time.Duration
	readTO      time.Duration
	writeTO     time.Duration
	maxRetries  int

	loadTimeout time.Duration
	metrics     MetricsRecorder
	now         func() time.Time
}

// Option 設定 Client 的單一參數。
type Option func(*options)

// defaultOptions 的原則：能交給 go-redis 預設的就交給它（零值不覆寫），
// rediskit 自己的行為才在這裡給預設。
func defaultOptions() options {
	return options{
		addr:        "localhost:6379",
		serializer:  JSONSerializer{},
		loadTimeout: 10 * time.Second,
		metrics:     nopRecorder{},
		now:         time.Now,
	}
}

// WithAddr 設定 Redis 位址（host:port）。預設 localhost:6379。
func WithAddr(addr string) Option { return func(o *options) { o.addr = addr } }

// WithPassword 設定連線密碼（對應 requirepass）。
func WithPassword(pw string) Option { return func(o *options) { o.password = pw } }

// WithDB 選擇邏輯 DB（cluster 模式下無效，只有 DB 0）。
func WithDB(db int) Option { return func(o *options) { o.db = db } }

// WithNamespace 設定 key 前綴，所有模組產出的 key 都會帶上 namespace:。
// 用於服務/環境隔離（app、svc-order、staging…）。
func WithNamespace(ns string) Option { return func(o *options) { o.namespace = ns } }

// WithSerializer 抽換序列化實作。預設 JSON；換 msgpack/proto 前先 benchmark。
func WithSerializer(s Serializer) Option { return func(o *options) { o.serializer = s } }

// WithPoolSize 設定每個節點的最大連線數。預設 10 * GOMAXPROCS（go-redis 預設）。
func WithPoolSize(n int) Option { return func(o *options) { o.poolSize = n } }

// WithMinIdleConns 設定常駐 idle 連線數，避免尖峰時冷啟建連。
func WithMinIdleConns(n int) Option { return func(o *options) { o.minIdle = n } }

// WithPoolTimeout 設定池滿時等待連線的上限。建議略大於 ReadTimeout，
// 池一忙就狂噴 timeout 通常是這個沒調。
func WithPoolTimeout(d time.Duration) Option { return func(o *options) { o.poolTimeout = d } }

// WithTimeouts 分層設定超時：dial（建 TCP）/ read（單次讀回應）/ write（單次寫請求）。
// 呼叫端的 ctx deadline 要 ≥ (read+write) × (重試次數+1)，否則重試沒機會跑。
func WithTimeouts(dial, read, write time.Duration) Option {
	return func(o *options) { o.dialTO, o.readTO, o.writeTO = dial, read, write }
}

// WithMaxRetries 設定連線層錯誤的自動重試次數。預設 3（go-redis 預設），-1 關閉。
// 注意這只對冪等安全：INCR 類自寫重試要自己想清楚（docs/07 §5.3）。
func WithMaxRetries(n int) Option { return func(o *options) { o.maxRetries = n } }

// WithLoadTimeout 設定 GetOrLoad 回源 loader 的獨立超時。
// 回源的壽命與任何單一呼叫端脫鉤（singleflight 的結果是大家共享的），
// 所以它需要自己的 timeout，不能繼承某個呼叫端的 deadline。預設 10s。
func WithLoadTimeout(d time.Duration) Option { return func(o *options) { o.loadTimeout = d } }

// WithMetrics 掛上指標記錄器（命中率、延遲、錯誤）。不設則零成本空轉。
func WithMetrics(rec MetricsRecorder) Option {
	return func(o *options) {
		if rec != nil {
			o.metrics = rec
		}
	}
}

// WithTimeSource 抽換時間來源。預設 time.Now。
// RateLimiter 的令牌補充以它為準——測試注入假時鐘就能不靠 sleep 驗證限流。
func WithTimeSource(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}
