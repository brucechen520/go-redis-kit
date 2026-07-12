# 07 · rediskit 收斂與生產化

> Stage 7 的目標：把前面各階段（cache-aside、分散式鎖、rate limit、token store…）散落的實作，收斂成一個**可重用、可測試、可上線**的內部函式庫 `rediskit/`，並補齊生產環境需要的可觀測性、連線池調參、重試退避與優雅降級。
>
> 模組：`github.com/twteam/go-redis-kit` · Go 1.26.1 · client `github.com/redis/go-redis/v9`
> 相依：`golang.org/x/sync/singleflight`（併發回源合併）、`github.com/alicebob/miniredis/v2`（單測）、`github.com/go-redsync/redsync/v4`（分散式鎖基礎）
>
> ⚠️ 本文件只**解釋設計與用法**，`rediskit/*.go` 由另一位負責實作。閱讀本文時把它當成 spec 與 code review 的對照標準。

---

## 1. 本階段目標：把前面模式收斂成可重用 lib

前面每個 lab 都在 `labs/` 底下各寫各的：cache 邏輯、Lua 腳本、key 命名、序列化各有一套。這在教學時沒問題，但要上線就會遇到：

- **重複程式碼**：三個服務各自貼一份 cache-aside，改一個 bug 要改三次。
- **不一致**：A 服務用 `user:123`、B 服務用 `users/123`，key 命名沒規範，維運翻 key 翻到死。
- **不可測**：直接把 `*redis.Client` 傳來傳去，單測一定要起一台真 Redis。
- **無觀測**：命中率、延遲、錯誤率沒有任何指標，線上出事只能猜。
- **無韌性**：Redis 一抖動整條線就掛，沒有降級路徑。

Stage 7 要交付的心智模型：

> **`rediskit` 是一層「意圖 API」**。呼叫端說「我要快取這個」「我要拿這把鎖」「這個 request 該不該放行」，而**不需要知道**底層是 `GET`/`SET`/`EVALSHA`、序列化用什麼、key 長什麼樣、TTL 要不要抖動。這些全部封進 lib。

判斷收斂是否成功的標準：**呼叫端的 import 裡不應該出現 `github.com/redis/go-redis/v9`**。只 import `rediskit`。

---

## 2. 分層架構圖

```
┌───────────────────────────────────────────────────────────────────┐
│  呼叫端 (business service)                                          │
│    只依賴 rediskit 的 interface，不 import go-redis                 │
└───────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│  Layer A · 業務 API (rediskit 對外語意)                             │
│                                                                     │
│   Cache          GetOrLoad / Get / Set / Delete                     │
│   Locker         Obtain / Release / (auto-renew)                    │
│   RateLimiter    Allow / AllowN                                     │
│   TokenStore     Save / Load / Rotate / Revoke                     │
│                                                                     │
│   語意：以「業務意圖」命名，回傳語意化 error (ErrCacheMiss…)        │
└───────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│  Layer B · 中間層 (橫切關注點, cross-cutting)                       │
│                                                                     │
│   Serializer     JSON(預設) / msgpack / proto  ← 可換               │
│   KeyBuilder     namespace:entity:id 統一命名                        │
│   TTL jitter     base ± rand%，打散過期避免雪崩                     │
│   Metrics Hook   命中率 / 延遲 histogram / 錯誤 counter             │
│   Tracing Hook   OpenTelemetry span (db.system=redis)              │
│   Error mapping  redis.Nil → ErrCacheMiss；timeout → ErrTimeout     │
│   singleflight   同 key 併發回源只打一次 DB                         │
└───────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────┐
│  Layer C · go-redis/v9 (傳輸與協定)                                 │
│                                                                     │
│   連線池 (PoolSize/MinIdleConns) · 重試 · pipeline ·               │
│   cluster/sentinel 拓撲 · RESP 協定 · Hook 鏈                      │
└───────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                          [ Redis Server ]
```

三層的責任邊界要記牢：

| 層 | 該做的事 | 絕對不該做的事 |
| --- | --- | --- |
| A 業務 API | 定義意圖、語意化 error、組合中間層 | 出現 `redis.Nil`、手拼 key 字串 |
| B 中間層 | 序列化、key 命名、TTL 抖動、觀測、錯誤映射 | 業務判斷（例如「這個 user 能不能登入」） |
| C go-redis | 連線、協定、重試、拓撲 | 業務語意 |

**依賴方向永遠是 A → B → C**，不可反向。中間層不知道「快取的是 User 還是 Order」，業務層不知道「底層 GET 回來是 `redis.Nil`」。

---

## 3. 目錄結構說明

```
rediskit/
├── client.go       連線建立與生命週期
├── options.go      functional options 設定
├── serializer.go   序列化抽象與 JSON 預設實作
├── keys.go         key 命名規範 (KeyBuilder)
├── errors.go       語意化錯誤 (哨兵 error)
├── script.go       Lua 腳本集中管理 (redis.NewScript)
├── cache.go        Cache: GetOrLoad / cache-aside + singleflight
├── lock.go         Locker: 分散式鎖 (redsync 封裝)
├── ratelimit.go    RateLimiter: 令牌桶 / 滑動視窗 (Lua)
└── tokenstore.go   TokenStore: refresh token / session 儲存
```

逐檔職責：

### `client.go` — 連線與生命週期
封裝 `*redis.Client`（或 `redis.UniversalClient`，同時支援 single/sentinel/cluster）。對外只暴露 `*Client` 結構，內部持有底層 client、`Serializer`、`KeyBuilder`、logger。提供 `New(...)`、`Ping(ctx)`、`Close()`。**這是唯一 import go-redis 的公開入口**，其他業務型別都從 `*Client` 生出來（`c.Cache()`、`c.Locker()`…）。

### `options.go` — 設定
所有 functional option：`WithAddr`、`WithPoolSize`、`WithSerializer`、`WithNamespace`、`WithMetrics`、`WithTracing`、`WithTimeouts`… 集中一處，`client.go` 只負責 apply。好處：新增設定不動 `New` 簽名。

### `serializer.go` — 序列化抽象
定義 `Serializer` interface（`Marshal`/`Unmarshal`）+ 預設 `jsonSerializer`。要換 msgpack/proto 只需實作 interface 並用 `WithSerializer` 注入。

### `keys.go` — key 命名
`KeyBuilder`：把 `(entity, id)` 組成 `namespace:entity:id`。統一大小寫、分隔符、跳脫。所有模組（cache/lock/ratelimit/tokenstore）都必須經過它產 key，**禁止在業務碼手拼字串**。

### `errors.go` — 語意化錯誤
哨兵 error：`ErrCacheMiss`、`ErrLockNotObtained`、`ErrRateLimited`、`ErrTokenNotFound`、`ErrTimeout`、`ErrClosed`。並提供把 go-redis 底層錯誤映射成這些的 helper。呼叫端用 `errors.Is(err, rediskit.ErrCacheMiss)` 判斷。

### `script.go` — Lua 集中管理
所有 Lua 腳本用 `redis.NewScript(src)` 宣告成套件變數（rate limit 的令牌桶、lock 的 CAS release、token 的原子 rotate）。集中一處方便 review 原子性、方便測試。

### `cache.go` — 快取
`Cache.GetOrLoad(ctx, key, dst, loader, ttl)`：cache-aside + singleflight + TTL 抖動 + 語意化 miss。是全 lib 最常用的型別。

### `lock.go` — 分散式鎖
封裝 `redsync`：`Obtain(ctx, name, ttl)` 回傳 `*Lock`，`lock.Release(ctx)`，可選 auto-renew（watchdog）。把 redsync 的細節藏起來，只暴露 `Obtain/Release`。

### `ratelimit.go` — 限流
`RateLimiter.Allow(ctx, key)` / `AllowN(ctx, key, n)`。底層走 `script.go` 的 Lua（令牌桶或滑動視窗），保證判斷+扣減原子。

### `tokenstore.go` — token 儲存
`Save/Load/Rotate/Revoke`。refresh token、session 的儲存，含原子輪替（舊 token 失效+新 token 寫入一步到位，走 Lua）。

---

## 4. 核心設計原則

### 4.1 包 interface，不漏 `*redis.Client`（可測、可換）

對外一律用 interface 或封裝結構，**不要**讓業務碼拿到 `*redis.Client`。這樣單測能塞 miniredis 或 mock，未來換底層（例如包一層讀寫分離）也不動呼叫端。

```go
// rediskit/cache.go — 對外是 interface
type Cache interface {
    GetOrLoad(ctx context.Context, key string, dst any,
        loader func(context.Context) (any, error), ttl time.Duration) error
    Get(ctx context.Context, key string, dst any) error
    Set(ctx context.Context, key string, val any, ttl time.Duration) error
    Delete(ctx context.Context, keys ...string) error
}

// 業務端只認得 rediskit.Cache，測試時能換掉整個實作
type UserService struct {
    cache rediskit.Cache // 不是 *redis.Client
}
```

反例（別這樣）：

```go
// ❌ 漏出底層，業務碼被 go-redis 綁死、無法單測
func NewUserService(rdb *redis.Client) *UserService
```

### 4.2 context 必傳

每個會碰網路的方法**第一個參數都是 `ctx context.Context`**，並且真的往下傳到 go-redis 呼叫。超時、取消、trace 傳播全靠它。禁止 `context.Background()` 藏在 lib 內部（除了 `Close()` 這種 shutdown 場景）。

```go
func (c *cache) Get(ctx context.Context, key string, dst any) error {
    b, err := c.rdb.Get(ctx, c.keys.Build(key)).Bytes() // ctx 一路傳下去
    ...
}
```

### 4.3 Serializer 抽象（JSON 預設，可換 msgpack/proto）

值進 Redis 前要序列化。把它抽成 interface，預設 JSON，效能敏感時換 msgpack、跨語言時換 proto，呼叫端零改動。

```go
type Serializer interface {
    Marshal(v any) ([]byte, error)
    Unmarshal(data []byte, v any) error
}

type jsonSerializer struct{}
func (jsonSerializer) Marshal(v any) ([]byte, error)       { return json.Marshal(v) }
func (jsonSerializer) Unmarshal(b []byte, v any) error     { return json.Unmarshal(b, v) }

// 換 msgpack：
client, _ := rediskit.New(
    rediskit.WithAddr("localhost:6379"),
    rediskit.WithSerializer(msgpackSerializer{}),
)
```

### 4.4 Lua 用 `redis.NewScript`

所有多步原子操作走 Lua，用 `redis.NewScript` 宣告。go-redis 的 `NewScript` 會**先試 `EVALSHA`，NOSCRIPT 時自動 fallback `EVAL` 並快取 SHA**，不用自己管腳本載入。集中在 `script.go` 方便審原子性。

```go
// rediskit/script.go
var releaseScript = redis.NewScript(`
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
  else
    return 0
  end
`)

// 呼叫
res, err := releaseScript.Run(ctx, c.rdb, []string{key}, token).Int()
```

### 4.5 錯誤語意化（`redis.Nil` → `ErrCacheMiss`）

go-redis 用 `redis.Nil` 表示 key 不存在——這是**傳輸層細節**，不該漏到業務碼。在中間層映射成哨兵 error。

```go
// rediskit/errors.go
var (
    ErrCacheMiss     = errors.New("rediskit: cache miss")
    ErrLockNotObtained = errors.New("rediskit: lock not obtained")
    ErrRateLimited   = errors.New("rediskit: rate limited")
    ErrTokenNotFound = errors.New("rediskit: token not found")
    ErrTimeout       = errors.New("rediskit: operation timeout")
)

func mapErr(err error) error {
    switch {
    case err == nil:
        return nil
    case errors.Is(err, redis.Nil):
        return ErrCacheMiss
    case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
        return ErrTimeout
    default:
        return err
    }
}
```

呼叫端：

```go
err := cache.Get(ctx, key, &u)
if errors.Is(err, rediskit.ErrCacheMiss) {
    // 回源
}
```

### 4.6 Functional options

設定用 functional options，而非一個巨大的 config struct 或超長參數列。可選、有預設、向後相容。

```go
type options struct {
    addr        string
    poolSize    int
    minIdle     int
    serializer  Serializer
    namespace   string
    dialTO, readTO, writeTO time.Duration
    metrics     MetricsRecorder
    tracer      trace.Tracer
}
type Option func(*options)

func WithPoolSize(n int) Option    { return func(o *options) { o.poolSize = n } }
func WithNamespace(ns string) Option { return func(o *options) { o.namespace = ns } }

func New(opts ...Option) (*Client, error) {
    o := defaultOptions() // 有合理預設
    for _, fn := range opts { fn(&o) }
    ...
}
```

### 4.7 singleflight 合併併發回源

熱點 key 過期瞬間，N 個 request 同時 miss 會**同時打 DB**（cache stampede / 擊穿）。用 `singleflight` 讓同一個 key 的併發回源只執行一次，其餘等結果。

```go
type cache struct {
    rdb  redis.UniversalClient
    sf   singleflight.Group
    ...
}

func (c *cache) GetOrLoad(ctx context.Context, key string, dst any,
    loader func(context.Context) (any, error), ttl time.Duration) error {

    // 1. 先讀快取
    if err := c.Get(ctx, key, dst); err == nil {
        return nil
    } else if !errors.Is(err, ErrCacheMiss) {
        return err
    }

    // 2. miss → 併發合併回源，同 key 只有一個真的跑 loader
    v, err, _ := c.sf.Do(key, func() (any, error) {
        val, err := loader(ctx)
        if err != nil {
            return nil, err
        }
        _ = c.Set(ctx, key, val, jitter(ttl)) // 回填 + TTL 抖動
        return val, nil
    })
    if err != nil {
        return err
    }
    return assign(dst, v) // 把結果指派回 dst
}
```

> 注意：`singleflight` 只在**單一 process 內**合併。跨 process 的擊穿仍要靠分散式鎖或 TTL 抖動緩解。兩者可疊加。

### 4.8 key 命名規範 `namespace:entity:id`

統一格式，維運看得懂、`SCAN` 撈得到、cluster 分片可控。全部經過 `KeyBuilder`。

```go
// rediskit/keys.go
type KeyBuilder struct{ ns string }

func (k KeyBuilder) Build(parts ...string) string {
    return k.ns + ":" + strings.Join(parts, ":")
}

// 用法：ns="app" → "app:user:123"
key := kb.Build("user", "123")
```

命名慣例：

| 段 | 意義 | 範例 |
| --- | --- | --- |
| namespace | 服務/環境隔離 | `app`, `svc-order` |
| entity | 資料類型 | `user`, `session`, `rl` |
| id | 主鍵/識別 | `123`, `token-abc` |

cluster 場景若要讓相關 key 落同 slot，用 hash tag：`app:{user:123}:profile`（`{}` 內才參與 slot 計算）。

---

## 5. 生產化

### 5.1 可觀測性：go-redis Hook 埋 metrics + OpenTelemetry tracing

go-redis/v9 提供 `redis.Hook` 介面，能攔截每個 command 與 pipeline。用它**在中間層一次埋好觀測**，業務碼零感知。

```go
type Hook interface {
    DialHook(next DialHook) DialHook
    ProcessHook(next ProcessHook) ProcessHook
    ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook
}
```

Metrics Hook 範例（延遲 histogram + 錯誤 counter；命中率由 cache 層依 `ErrCacheMiss` 計）：

```go
// rediskit/observability.go (概念示意)
type metricsHook struct{ rec MetricsRecorder }

func (h metricsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
    return func(ctx context.Context, cmd redis.Cmder) error {
        start := time.Now()
        err := next(ctx, cmd)               // 執行實際命令
        h.rec.ObserveLatency(cmd.Name(), time.Since(start))
        if err != nil && !errors.Is(err, redis.Nil) {
            h.rec.IncError(cmd.Name())      // redis.Nil 不算錯誤
        }
        return err
    }
}
func (h metricsHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h metricsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook { return next }

// 命中率在 cache 層記：
func (c *cache) Get(ctx context.Context, key string, dst any) error {
    err := ...
    if errors.Is(err, ErrCacheMiss) { c.rec.IncMiss() } else if err == nil { c.rec.IncHit() }
    return err
}
```

Tracing Hook 範例（OpenTelemetry span）：

```go
type tracingHook struct{ tracer trace.Tracer }

func (h tracingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
    return func(ctx context.Context, cmd redis.Cmder) error {
        ctx, span := h.tracer.Start(ctx, "redis."+cmd.Name(),
            trace.WithSpanKind(trace.SpanKindClient),
            trace.WithAttributes(
                attribute.String("db.system", "redis"),
                attribute.String("db.operation", cmd.Name()),
            ))
        defer span.End()
        err := next(ctx, cmd)
        if err != nil && !errors.Is(err, redis.Nil) {
            span.RecordError(err)
            span.SetStatus(codes.Error, err.Error())
        }
        return err
    }
}
```

掛上去：

```go
rdb := redis.NewClient(&redis.Options{Addr: addr})
rdb.AddHook(metricsHook{rec})
rdb.AddHook(tracingHook{tracer})
```

> 現成方案：官方 `redisotel`（`github.com/redis/go-redis/extra/redisotel/v9`）已封好 tracing+metrics，生產可直接用；自寫 Hook 的價值在完全掌控指標維度與避免額外相依，教學用自寫理解原理。

要埋的核心指標：

| 指標 | 型別 | 用途 |
| --- | --- | --- |
| `cache_hit_total` / `cache_miss_total` | counter | 算命中率 = hit/(hit+miss) |
| `redis_cmd_duration_seconds` | histogram | p50/p99 延遲、抓慢命令 |
| `redis_cmd_error_total{cmd}` | counter | 錯誤率、告警 |
| `redis_pool_hits/misses/timeouts` | gauge | 連線池是否吃緊（見 `rdb.PoolStats()`） |

### 5.2 連線池調參

go-redis 的 `redis.Options` 關鍵欄位：

| 參數 | 意義 | 建議起點 |
| --- | --- | --- |
| `PoolSize` | 每個節點最大連線數 | `10 * GOMAXPROCS`（預設）；高併發可調大 |
| `MinIdleConns` | 常駐 idle 連線，避免冷啟建連 | 依尖峰預熱，如 `PoolSize/4` |
| `PoolTimeout` | 池滿時等連線的最長時間 | `ReadTimeout + 1s` 左右 |
| `ConnMaxIdleTime` | idle 連線多久回收 | `30m` |
| `ConnMaxLifetime` | 連線最長壽命（配合 LB/故障轉移） | `0`（不限）或 `1h` |

超時要**分層**設定，別只靠一個 ctx：

```go
rdb := redis.NewClient(&redis.Options{
    Addr:         "localhost:6379",
    PoolSize:     50,
    MinIdleConns: 10,
    PoolTimeout:  4 * time.Second,

    DialTimeout:  5 * time.Second,  // 建立 TCP 連線
    ReadTimeout:  3 * time.Second,  // 單次讀回應
    WriteTimeout: 3 * time.Second,  // 單次寫請求
})
```

超時分層的關係：

```
ctx deadline (整個操作總預算, 呼叫端設)
   └── 涵蓋可能的重試次數
        └── 每次嘗試: DialTimeout(首次建連) + WriteTimeout + ReadTimeout
```

原則：**ctx deadline 要 ≥ (單次 Read+Write timeout) × (重試次數+1)**，否則 ctx 先到期，重試根本沒機會跑。`PoolTimeout` 要略大於 `ReadTimeout`，避免池一忙就狂噴 timeout。

觀察池健康：

```go
s := rdb.PoolStats()
// s.Hits / s.Misses / s.Timeouts / s.TotalConns / s.IdleConns
// Timeouts 持續 >0 → PoolSize 不夠或後端變慢
```

### 5.3 重試 + 退避（只冪等重試）

go-redis 內建重試，由 `MaxRetries` + `MinRetryBackoff`/`MaxRetryBackoff` 控制（指數退避 + jitter）：

```go
rdb := redis.NewClient(&redis.Options{
    MaxRetries:      3,                      // -1 關閉；預設 3
    MinRetryBackoff: 8 * time.Millisecond,
    MaxRetryBackoff: 512 * time.Millisecond,
})
```

**關鍵：只有冪等操作能安全重試。**

| 操作 | 冪等？ | 能否自動重試 |
| --- | --- | --- |
| `GET`, `EXISTS`, `TTL` | 是 | ✅ |
| `SET key v`（無條件覆寫） | 是 | ✅ |
| `INCR`, `LPUSH`, `SETNX` | **否** | ⚠️ 重試可能重複計數/重複入列 |
| Lua 扣令牌 / rotate token | 否 | ⚠️ 需腳本自身冪等，或別重試 |

go-redis 的內建重試主要針對**連線層錯誤**（網路瞬斷、`LOADING`、`CLUSTERDOWN`…）而非已成功送達的命令，相對安全；但對 `INCR` 類自寫重試時務必當心。設計上：

- 讀多的 cache/tokenstore Load → 放心重試。
- rate limit 扣減、lock 取得 → **交給 Lua 的原子性**，寧可失敗回 `ErrRateLimited`/`ErrLockNotObtained` 讓上層決定，也不要盲目重試造成超扣。
- 需要重試非冪等寫時，用「請求去重 token」讓腳本自身冪等（同 token 只生效一次）。

### 5.4 優雅降級 / 熔斷

Redis 掛掉不該讓整個服務掛掉。分兩種降級路徑：

**A. Cache 降級 → 直接打 DB（fail-open）**
快取只是加速，Redis 不可用時退回直讀 DB（要注意 DB 別被打爆，配合限流/local cache）。

```go
func (s *UserService) GetUser(ctx context.Context, id string) (*User, error) {
    var u User
    err := s.cache.GetOrLoad(ctx, key(id), &u, s.loadFromDB, 10*time.Minute)
    if err != nil && isRedisDown(err) {
        // Redis 掛了 → 降級直讀 DB，不讓請求失敗
        s.metrics.IncCacheDegraded()
        return s.loadUserFromDB(ctx, id)
    }
    return &u, err
}
```

**B. 加 local cache 當第二層（防雪崩 + 降級）**
Redis 前面擋一層 process 內 LRU（如 `ristretto`），Redis 掛時還能撐一段：`local → redis → DB`。

**C. 熔斷器（circuit breaker）**
Redis 連續錯誤達閾值就「開路」，一段時間內直接走降級路徑、不再打 Redis，避免每個請求都卡滿 timeout（`ReadTimeout` × QPS 會拖垮延遲）。用 `sony/gobreaker` 之類包在 client 呼叫外層：

```
關閉(正常) --錯誤率超閾值--> 開路(全走降級, 不打 redis)
    ^                              │ 冷卻時間到
    └──探測成功──── 半開(放少量探測) <┘
```

降級的**安全性差異**要想清楚：

| 型別 | 降級方向 | 風險 |
| --- | --- | --- |
| Cache | fail-open（照放，打 DB） | DB 壓力上升 → 要配限流 |
| RateLimiter | fail-open 或 fail-close？ | fail-open 會被刷爆；安全場景該 fail-close |
| Lock | **fail-close**（拿不到就別動） | fail-open = 破壞互斥，資料損毀 |
| TokenStore | fail-close | fail-open = 認證繞過，嚴重 |

> 一句話：**加速型降級可以 fail-open，安全型（鎖/認證/限流）預設 fail-close。** 這個決策要寫進 lib 註解，別讓呼叫端誤用。

---

## 6. 測試策略

三層測試，成本由低到高：

### 6.1 miniredis 單元測試（免 Docker，秒級）

`miniredis` 是純 Go 的 in-memory Redis，跑在 process 內，CI 免起 container。適合測 cache/keys/serializer/errors 邏輯。

```go
func TestCache_GetOrLoad(t *testing.T) {
    mr := miniredis.RunT(t) // t.Cleanup 自動關

    client, err := rediskit.New(rediskit.WithAddr(mr.Addr()))
    if err != nil { t.Fatal(err) }
    c := client.Cache()

    ctx := context.Background()
    calls := 0
    loader := func(context.Context) (any, error) {
        calls++
        return &User{ID: "1", Name: "Ada"}, nil
    }

    var u User
    // 第一次 miss → 呼叫 loader
    if err := c.GetOrLoad(ctx, "user:1", &u, loader, time.Minute); err != nil {
        t.Fatal(err)
    }
    // 第二次 hit → 不再呼叫 loader
    _ = c.GetOrLoad(ctx, "user:1", &u, loader, time.Minute)
    if calls != 1 {
        t.Fatalf("loader 應只被呼叫 1 次, 實際 %d", calls)
    }

    // 驗 TTL、驗 key 命名
    mr.FastForward(time.Minute + time.Second) // 快轉讓 key 過期
    if err := c.Get(ctx, "user:1", &u); !errors.Is(err, rediskit.ErrCacheMiss) {
        t.Fatalf("過期後應 miss, got %v", err)
    }
}
```

miniredis 的殺手鐧：`mr.FastForward()` 快轉時間測 TTL、`mr.SetError()` 注入錯誤測降級路徑。**限制**：Lua 支援不完整（部分指令/`redis.call` 行為與真 Redis 有落差）、cluster 語意沒有——所以 Lua 腳本與 cluster 行為要靠整合測補。

### 6.2 真 Redis 整合測（build tag 隔離）

Lua 原子性、pipeline、cluster、真實 TTL 精度用真 Redis。用 build tag 讓它預設不跑，只在有 Redis 時手動觸發。

```go
//go:build integration

package rediskit_test

func TestRateLimiter_Lua_Integration(t *testing.T) {
    addr := os.Getenv("REDIS_ADDR")
    if addr == "" { t.Skip("需設 REDIS_ADDR") }
    client, _ := rediskit.New(rediskit.WithAddr(addr))
    rl := client.RateLimiter(10, time.Second) // 10/s
    ...
}
```

```bash
# 起真 redis（用 repo 的 compose）
make single-up

# 只跑整合測
REDIS_ADDR=localhost:6379 go test -tags=integration ./rediskit/...

# 一般 CI 不帶 tag → 只跑 miniredis 單測
go test ./...
```

### 6.3 壓測：`go test -bench` + `redis-benchmark`

**Go 層 benchmark**（測 lib 本身開銷：序列化、singleflight、key 組裝）：

```go
func BenchmarkCache_GetOrLoad_Hit(b *testing.B) {
    mr := miniredis.RunT(b)
    client, _ := rediskit.New(rediskit.WithAddr(mr.Addr()))
    c := client.Cache()
    ctx := context.Background()
    loader := func(context.Context) (any, error) { return &User{ID: "1"}, nil }
    _ = c.GetOrLoad(ctx, "user:1", &User{}, loader, time.Hour) // 先暖 key

    b.ReportAllocs()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        var u User
        _ = c.GetOrLoad(ctx, "user:1", &u, loader, time.Hour)
    }
}
```

```bash
make bench                                   # go test -bench=. -benchmem ./...
go test -bench=BenchmarkCache -benchmem -cpu=1,4,8 ./rediskit/
```

看 `ns/op`（延遲）、`B/op` + `allocs/op`（記憶體/GC 壓力）。序列化與 key 組裝是常見 alloc 熱點。

**redis-benchmark**（測 Redis server + 網路的天花板，跟 lib 無關，用來對照 lib 開銷佔比）：

```bash
# 對真 redis 打 GET/SET，100 併發、10 萬次、pipeline 16
redis-benchmark -h localhost -p 6379 -c 100 -n 100000 -P 16 -t get,set -q
```

判讀：若 `redis-benchmark` 顯示 server 能 100k QPS，但你的 Go bench 只有 20k，差距就是 lib+網路+序列化的成本，據此找優化點（換 msgpack、開 pipeline、調池）。

---

## 7. API 使用範例（完整串起來）

```go
package main

import (
    "context"
    "errors"
    "log"
    "time"

    "github.com/twteam/go-redis-kit/rediskit"
)

func main() {
    ctx := context.Background()

    // 1) 建 client：functional options，一次設好池/超時/命名空間/觀測
    client, err := rediskit.New(
        rediskit.WithAddr("localhost:6379"),
        rediskit.WithNamespace("app"),
        rediskit.WithPoolSize(50),
        rediskit.WithMinIdleConns(10),
        rediskit.WithTimeouts(5*time.Second, 3*time.Second, 3*time.Second), // dial/read/write
        rediskit.WithMetrics(promRecorder),
        rediskit.WithTracing(otelTracer),
        // rediskit.WithSerializer(msgpackSerializer{}), // 要換序列化就開這行
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // 2) Cache.GetOrLoad —— cache-aside + singleflight + TTL 抖動全內建
    cache := client.Cache()
    var u User
    err = cache.GetOrLoad(ctx, "user:123", &u,
        func(ctx context.Context) (any, error) {
            return loadUserFromDB(ctx, "123") // 只有 cache miss 時才會被呼叫
        },
        10*time.Minute,
    )
    switch {
    case err == nil:
        log.Printf("user = %+v", u)
    case errors.Is(err, rediskit.ErrTimeout):
        log.Println("redis 超時，走降級")
    default:
        log.Printf("load 失敗: %v", err)
    }

    // 3) Locker —— 分散式互斥；拿不到就別動（fail-close）
    locker := client.Locker()
    lock, err := locker.Obtain(ctx, "job:daily-report", 30*time.Second)
    if errors.Is(err, rediskit.ErrLockNotObtained) {
        log.Println("別人正在跑，本次跳過")
        return
    } else if err != nil {
        log.Fatal(err)
    }
    defer lock.Release(ctx) // 只會刪自己那把（Lua CAS）
    runDailyReport(ctx)

    // 4) RateLimiter.Allow —— 令牌桶，判斷+扣減原子（Lua）
    rl := client.RateLimiter(100, time.Minute) // 每分鐘 100 次
    ok, err := rl.Allow(ctx, "login:user:123")
    if err != nil {
        log.Printf("限流檢查失敗: %v", err)
    }
    if !ok {
        log.Println("太頻繁，擋下") // 或回 429
        return
    }

    // 5) TokenStore —— refresh token 儲存與原子輪替
    ts := client.TokenStore()
    if err := ts.Save(ctx, "sess:abc", "refresh-token-xyz", 24*time.Hour); err != nil {
        log.Fatal(err)
    }
    tok, err := ts.Load(ctx, "sess:abc")
    if errors.Is(err, rediskit.ErrTokenNotFound) {
        log.Println("session 不存在，要求重新登入")
        return
    }
    log.Printf("token = %s", tok)

    // 輪替：舊失效 + 新寫入 一步原子完成
    newTok, err := ts.Rotate(ctx, "sess:abc", "refresh-token-new", 24*time.Hour)
    _ = newTok
    _ = err
}
```

要點回顧：呼叫端**完全沒 import go-redis**，錯誤全是 `rediskit.Err*`，key 只給業務片段（`user:123`）由 lib 補 namespace，觀測與池化在 `New` 一次設定完。

---

## 8. 交付物 checklist

- [ ] `rediskit/` 十個檔案齊備，公開 API 皆為 interface 或封裝結構，**無任何 `*redis.Client` 外漏**。
- [ ] 每個碰網路的方法第一參數為 `ctx`，且真的往下傳。
- [ ] `Serializer` 可透過 `WithSerializer` 抽換；JSON 為預設。
- [ ] 所有 key 經 `KeyBuilder`，格式 `namespace:entity:id`，無手拼字串。
- [ ] 語意化 error 完整（`ErrCacheMiss`/`ErrLockNotObtained`/`ErrRateLimited`/`ErrTokenNotFound`/`ErrTimeout`），並有 `mapErr` 把 `redis.Nil` 等映射掉。
- [ ] 多步原子操作全走 `script.go` 的 `redis.NewScript`。
- [ ] `Cache.GetOrLoad` 具備 singleflight 併發合併 + TTL 抖動。
- [ ] Metrics Hook（命中率/延遲/錯誤）與 Tracing Hook（OTel）可透過 options 掛上。
- [ ] 連線池與分層超時（dial/read/write + PoolTimeout）可設定，有合理預設。
- [ ] 重試策略明確：冪等操作才重試；鎖/限流/token 降級為 fail-close。
- [ ] 降級路徑：cache fail-open（回 DB），安全型 fail-close，有註解說明。
- [ ] miniredis 單測涵蓋 cache/keys/serializer/errors；`FastForward` 測 TTL。
- [ ] `//go:build integration` 整合測涵蓋 Lua/cluster；`go test ./...` 預設只跑單測。
- [ ] benchmark（`make bench`）有覆蓋 GetOrLoad hit/miss，回報 `allocs/op`。
- [ ] `Client.Close()` 正確釋放連線池；`New` 失敗不外洩資源。

---

## 9. 練習題 + 檢查點 + 延伸閱讀

### 練習題

1. **抽換序列化**：實作一個 `msgpackSerializer` 並用 `WithSerializer` 注入，用 benchmark 比較它與 JSON 的 `ns/op` 與 `allocs/op`。何時值得換？
2. **降級演練**：用 `miniredis` 的 `mr.SetError("mock down")` 或直接 `mr.Close()`，驗證 `Cache.GetOrLoad` 走 DB 降級、`Locker.Obtain` 回 `ErrLockNotObtained`（fail-close）。
3. **擊穿測試**：對同一個 miss 的 key 併發 1000 goroutine 呼叫 `GetOrLoad`，斷言 loader 只被呼叫 1 次（singleflight 生效）。
4. **加熔斷**：用 `sony/gobreaker` 包住 client 呼叫，模擬 Redis 連續失敗後開路，量測 p99 延遲在有/無熔斷下的差異。
5. **cluster hash tag**：把 rate limit 的相關 key 用 `{}` 綁到同 slot，觀察 `MOVED`/`CROSSSLOT` 是否消失。

### 檢查點（自問）

- 我的呼叫端 import 列表裡還有 `go-redis` 嗎？（應該沒有）
- Redis 掛掉時，哪些功能該繼續、哪些該擋？我有沒有寫錯 fail-open/fail-close？
- ctx deadline 有沒有小於「重試次數 × 單次超時」導致重試無效？
- 我的 Lua 腳本在 `INCR`/`SET` 混用時，跨網路重試會不會超扣？
- 命中率指標是不是把 `redis.Nil` 誤算成錯誤了？

### 延伸閱讀

- go-redis 文件：Hooks、`redis.NewScript`、`UniversalClient`、`PoolStats`
- `golang.org/x/sync/singleflight` 原始碼與 `DoChan`（帶 ctx 取消版本）
- Redis 官方：cache-aside、cache stampede、hash tags、Lua scripting、`redis-benchmark`
- OpenTelemetry Semantic Conventions for Database（`db.system` / `db.operation`）
- `redisotel`（官方 OTel instrumentation）、`sony/gobreaker`（熔斷）、`dgraph-io/ristretto`（local cache）
- 前面各階段 lab（`labs/`）：對照 rediskit 如何把它們收斂
