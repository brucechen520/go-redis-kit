# rediskit 使用範例

> 從零到能用：連線 → 快取 → 錯誤處理 → 鎖 → 限流 → token → 逃生艙 → 測試。
> 每個範例後面都有「實際情境」說明這段 code 在真實系統的哪個位置。
> 設計原理見 [07-rediskit-production.md](./07-rediskit-production.md)；本文只管「怎麼用」。

前置：起一台 Redis（`make single-up`），或跑測試時用 miniredis（見 §9，完全不用 Docker）。

---

## 1. 建立連線

### 最小版

```go
import "github.com/brucechen520/go-redis-kit/rediskit"

client, err := rediskit.New(
    rediskit.WithAddr("localhost:6379"),
    rediskit.WithPassword(os.Getenv("REDIS_PASSWORD")), // repo 的 compose 有設 requirepass
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// New 是惰性的，不碰網路；啟動時 Ping 一次 fail fast
if err := client.Ping(ctx); err != nil {
    log.Fatalf("redis 連不上: %v", err)
}
```

### 生產版（把 docs/07 §5.2 的調參一次設好）

```go
client, err := rediskit.New(
    rediskit.WithAddr("redis.internal:6379"),
    rediskit.WithPassword(secret),
    rediskit.WithNamespace("svc-order"),                // 所有 key 自動帶 svc-order: 前綴
    rediskit.WithPoolSize(50),
    rediskit.WithMinIdleConns(10),
    rediskit.WithTimeouts(5*time.Second, 3*time.Second, 3*time.Second), // dial / read / write
    rediskit.WithPoolTimeout(4*time.Second),            // 略大於 read timeout
    rediskit.WithMetrics(myPromRecorder),               // 見 §8
)
```

> **實際情境**：`New` 在 `main.go` 或 DI container 呼叫**一次**，整個 process 共用同一個
> `*Client`（內含連線池）。不要每個 request 建一個——那等於沒有連線池。
> `WithNamespace` 用服務名，兩個服務共用一台 Redis 時 key 不會互踩。

---

## 2. Cache：Get / Set / Delete

```go
cache := client.Cache()

type User struct {
    ID   string `json:"id"`
    Name string `json:"name"`
}

// 寫入：val 會自動走 Serializer（預設 JSON），TTL 自動加 ±10% 抖動
err := cache.Set(ctx, "user:123", User{ID: "123", Name: "Ada"}, 10*time.Minute)

// 讀取：反序列化進你給的指標
var u User
err = cache.Get(ctx, "user:123", &u)
if errors.Is(err, rediskit.ErrCacheMiss) {
    // key 不存在——正常路徑，不是故障
}

// 失效：改了 DB 之後把快取踢掉
err = cache.Delete(ctx, "user:123")
```

> **實際情境**：`Delete` 放在「寫 DB 成功之後」——更新 user 資料的 handler 最後一行
> `cache.Delete(ctx, "user:"+id)`，下次讀取自然回源拿新資料（cache-aside 的失效策略）。
> 直接 `Set` 新值也行，但「刪掉讓它回源」比較不會有寫入順序的 race。

---

## 3. GetOrLoad：cache-aside 一次呼叫版（最常用）

§2 的 Get/Set 適合手動控制；日常讀取用 `GetOrLoad` 一行搞定「讀快取 → miss 回源 → 回填」：

```go
u, err := rediskit.GetOrLoad(ctx, client.Cache(), "user:123", 10*time.Minute,
    func(ctx context.Context) (User, error) {
        // 只有 cache miss 時才會被呼叫；ctx 是 lib 給的獨立壽命（見下）
        return userRepo.FindByID(ctx, "123")
    })
```

三件事自動發生，你不用管：

1. **singleflight**：1000 個 request 同時 miss 同一個 key，`userRepo.FindByID` 只跑一次，其他 999 個等結果——DB 不會被擊穿。
2. **TTL 抖動**：大量 key 不會同一秒集體過期（雪崩防線）。
3. **loader 壽命獨立**：loader 拿到的 ctx 與任何單一呼叫端脫鉤（`WithLoadTimeout` 控制，預設 10s）。發起回源的那個 request 斷線，回源照跑、結果照回填，其他等待者不陪葬。

泛型：回傳型別由 loader 推導，接錯型別**編譯期**就報錯，不用 runtime 才炸。

```go
// 呼叫端自己的 ctx 只決定「自己要等多久」
u, err := rediskit.GetOrLoad(ctx, cache, key, ttl, loader)
switch {
case err == nil:                                // 拿到（快取或回源）
case errors.Is(err, rediskit.ErrCanceled):      // 自己不等了（client 斷線）
case errors.Is(err, rediskit.ErrTimeout):       // Redis 慢——考慮降級直讀 DB
default:                                        // loader 的錯誤原樣傳回（DB 掛了等）
}
```

> **實際情境**：這是讀多寫少資料的標準讀路徑——商品頁、user profile、設定檔。
> 搭配 §2 的 `Delete` 做失效，就是完整的 cache-aside。
> 降級要不要「Redis 掛了直讀 DB」是**你的**決定（lib 不替你打 DB）：
> `errors.Is(err, rediskit.ErrTimeout) || errors.Is(err, rediskit.ErrClosed)` 時
> 直接呼叫 loader 本尊，並記一個 degraded metric——記得配限流保護 DB。

---

## 4. 錯誤處理：8 個哨兵一張表

所有錯誤用 `errors.Is` 判斷，**永遠不要**比對錯誤字串、也看不到 `redis.Nil`：

| 哨兵 | 意義 | 呼叫端該做什麼 |
| --- | --- | --- |
| `ErrCacheMiss` | key 不存在 | 回源；正常路徑 |
| `ErrLockNotObtained` | 鎖被別人持有 | 跳過本輪 / 稍後再試 |
| `ErrLockLost` | 釋放/續命時鎖已非己有 | **告警**——臨界區可能與別人並行了 |
| `ErrRateLimited` | 額度用罄 | 回 429 |
| `ErrTokenNotFound` | token 不存在/過期/已輪替 | 要求重新登入 |
| `ErrTimeout` | Redis 或網路慢 | 告警、考慮降級 |
| `ErrCanceled` | 呼叫端自己取消 | 靜默返回；**不要**觸發降級或熔斷 |
| `ErrClosed` | client 已 Close | 程式生命週期 bug，修 code |

`ErrTimeout` vs `ErrCanceled` 要分清楚：前者是 Redis 的問題（該進告警），後者是上游不要了（進告警就是雜訊）。哨兵有用 `%w` 保留原因鏈，`errors.Is(err, context.DeadlineExceeded)` 同時成立，log 印 `%v` 看得到底層原因。

---

## 5. Locker：分散式鎖

### 基本款：排程任務防重跑

```go
lock, err := client.Locker().Obtain(ctx, "job:daily-report", 30*time.Second)
if errors.Is(err, rediskit.ErrLockNotObtained) {
    log.Println("別台正在跑，本次跳過")   // fail-close：拿不到就不做
    return
}
if err != nil {
    return err                          // Redis 掛了也是不做（安全型不 fail-open）
}
defer func() {
    if err := lock.Release(ctx); errors.Is(err, rediskit.ErrLockLost) {
        log.Println("警告: 鎖在任務期間過期，本次結果可能與他台並行")
        // 進 metrics/告警——這通常代表 TTL 設太短或任務太慢
    }
}()

runDailyReport(ctx)
```

> **實際情境**：k8s 多副本部署，每台都掛了同一個 cron。沒有鎖 → 日報寄三份。
> TTL 取「任務正常時長 × 3」；任務可能超時的話用下面的 Refresh。

### 長任務：手動 watchdog 續命

```go
ctx, cancel := context.WithCancel(ctx) // 鎖丟了要能取消主任務
defer cancel()

lock, err := client.Locker().Obtain(ctx, "job:rebuild-index", 30*time.Second)
if err != nil { return err }
defer lock.Release(ctx)

// 每 TTL/3 續一次命；續不動（ErrLockLost）就中止任務
go func() {
    t := time.NewTicker(10 * time.Second)
    defer t.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-t.C:
            if err := lock.Refresh(ctx); err != nil {
                cancel() // 鎖丟了 → 取消主任務，別繼續寫
                return
            }
        }
    }
}()

rebuildIndex(ctx) // 跑多久都行，鎖一直是你的
```

### fencing token：下游會寫入時的最後防線

```go
lock, err := client.Locker().ObtainWithFence(ctx, "order:9527", 10*time.Second)
if err != nil { return err }
defer lock.Release(ctx)

// 把 fence 帶給下游，下游拒絕比看過的最大值小的請求
db.Exec(`UPDATE orders SET state=?, fence=? WHERE id=? AND fence < ?`,
    newState, lock.Fence(), orderID, lock.Fence())
```

> **實際情境**：TTL 鎖有個固有破口——A 拿鎖後 GC 停頓 15 秒，鎖過期、B 拿到新鎖,
> A 醒來還以為自己持有、繼續寫入。fence 單調遞增，下游用 `fence < ?` 一個條件擋掉
> A 的舊請求。只在「鎖保護的是會寫入的外部資源」時需要；純防重跑用基本款即可。

---

## 6. RateLimiter：限流

```go
// 建構時定政策：每分鐘 100 次（令牌桶，允許瞬間 burst 到 100）
loginLimiter := client.RateLimiter(100, time.Minute)

// HTTP middleware
func rateLimitMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // key 決定「按什麼對象限」：這裡按 IP
        err := loginLimiter.Allow(r.Context(), "login:"+clientIP(r))
        switch {
        case errors.Is(err, rediskit.ErrRateLimited):
            w.WriteHeader(http.StatusTooManyRequests)
            return
        case err != nil:
            // Redis 掛了。fail-open 還是 fail-close 是業務決定：
            // 登入防爆破 → fail-close（擋下）；一般 API → fail-open（放行 + 記 metric）
            w.WriteHeader(http.StatusServiceUnavailable)
            return
        }
        next.ServeHTTP(w, r)
    })
}

// 加權成本：一次批量操作吃 5 個令牌，原子——要嘛全拿要嘛不拿
err := exportLimiter.AllowN(ctx, "export:"+userID, 5)
```

> **實際情境**：`RateLimiter` 每個「政策」建一個（登入 100/min、寄信 10/hour、
> 匯出 5/min），`key` 區分「對象」（IP、user id、API key）。
> 因為是分散式的（狀態在 Redis），多副本部署額度是全域共享的——
> 這是它和 in-process limiter（`golang.org/x/time/rate`）的差別。

---

## 7. TokenStore：refresh token / session

```go
ts := client.TokenStore()

// 登入：發 refresh token
err := ts.Save(ctx, "sess:"+sessionID, refreshToken, 30*24*time.Hour)
// 注意：ttl 必須 > 0，永不過期的憑證直接被拒

// 驗證
tok, err := ts.Load(ctx, "sess:"+sessionID)
if errors.Is(err, rediskit.ErrTokenNotFound) {
    // 過期或已撤銷 → 要求重新登入
}

// 換發（refresh token rotation）：舊失效 + 新生效，一步原子
err = ts.Rotate(ctx, "sess:"+sessionID, oldToken, newToken, 30*24*time.Hour)
if errors.Is(err, rediskit.ErrTokenNotFound) {
    // 舊 token 對不上。兩種可能：過期，或「已經被輪替過」——
    // 後者代表舊 token 被重放（可能外洩），保守做法：撤銷整個 session
    _ = ts.Revoke(ctx, "sess:"+sessionID)
    // 並要求重新登入 + 記安全事件
}

// 登出（冪等，token 不存在也回 nil）
err = ts.Revoke(ctx, "sess:"+sessionID)
```

> **實際情境**：OAuth refresh token rotation 的標準流程。`Rotate` 的原子性是重點：
> 沒有它，兩個併發的 refresh 請求（正常 app + 偷到 token 的攻擊者）會**都成功**，
> 重放偵測就形同虛設。Redis 掛掉時一律 fail-close——驗不了就是不放行。

---

## 8. 進階：觀測與逃生艙

### 掛 metrics

實作 `MetricsRecorder` 接你的指標系統（4 個方法），`WithMetrics` 掛上，
之後每個指令的延遲/錯誤 + 快取命中率自動被記，業務碼零改動：

```go
type promRecorder struct{ /* prometheus collectors */ }

func (p *promRecorder) ObserveLatency(cmd string, d time.Duration) { p.hist.WithLabelValues(cmd).Observe(d.Seconds()) }
func (p *promRecorder) IncError(cmd string)                        { p.errs.WithLabelValues(cmd).Inc() }
func (p *promRecorder) IncHit()                                    { p.hits.Inc() }
func (p *promRecorder) IncMiss()                                   { p.misses.Inc() }

client, _ := rediskit.New(rediskit.WithAddr(addr), rediskit.WithMetrics(&promRecorder{...}))
```

命中率 = `hit / (hit + miss)`；`IncError` 收到 `cache_backfill` 代表回填失敗（Redis 寫不進去），持續出現要查。

### Raw()：意圖 API 之外的 Redis 能力

```go
// ✗ 業務碼禁止這樣
client.Raw().SetBit(ctx, "bloom", pos, 1)

// ✓ 包進自己的基礎設施型別（像 labs/06 的 BloomFilter），Raw 呼叫點集中可 review
type BloomFilter struct{ rdb redis.UniversalClient }

func NewBloomFilter(c *rediskit.Client) *BloomFilter {
    return &BloomFilter{rdb: c.Raw()}
}
```

> **實際情境**：bitmap（布隆過濾器）、Stream、PubSub、SCAN 這些 rediskit 沒封。
> 走 `Raw()` 至少共享連線池和 metrics hook；但錯誤映射、namespace、序列化都沒有——
> 全自己來。每個新的 `Raw()` 呼叫點都該在 code review 被問一次「這真的進不了意圖 API？」

---

## 9. 測試你的業務碼

lib 自己 68 個單測全跑 miniredis；你的業務碼一樣搞：

```go
import "github.com/alicebob/miniredis/v2"

func TestUserService_GetUser(t *testing.T) {
    mr := miniredis.RunT(t) // in-process Redis，t.Cleanup 自動關

    client, err := rediskit.New(rediskit.WithAddr(mr.Addr()))
    if err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = client.Close() })

    svc := NewUserService(client.Cache(), fakeRepo)

    // ... 呼叫 svc，斷言結果

    // 殺手鐧 1：快轉時間測 TTL，不用 sleep
    mr.FastForward(11 * time.Minute)
    // ... 斷言快取已過期、會重新回源

    // 殺手鐧 2：模擬 Redis 掛掉測降級路徑
    mr.SetError("connection refused")
    // ... 斷言你的 fail-open / fail-close 行為
}
```

測限流不用等真實時間——注入假時鐘：

```go
clock := &fakeClock{now: time.Unix(1_700_000_000, 0)} // 自己寫 3 行的假時鐘
client, _ := rediskit.New(
    rediskit.WithAddr(mr.Addr()),
    rediskit.WithTimeSource(clock.Now),
)
rl := client.RateLimiter(3, time.Second)
// 打滿 3 次 → clock.Advance(time.Second) → 額度回來了，全程 0 sleep
```

> **實際情境**：CI 不用起 Docker、單測秒級跑完。
> 已知限制：miniredis 的 Lua 與真 Redis 有行為落差（docs/07 §6.1），
> lib 的四段 Lua 腳本要上 production 前建議對真 Redis 跑過一輪。

---

## 10. key 怎麼組：Build vs Qualify

```go
// 動態 id 組 key 片段 → Build（段內的 : 和 % 會跳脫，防碰撞）
kb := rediskit.NewKeyBuilder("")
key := kb.Build("user", userInput)   // userInput = "a:b" → "user:a%3Ab"，不會撞到別人

// 然後把片段交給各模組——namespace 和模組前綴（lock:/rl:/token:）lib 自動加
cache.Get(ctx, key, &u)              // 實際 key：app:user:a%3Ab
```

建議在業務 repo 開一個 key 目錄檔，全系統的 key 形狀集中一處、可盤點：

```go
// internal/cachekeys/keys.go
package cachekeys

var kb = rediskit.NewKeyBuilder("")

func User(id string) string          { return kb.Build("user", id) }
func Session(token string) string    { return kb.Build("session", token) }
func LoginRate(ip string) string     { return kb.Build("login", ip) }
```

> **實際情境**：維運半夜查線上問題，打開這一個檔就知道 Redis 裡有哪些 key、
> 格式長怎樣；參數有型別，不會把 orderID 塞進 `User()`。
> 直接在業務碼手拼 `"user:" + id` 是 docs/07 明文禁止的——id 帶冒號時會靜默撞 key。
