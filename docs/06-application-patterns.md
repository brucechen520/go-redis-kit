# Stage 6：應用層設計模式（Application Patterns）

> 模組：`github.com/twteam/go-redis-kit`　Go 1.26.1　Client：`github.com/redis/go-redis/v9`　輔助：`golang.org/x/sync/singleflight`
>
> 本階段大量重用 Stage 5 打造的分散式鎖（下文以 `lock.Mutex` 泛稱，對應 `rediskit.Lock`）。若你尚未讀完 Stage 5，請先回頭補齊「SET NX PX + Lua 安全解鎖 + fencing token」的觀念，再回來。

---

## 目錄

1. [本階段目標](#1-本階段目標)
2. [快取三大問題](#2-快取三大問題穿透擊穿雪崩一致性)
3. [限流四算法](#3-限流四算法)
4. [Token / 認證](#4-token--認證)
5. [其他實用模式](#5-其他實用模式)
6. [練習題 + 檢查點 + 延伸閱讀](#6-練習題--檢查點--延伸閱讀)

---

## 1. 本階段目標

前面幾個 Stage 我們把 Redis 當成「資料結構伺服器」在練：String、Hash、ZSet、Stream、Pipeline、Lua、分散式鎖。這些都是**原語（primitive）**。本階段要做的事情是：**把原語組裝成生產環境真正會用到的應用模式**。

學完本階段，你應該能回答以下面試 / 架構評審會被問爆的問題：

- 有人惡意用不存在的 ID 狂刷你的 API，DB 被打爆，怎麼辦？（快取穿透）
- 一個首頁熱點 key 剛好過期的那一瞬間，十萬 QPS 同時湧向 DB，怎麼辦？（快取擊穿）
- 半夜三點你設的一批快取「整齊地」同時過期，DB 瞬間雪崩，怎麼辦？（快取雪崩）
- 更新資料時到底要「更新快取」還是「刪除快取」？先動 DB 還是先動 cache？（快取一致性）
- 要限制每個使用者每分鐘只能打 100 次 API，四種算法怎麼選？（限流）
- 使用者登出後，舊 token 為何還能用？JWT 沒辦法主動撤銷怎麼破？（認證）
- 延遲隊列、排行榜分頁、分散式 ID、分散式 session 這些「redis 一招搞定」的小模式怎麼寫？

**本階段的教學節奏**：每個模式都用同一套結構拆解 ——

> **問題描述 → 解法原理 → redis-cli / Lua → Go 範例 → 專案流程步驟 → 坑（踩過才懂）**

程式碼一律使用 `go-redis/v9`，能跑、貼近生產。看完務必自己敲一遍。

---

## 2. 快取三大問題（穿透、擊穿、雪崩）+ 一致性

先建立一個共同心智模型。一個「讀快取」的標準流程長這樣：

```
       ┌─────────┐   hit    ┌──────────┐
Client │  Cache  ├─────────▶│  return  │
──────▶│ (Redis) │          └──────────┘
       └────┬────┘
            │ miss
            ▼
       ┌─────────┐          ┌──────────┐
       │   DB    ├─────────▶│ backfill │──▶ return
       └─────────┘          │  cache   │
                            └──────────┘
```

三大問題都是在攻擊這條路徑的不同環節：

| 問題 | 一句話 | 攻擊點 | 核心防法 |
| --- | --- | --- | --- |
| **穿透** Penetration | 查一個「本來就不存在」的資料 | miss 永遠打到 DB | 空值快取 / 布隆過濾器 |
| **擊穿** Breakdown | 單一熱 key 過期瞬間全打 DB | 熱點 key 的 miss | 互斥鎖重建 / 邏輯過期 / singleflight |
| **雪崩** Avalanche | 大量 key 同時失效 | 大範圍同時 miss | TTL 抖動 / 多級快取 / 熔斷 |

一致性則是另一個維度：資料被改了，快取怎麼跟上。

---

### 2.1 快取穿透（Cache Penetration）

#### 問題描述

「穿透」指的是查詢一個**資料庫裡根本不存在**的資料。因為 DB 沒有，所以永遠寫不進快取；快取永遠 miss；每一次請求都「穿透」快取直達 DB。

攻擊者最愛這招：拿 `id = -1`、`id = 999999999`、隨機 UUID 狂打你的 `/user/{id}`。你的快取一點忙都幫不上，DB 直接扛全部流量。

#### 解法原理

有兩條主線，通常**兩者合用**：

1. **空值快取（cache null）**：DB 查不到時，也往快取寫一個「空」標記（例如 `""` 或哨兵字串 `"__NULL__"`），並設**很短的 TTL**（30 秒 ~ 幾分鐘）。之後同一個不存在的 key 會命中這個空值，直接返回，不再打 DB。
   - 短 TTL 是為了避免「之後這筆資料真的被建立了」卻長時間查不到（資料被空值遮住）。
2. **布隆過濾器（Bloom Filter）**：在請求進 DB 前，先問布隆過濾器「這個 key **可能**存在嗎？」。布隆過濾器有一個關鍵特性——**沒有 false negative**：它說「不存在」就一定不存在，可以直接擋掉；它說「存在」則可能誤判（false positive），放行去查 DB 即可。用極小的記憶體攔截絕大多數非法 key。

#### redis-cli

空值快取（用哨兵值，避免和「真的存了空字串」混淆）：

```bash
# DB 查不到 user:404 → 寫入哨兵，TTL 60 秒
127.0.0.1:6379> SET user:404 "__NULL__" EX 60
OK
127.0.0.1:6379> GET user:404
"__NULL__"
127.0.0.1:6379> TTL user:404
(integer) 58
```

布隆過濾器（RedisBloom 模組，指令前綴 `BF.`）：

```bash
# 建立：預估 1,000,000 元素，誤判率 0.1%
127.0.0.1:6379> BF.RESERVE users:bloom 0.001 1000000
OK
# 資料建立時，把合法 id 加進去
127.0.0.1:6379> BF.ADD users:bloom 10086
(integer) 1
# 查詢：1=可能存在，0=一定不存在
127.0.0.1:6379> BF.EXISTS users:bloom 10086
(integer) 1
127.0.0.1:6379> BF.EXISTS users:bloom 999999999
(integer) 0
```

#### Go 範例（空值快取）

```go
package cachekit

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

const nullSentinel = "__NULL__" // 哨兵：代表「DB 確認不存在」

var ErrNotFound = errors.New("resource not found")

// GetUser 讀 user，帶空值快取防穿透。
func GetUser(ctx context.Context, rdb *redis.Client, id string) (string, error) {
	key := "user:" + id

	val, err := rdb.Get(ctx, key).Result()
	switch {
	case err == nil && val == nullSentinel:
		// 命中「不存在」標記，直接擋掉，不打 DB
		return "", ErrNotFound
	case err == nil:
		return val, nil // 正常命中
	case !errors.Is(err, redis.Nil):
		return "", err // Redis 真的壞了
	}

	// 快取 miss，回源 DB
	user, err := loadUserFromDB(ctx, id)
	if errors.Is(err, ErrNotFound) {
		// 關鍵：DB 也沒有 → 寫短 TTL 空值快取
		rdb.Set(ctx, key, nullSentinel, 60*time.Second)
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}

	rdb.Set(ctx, key, user, 10*time.Minute)
	return user, nil
}
```

#### Go 範例（布隆過濾器前置攔截）

```go
// 用 rdb.Do 直呼 RedisBloom 指令（go-redis 沒有專屬方法）
func mightExist(ctx context.Context, rdb *redis.Client, id string) (bool, error) {
	res, err := rdb.Do(ctx, "BF.EXISTS", "users:bloom", id).Int()
	if err != nil {
		return true, err // 布隆壞了就放行，別因為防護層擋掉正常流量
	}
	return res == 1, nil
}

func GetUserWithBloom(ctx context.Context, rdb *redis.Client, id string) (string, error) {
	ok, err := mightExist(ctx, rdb, id)
	if err == nil && !ok {
		return "", ErrNotFound // 布隆說一定不存在 → 連快取都不用查
	}
	return GetUser(ctx, rdb, id) // 可能存在 → 走正常快取流程
}
```

#### 專案流程步驟

1. 決定哨兵值（建議用不可能與正常資料衝突的字串，如 `"\x00NULL\x00"`）。
2. 在「回源 → DB 也 miss」的分支寫入短 TTL 空值。
3. 若非法流量佔比高（惡意攻擊場景），加掛布隆過濾器：資料寫入 DB 時同步 `BF.ADD`，讀取時先 `BF.EXISTS` 攔截。
4. 資料**新建**時，記得清掉可能存在的空值快取（`DEL user:<id>`），否則新資料會被舊空值遮蔽到 TTL 到期。

#### 坑

- **空值 TTL 太長**：資料建立後很久查不到。控制在幾十秒到幾分鐘。
- **哨兵值與業務值衝突**：如果業務本身允許存空字串，別用 `""` 當哨兵，要用不可能出現的魔術字串。
- **布隆過濾器不能刪元素**（標準 BF 不支援 remove，刪了會誤傷其他 key）。要能刪就用 Cuckoo Filter（`CF.*`）或定期重建。
- **布隆的容量與誤判率要預估準**：塞爆後誤判率飆升，攔截效果崩壞。
- **降級要向「放行」**：布隆 / Redis 掛掉時，寧可放行去查 DB，也不要因為防護層故障就把正常使用者擋在門外。

---

### 2.2 快取擊穿（Cache Breakdown / Hotspot Invalid）

#### 問題描述

「擊穿」是**單一熱點 key** 的問題：某個 key（例如首頁 banner、爆款商品）承載了巨大 QPS，它**過期的那一瞬間**，所有請求同時 miss，同時湧向 DB 去重建這一個 key。DB 瞬間被同一條查詢打了幾萬次。

和穿透的差別：穿透是「資料不存在」，擊穿是「資料存在且很熱，只是剛好過期」。

#### 解法原理

三種主流解法，各有取捨：

1. **互斥鎖重建（mutex rebuild）**：miss 時，只允許**一個**執行緒拿到分散式鎖去查 DB 重建快取，其餘執行緒**短暫等待後重試讀快取**。用 Stage 5 的鎖即可。優點：實作直觀、不會有髒資料；缺點：等待的執行緒被阻塞、鎖本身要處理超時。
2. **邏輯過期（logical expiration，不真過期）**：key **永不設 TTL**，改在 value 裡塞一個 `expireAt` 欄位。讀到「邏輯上已過期」的資料時，**先返回舊資料（stale）**，同時**背景異步**拿鎖去重建。優點：讀請求**永不阻塞**、永遠有資料可回；缺點：短時間會讀到舊資料、實作較複雜、key 不會自動被 Redis 清掉。
3. **singleflight 合併回源**：這是**行程內（in-process）**的手段。同一個 Go 行程裡，對同一個 key 的並發回源請求會被 `golang.org/x/sync/singleflight` 合併成**一次** DB 查詢，其餘 goroutine 共享結果。優點：零 Redis 往返、極輕量；缺點：只能合併**單機**內的並發，跨機器還是要靠分散式鎖。

**生產最佳實踐 = singleflight（擋單機並發）+ 分散式鎖（擋跨機並發）+ TTL 抖動（防雪崩）三者合體。**

#### redis-cli / Lua（互斥鎖重建的鎖，沿用 Stage 5）

```bash
# 嘗試搶「重建鎖」：SET NX PX
127.0.0.1:6379> SET lock:rebuild:hot_key <token> NX PX 3000
OK          # 搶到 → 我去查 DB
(nil)       # 沒搶到 → sleep 一下再讀快取
```

Stage 5 的安全解鎖 Lua（比對 token 才刪，避免刪到別人的鎖）：

```lua
-- KEYS[1]=鎖 key, ARGV[1]=我的 token
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
```

#### 邏輯過期的 value 結構

value 存 JSON，把過期時間包進去，**Redis key 本身不設 TTL**：

```json
{ "data": "{...真正的業務資料...}", "expireAt": 1751894400 }
```

#### Go 範例（完整 `GetOrLoad`：singleflight + 分散式鎖 + TTL 抖動）

這是本節的重頭戲，把三種手段組在一起，是可以直接搬進專案的通用快取讀取器。

```go
package cachekit

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// Loader 為快取 miss 時回源 DB 的函式。
type Loader func(ctx context.Context) (string, error)

// Cache 封裝一個帶完整防護的讀取器。
type Cache struct {
	rdb *redis.Client
	sf  singleflight.Group // 單機並發合併
	// 依賴 Stage 5 的分散式鎖
	locker interface {
		// TryLock 搶鎖，回傳 (是否搶到, unlock 函式, error)
		TryLock(ctx context.Context, key string, ttl time.Duration) (bool, func(context.Context) error, error)
	}
	ttl    time.Duration // 基礎 TTL
	jitter time.Duration // TTL 抖動幅度（防雪崩，見 2.3）
}

// jitteredTTL 在基礎 TTL 上加一段隨機抖動，避免大量 key 同時過期。
func (c *Cache) jitteredTTL() time.Duration {
	if c.jitter <= 0 {
		return c.ttl
	}
	return c.ttl + time.Duration(rand.Int63n(int64(c.jitter)))
}

// GetOrLoad：讀快取，miss 時安全重建。
func (c *Cache) GetOrLoad(ctx context.Context, key string, load Loader) (string, error) {
	// 1) 先讀快取
	if v, err := c.rdb.Get(ctx, key).Result(); err == nil {
		if v == nullSentinel {
			return "", ErrNotFound
		}
		return v, nil
	} else if !errors.Is(err, redis.Nil) {
		return "", err
	}

	// 2) miss：用 singleflight 把「本機」對同一 key 的並發回源合併成一次
	v, err, _ := c.sf.Do(key, func() (any, error) {
		return c.rebuild(ctx, key, load)
	})
	if err != nil {
		return "", err
	}
	if s := v.(string); s == nullSentinel {
		return "", ErrNotFound
	}
	return v.(string), nil
}

// rebuild：跨機器層級用分散式鎖，確保全叢集只有一個節點打 DB。
func (c *Cache) rebuild(ctx context.Context, key string, load Loader) (string, error) {
	lockKey := "lock:rebuild:" + key

	locked, unlock, err := c.locker.TryLock(ctx, lockKey, 5*time.Second)
	if err != nil {
		return "", err
	}

	if !locked {
		// 沒搶到鎖：別人正在重建 → 短暫 backoff 後再讀一次快取
		time.Sleep(50 * time.Millisecond)
		if v, err := c.rdb.Get(ctx, key).Result(); err == nil {
			return v, nil // 別人重建好了
		}
		// 還沒好也別死等，這次直接回源（退化為多打一次 DB，可接受）
		return c.loadAndSet(ctx, key, load)
	}
	defer unlock(ctx)

	// 搶到鎖：double-check，可能在搶鎖期間別人已經填好快取
	if v, err := c.rdb.Get(ctx, key).Result(); err == nil {
		return v, nil
	}
	return c.loadAndSet(ctx, key, load)
}

// loadAndSet：真正查 DB 並回填快取（含空值快取 + TTL 抖動）。
func (c *Cache) loadAndSet(ctx context.Context, key string, load Loader) (string, error) {
	data, err := load(ctx)
	if errors.Is(err, ErrNotFound) {
		c.rdb.Set(ctx, key, nullSentinel, 60*time.Second) // 防穿透
		return nullSentinel, nil
	}
	if err != nil {
		return "", err
	}
	c.rdb.Set(ctx, key, data, c.jitteredTTL()) // 防雪崩
	return data, nil
}
```

> **為什麼要 singleflight + 鎖兩層？** singleflight 只作用在**同一個 Go 行程**內。你的服務通常跑 N 個 Pod，singleflight 能把每個 Pod 的並發壓成「每 Pod 一次」，但 N 個 Pod 仍會有 N 次 DB 回源。分散式鎖再把這 N 次壓成**全叢集一次**。兩層各司其職。

#### Go 範例（邏輯過期，讀不阻塞）

```go
type logicalValue struct {
	Data     string `json:"data"`
	ExpireAt int64  `json:"expireAt"` // Unix 秒
}

// GetLogical：邏輯過期。永遠先回舊值，過期則背景重建。
func (c *Cache) GetLogical(ctx context.Context, key string, load Loader) (string, error) {
	raw, err := c.rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		// 冷啟動：第一次沒資料，只能同步建一次
		return c.rebuildLogical(ctx, key, load)
	} else if err != nil {
		return "", err
	}

	var lv logicalValue
	_ = json.Unmarshal([]byte(raw), &lv)

	if time.Now().Unix() < lv.ExpireAt {
		return lv.Data, nil // 邏輯上還新鮮
	}

	// 邏輯上過期：立刻回舊值，另開 goroutine 拿鎖重建
	go func() {
		bg := context.Background()
		locked, unlock, err := c.locker.TryLock(bg, "lock:logical:"+key, 5*time.Second)
		if err != nil || !locked {
			return // 沒搶到就算了，別人會建
		}
		defer unlock(bg)
		_, _ = c.rebuildLogical(bg, key, load)
	}()

	return lv.Data, nil // 讀請求永不阻塞
}

func (c *Cache) rebuildLogical(ctx context.Context, key string, load Loader) (string, error) {
	data, err := load(ctx)
	if err != nil {
		return "", err
	}
	lv := logicalValue{Data: data, ExpireAt: time.Now().Add(c.ttl).Unix()}
	b, _ := json.Marshal(lv)
	c.rdb.Set(ctx, key, b, 0) // TTL=0：Redis 永不物理過期
	return data, nil
}
```

#### 專案流程步驟

1. 一般讀取用 `GetOrLoad`（singleflight + 鎖 + 抖動），涵蓋 90% 場景。
2. 對「絕對不能阻塞、可容忍短暫舊資料」的超熱 key（首頁、榜單），改用 `GetLogical` 邏輯過期。
3. 兩者都要在 `loadAndSet` 保留空值快取分支，順手把穿透也防了。
4. 壓測驗證：把某 key 手動 `DEL`，同時打 1 萬並發，觀察 DB 只被查一次（或每 Pod 一次）。

#### 坑

- **只用 singleflight 不用鎖**：多 Pod 場景照樣擊穿 DB N 次。
- **只用鎖不設 double-check**：搶到鎖後不重讀快取，會多打一次 DB。
- **鎖 TTL < DB 查詢耗時**：重建還沒好鎖就過期，第二個節點進來又打一次，甚至互相踩。鎖 TTL 要 > 最慢的一次回源。
- **邏輯過期的 key 永不物理過期**：Redis 記憶體只增不減，要有淘汰策略（`maxmemory-policy allkeys-lru`）或定期清理冷 key。
- **`sf.Do` 的錯誤會被所有等待者共享**：一次回源失敗，該批全部拿到同一個 error。若要讓失敗者各自重試，考慮 `sf.Forget(key)`。
- **背景 goroutine 洩漏 / panic**：邏輯過期的 `go func()` 一定要 `recover` 並帶自己的 `context`，別用會被請求取消的 ctx。

---

### 2.3 快取雪崩（Cache Avalanche）

#### 問題描述

「雪崩」是**大範圍 key 同時失效**的問題。典型成因：

- 系統啟動 / 大批預熱時，一次性把一萬個 key 都設成 `TTL = 3600s`，一小時後它們**整整齊齊一起過期**，DB 瞬間被全量回源打爆。
- Redis 整個掛掉（實例故障），所有快取瞬間全失效，全部流量直落 DB。

和擊穿的差別：擊穿是**一個**熱 key，雪崩是**一大片** key。

#### 解法原理

三管齊下：

1. **TTL 抖動（jitter）**：設 TTL 時加一段隨機值，`TTL = base + rand(0, jitter)`，把「同時過期」打散成「一段時間內陸續過期」。**最便宜、最有效、必做。**
2. **多級快取（multi-level）**：本地快取（行程內 LRU，如 `ristretto` / `groupcache`）＋ Redis 兩層。Redis 掛了還有本地快取頂一陣子；本地快取還能擋掉大量重複讀取，減輕 Redis 壓力。
3. **熔斷 / 降級（circuit breaker）**：偵測到 DB 回源大量失敗 / 延遲飆高時，**主動熔斷**，短時間內對回源請求直接返回降級資料（舊值、預設值、友善錯誤），給 DB 喘息時間，避免「越慢越重試、越重試越慢」的死亡螺旋。

#### redis-cli（TTL 抖動示意）

```bash
# 不要這樣（大家同時過期）：
127.0.0.1:6379> MSET item:1 a item:2 b item:3 c
127.0.0.1:6379> EXPIRE item:1 3600
127.0.0.1:6379> EXPIRE item:2 3600
127.0.0.1:6379> EXPIRE item:3 3600

# 要這樣（各自加隨機抖動，例如 3600 + 0~600 秒）：
127.0.0.1:6379> SET item:1 a EX 3712
127.0.0.1:6379> SET item:2 b EX 3899
127.0.0.1:6379> SET item:3 c EX 3654
```

#### Go 範例（TTL 抖動 + 簡易熔斷）

TTL 抖動已在 2.2 的 `jitteredTTL()` 給出。這裡補一個極簡熔斷器骨架：

```go
package cachekit

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("circuit breaker open: serving degraded")

// Breaker：極簡熔斷器。連續失敗達門檻 → 開路，冷卻期內直接拒絕回源。
type Breaker struct {
	mu           sync.Mutex
	failures     int
	threshold    int           // 連續失敗幾次開路
	cooldown     time.Duration // 開路後多久嘗試半開
	openedAt     time.Time
	open         bool
}

func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{threshold: threshold, cooldown: cooldown}
}

// Allow：回源前呼叫，判斷是否放行。
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open {
		return true
	}
	if time.Since(b.openedAt) > b.cooldown {
		b.open = false // 半開：放一個試探請求
		b.failures = 0
		return true
	}
	return false
}

func (b *Breaker) Record(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if success {
		b.failures = 0
		b.open = false
		return
	}
	b.failures++
	if b.failures >= b.threshold {
		b.open = true
		b.openedAt = time.Now()
	}
}

// GetOrLoadWithBreaker：在回源前套上熔斷。
func (c *Cache) GetOrLoadWithBreaker(
	ctx context.Context, key string, load Loader, br *Breaker, fallback string,
) (string, error) {
	if v, err := c.rdb.Get(ctx, key).Result(); err == nil {
		return v, nil
	}
	if !br.Allow() {
		return fallback, ErrCircuitOpen // 熔斷中：回降級值，保護 DB
	}
	v, err := c.loadAndSet(ctx, key, load)
	br.Record(err == nil)
	if err != nil {
		return fallback, err
	}
	return v, nil
}
```

#### 專案流程步驟

1. **所有** `SET ... EX` 一律走 `jitteredTTL()`，抖動幅度取基礎 TTL 的 10%~20%。
2. 對讀多寫少、可容忍秒級不一致的資料，前面加一層本地 LRU 快取。
3. 回源路徑統一套熔斷器；熔斷開路時返回預先準備好的降級資料。
4. 冷啟動預熱時，刻意把 TTL 打散（別用同一個常數）。
5. Redis 部署要有高可用（Sentinel / Cluster），避免「單點掛掉 = 全站雪崩」。

#### 坑

- **抖動幅度太小**：`rand(0,10s)` 對 3600s 的基礎 TTL 幾乎沒用，過期還是擠在一起。幅度要夠大。
- **本地快取一致性更難**：本地快取無法被主動刪，只能靠短 TTL 或訂閱失效事件（Pub/Sub 廣播刪除）。
- **熔斷門檻設太敏感**：偶發抖動就開路，正常請求被大量降級，體驗變差。
- **熔斷降級值過期 / 誤導**：降級返回的舊資料要標記清楚，別讓上游誤以為是最新值做出錯誤決策。
- **只顧快取層雪崩，忘了連線池雪崩**：DB 連線池打滿也會連鎖崩，回源要配合 DB 側的限流 / 連線上限。

---

### 2.4 快取一致性（Cache Consistency）

#### 問題描述

資料同時活在 DB 和 Redis 兩處。一旦資料被修改，兩邊如何同步？只要是「雙寫」，就存在時序問題，永遠無法做到**強一致**，我們追求的是「**最終一致 + 不一致窗口盡量小**」。

四種組合，先記結論：**業界標準是 Cache Aside 的「先更 DB，後刪 cache」**。

#### 解法原理

**Cache Aside（旁路快取）模式**：

- **讀**：先讀 cache，miss 則讀 DB 並回填 cache（就是前面的 `GetOrLoad`）。
- **寫**：先更新 DB，**然後刪除（而非更新）cache**。

##### 為什麼是「刪」不是「更新」？

1. **省算力**：如果每次寫都重算 cache，但這筆資料寫完後根本沒人讀，就白算了。刪除採「lazy」策略——下次真的有人讀時才重建，用不到就不建。
2. **避免並發寫覆蓋**：兩個並發寫 A、B。若採「更新 cache」，DB 的寫入順序和 cache 的寫入順序可能不一致（A 先寫 DB、B 後寫 DB，但 B 先更 cache、A 後更 cache），cache 就留下了 A 的舊值——髒資料。刪除就沒這問題，反正下次讀會從 DB 拉最新。

##### 為什麼是「先更 DB，後刪 cache」而非「先刪 cache，後更 DB」？

考慮「先刪 cache 後更 DB」的並發：

```
T1: 刪 cache
T2: 讀 cache（miss）→ 讀 DB（讀到舊值）→ 回填 cache（舊值！）
T1: 更新 DB（新值）
結果：cache 是舊值，DB 是新值 → 長期不一致（直到 TTL）
```

改成「先更 DB 後刪 cache」，這個窗口就小得多：

```
T1: 更新 DB（新值）
T1: 刪 cache
T2: 讀 cache（miss）→ 讀 DB（讀到新值）→ 回填 cache（新值）✓
```

##### 「先更 DB 後刪 cache」仍有的短暫不一致

理論上還有一個極小機率的窗口：

```
T2: 讀 cache（剛好 miss，例如剛過期）→ 讀 DB（舊值）
T1: 更新 DB（新值）→ 刪 cache
T2: 回填 cache（舊值！）
```

要湊齊「讀操作的 DB 查詢**早於**寫、但 cache 回填**晚於**寫的刪除」時序很難（讀通常比寫快），機率極低。但要更保險，就上**延遲雙刪**。

##### 延遲雙刪（Delayed Double Delete）

流程：**刪 cache → 更 DB → 睡一小段（如 500ms~1s）→ 再刪一次 cache**。第二次刪除是為了清掉「更新 DB 期間，可能被其他讀請求用舊值回填的 cache」。睡的時間要略大於「一次讀操作的耗時」。

#### redis-cli（Cache Aside 寫路徑）

```bash
# 寫：先更 DB（應用層做），再刪 cache
127.0.0.1:6379> DEL user:10086
(integer) 1
```

#### Go 範例（Cache Aside + 延遲雙刪）

```go
// UpdateUser：Cache Aside 寫路徑。
func UpdateUser(ctx context.Context, rdb *redis.Client, id, newVal string) error {
	key := "user:" + id

	// 1) 先更新 DB（真相來源）
	if err := updateUserInDB(ctx, id, newVal); err != nil {
		return err
	}

	// 2) 刪除 cache（不是更新！）
	if err := rdb.Del(ctx, key).Err(); err != nil {
		// 刪快取失敗要重試或送進「重試隊列」，否則會殘留髒資料
		return fmt.Errorf("db updated but cache delete failed: %w", err)
	}
	return nil
}

// UpdateUserDoubleDelete：延遲雙刪，進一步壓縮不一致窗口。
func UpdateUserDoubleDelete(ctx context.Context, rdb *redis.Client, id, newVal string) error {
	key := "user:" + id

	// 第一刪：清掉可能存在的舊 cache
	_ = rdb.Del(ctx, key).Err()

	// 更新 DB
	if err := updateUserInDB(ctx, id, newVal); err != nil {
		return err
	}

	// 第二刪：延遲執行，清掉「更 DB 期間被舊值回填」的 cache
	go func() {
		time.Sleep(500 * time.Millisecond) // 略大於一次讀操作耗時
		_ = rdb.Del(context.Background(), key).Err()
	}()
	return nil
}
```

> **刪快取失敗怎麼辦？** 生產級做法是把「刪快取」動作丟進可靠重試機制：可用訂閱 DB binlog（如 Canal）→ 發訊息 → 消費者刪快取，或寫入本地重試隊列。核心是保證「DB 更新了，cache 最終一定會被刪掉」。

#### 專案流程步驟

1. 讀走 `GetOrLoad`，寫走 `UpdateUser`（先 DB 後刪 cache）。
2. 對一致性要求較高的資料，寫路徑升級為延遲雙刪。
3. 刪快取務必有失敗重試（訊息隊列 / binlog 訂閱），別 fire-and-forget 就當成功。
4. 所有 cache key 都要有 TTL 作為「最終兜底」——即使刪除全失敗，TTL 到了也會自動修正。

#### 坑

- **先刪 cache 後更 DB**：如前所述，並發下留舊值，別這樣寫。
- **用「更新 cache」取代「刪 cache」**：並發寫覆蓋出髒值。
- **刪快取 fire-and-forget**：刪失敗沒重試 → 髒資料殘留到 TTL。
- **延遲雙刪的 sleep 阻塞主流程**：務必放背景 goroutine，且帶獨立 context。
- **忘了給 TTL**：一旦刪除機制失效，沒有 TTL 兜底就是永久髒資料。
- **跨服務多級快取的一致性**：本地快取刪不掉，需靠 Pub/Sub 廣播失效事件，複雜度陡增。

---

## 3. 限流四算法

限流（Rate Limiting）：限制單位時間內的請求數，保護後端。四種經典算法，各有精度、記憶體、突發容忍度的取捨。

### 3.1 固定窗口（Fixed Window）

#### 問題描述 / 解法原理

把時間切成固定長度的窗口（如每分鐘），每個窗口一個計數器。請求來就 `INCR`，超過上限就拒絕。窗口結束計數歸零（靠 key 過期）。

最簡單，但有**邊界雙倍突發**問題（見坑）。

#### redis-cli / Lua

用 `INCR` + `EXPIRE`，key 帶上窗口時間戳：

```bash
# 每分鐘限 100 次，key = rl:user:42:<當前分鐘>
127.0.0.1:6379> INCR rl:user:42:29142857
(integer) 1
127.0.0.1:6379> EXPIRE rl:user:42:29142857 60 NX
(integer) 1
```

`INCR` 和 `EXPIRE` 兩步非原子，用 Lua 合成一步（避免只 INCR 沒設到過期造成計數器永生）：

```lua
-- KEYS[1]=計數器 key, ARGV[1]=limit, ARGV[2]=window 秒
local current = redis.call("INCR", KEYS[1])
if current == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[2])
end
if current > tonumber(ARGV[1]) then
    return 0   -- 拒絕
end
return 1       -- 放行
```

#### Go 範例

```go
var fixedWindowScript = redis.NewScript(`
local current = redis.call("INCR", KEYS[1])
if current == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[2])
end
if current > tonumber(ARGV[1]) then
    return 0
end
return 1
`)

func AllowFixedWindow(ctx context.Context, rdb *redis.Client, userID string, limit int, window time.Duration) (bool, error) {
	slot := time.Now().Unix() / int64(window.Seconds())
	key := fmt.Sprintf("rl:fw:%s:%d", userID, slot)
	res, err := fixedWindowScript.Run(ctx, rdb, []string{key}, limit, int(window.Seconds())).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}
```

#### 坑

- **邊界雙倍突發**：limit=100/分鐘。使用者在 00:59 打 100 次、01:00 打 100 次，跨越窗口邊界的 1 秒內實際放行了 **200 次**，是設定的兩倍。這是固定窗口的致命缺陷，對突發敏感的場景別用。
- **INCR/EXPIRE 非原子**：一定要用 Lua 綁在一起，否則 EXPIRE 沒設成功計數器永不歸零。

---

### 3.2 滑動窗口（Sliding Window，ZSet）

#### 問題描述 / 解法原理

為了消除固定窗口的邊界突發，滑動窗口精確記錄「過去 N 秒內」每一次請求的時間戳。用 ZSet：**member = 請求唯一 id，score = 請求時間戳**。每次請求：

1. `ZREMRANGEBYSCORE` 移除窗口外（過期）的成員。
2. `ZCARD` 數目前窗口內有幾個。
3. 未超限就 `ZADD` 記錄本次，超限就拒絕。

精度最高（真正的「滑動」），代價是**記憶體隨請求量線性增長**（每個請求存一個 member）。

#### redis-cli / Lua

```bash
# 窗口 60 秒，now=1751894400000（毫秒）
127.0.0.1:6379> ZREMRANGEBYSCORE rl:sw:user:42 0 1751894340000
127.0.0.1:6379> ZCARD rl:sw:user:42
(integer) 87
127.0.0.1:6379> ZADD rl:sw:user:42 1751894400000 1751894400000-abc123
```

原子化 Lua：

```lua
-- KEYS[1]=zset key
-- ARGV[1]=now(ms), ARGV[2]=window(ms), ARGV[3]=limit, ARGV[4]=member
local clearBefore = tonumber(ARGV[1]) - tonumber(ARGV[2])
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, clearBefore)
local count = redis.call("ZCARD", KEYS[1])
if count < tonumber(ARGV[3]) then
    redis.call("ZADD", KEYS[1], ARGV[1], ARGV[4])
    redis.call("PEXPIRE", KEYS[1], ARGV[2]) -- 讓閒置 key 自動回收
    return 1
end
return 0
```

#### Go 範例

```go
var slidingWindowScript = redis.NewScript(`
local clearBefore = tonumber(ARGV[1]) - tonumber(ARGV[2])
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, clearBefore)
local count = redis.call("ZCARD", KEYS[1])
if count < tonumber(ARGV[3]) then
    redis.call("ZADD", KEYS[1], ARGV[1], ARGV[4])
    redis.call("PEXPIRE", KEYS[1], ARGV[2])
    return 1
end
return 0
`)

func AllowSlidingWindow(ctx context.Context, rdb *redis.Client, userID string, limit int, window time.Duration) (bool, error) {
	now := time.Now().UnixMilli()
	key := "rl:sw:" + userID
	member := fmt.Sprintf("%d-%d", now, rand.Int63()) // 唯一
	res, err := slidingWindowScript.Run(ctx, rdb, []string{key},
		now, window.Milliseconds(), limit, member).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}
```

#### 坑

- **記憶體爆炸**：高 QPS 下 ZSet 成員暴增。務必 `PEXPIRE` 讓閒置 key 回收，並評估單 key 最壞成員數。
- **member 必須唯一**：同毫秒多請求若 member 相同會覆蓋（少算），要加隨機後綴。
- **仍是近似**：這是「滑動窗口計數」，不是「滑動窗口日誌」的完全精確版，但對絕大多數場景夠用。

---

### 3.3 令牌桶（Token Bucket，Lua）★最推薦

#### 問題描述 / 解法原理

想像一個容量固定的桶，以**固定速率**往裡加令牌（token），桶滿則溢出。每個請求要**拿走一個令牌**才能通過，桶空則拒絕。

- **平滑速率**：令牌按 `rate` 勻速補充，長期平均速率被限住。
- **容忍突發**：桶裡可以攢下最多 `capacity` 個令牌，所以允許一次性的突發（把攢的令牌一次用掉），但突發後又回歸勻速。這個「既限速又容突發」的特性讓它成為**最實用**的限流算法。

實作技巧：不需要背景執行緒真的去加令牌。用**惰性計算**——記錄「上次補充時間」和「當前令牌數」，每次請求時根據「距上次過了多久 × rate」算出這段時間該補多少，再判斷夠不夠扣。所有邏輯用一段 Lua 原子完成。

#### 完整 Lua 腳本

```lua
-- 令牌桶限流（惰性補充版）
-- KEYS[1] = 桶的 key（一個 hash，存 tokens 與 last_refill_ms）
-- ARGV[1] = capacity   桶容量（最大令牌數）
-- ARGV[2] = rate       每秒補充令牌數
-- ARGV[3] = now_ms     當前時間（毫秒）
-- ARGV[4] = requested  本次要拿幾個令牌（通常 1）
-- 回傳: {allowed(1/0), remaining_tokens}

local capacity  = tonumber(ARGV[1])
local rate      = tonumber(ARGV[2])
local now       = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local bucket = redis.call("HMGET", KEYS[1], "tokens", "last")
local tokens = tonumber(bucket[1])
local last   = tonumber(bucket[2])

if tokens == nil then
    -- 初始化：桶滿
    tokens = capacity
    last   = now
end

-- 依經過時間補充令牌（惰性 refill）
local elapsed = math.max(0, now - last) / 1000.0
tokens = math.min(capacity, tokens + elapsed * rate)

local allowed = 0
if tokens >= requested then
    tokens  = tokens - requested
    allowed = 1
end

-- 寫回狀態，並設過期（桶裝滿所需時間即為安全上限）
redis.call("HMSET", KEYS[1], "tokens", tokens, "last", now)
redis.call("PEXPIRE", KEYS[1], math.ceil(capacity / rate * 1000))

return { allowed, tokens }
```

#### Go 呼叫

```go
var tokenBucketScript = redis.NewScript(`
local capacity  = tonumber(ARGV[1])
local rate      = tonumber(ARGV[2])
local now       = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local bucket = redis.call("HMGET", KEYS[1], "tokens", "last")
local tokens = tonumber(bucket[1])
local last   = tonumber(bucket[2])
if tokens == nil then tokens = capacity; last = now end
local elapsed = math.max(0, now - last) / 1000.0
tokens = math.min(capacity, tokens + elapsed * rate)
local allowed = 0
if tokens >= requested then tokens = tokens - requested; allowed = 1 end
redis.call("HMSET", KEYS[1], "tokens", tokens, "last", now)
redis.call("PEXPIRE", KEYS[1], math.ceil(capacity / rate * 1000))
return { allowed, tokens }
`)

// TokenBucketResult 承載腳本回傳。
type TokenBucketResult struct {
	Allowed   bool
	Remaining float64
}

func AllowTokenBucket(
	ctx context.Context, rdb *redis.Client, key string,
	capacity int, ratePerSec float64, n int,
) (TokenBucketResult, error) {
	now := time.Now().UnixMilli()
	res, err := tokenBucketScript.Run(ctx, rdb, []string{"rl:tb:" + key},
		capacity, ratePerSec, now, n).Slice()
	if err != nil {
		return TokenBucketResult{}, err
	}
	allowed, _ := res[0].(int64)
	// 令牌數可能是 int64 或字串（依 Redis 版本），保險轉換
	var remaining float64
	switch v := res[1].(type) {
	case int64:
		remaining = float64(v)
	case string:
		remaining, _ = strconv.ParseFloat(v, 64)
	}
	return TokenBucketResult{Allowed: allowed == 1, Remaining: remaining}, nil
}
```

#### 坑

- **Lua 回傳型別不穩**：`tokens` 是浮點，Redis 經 Lua 回傳浮點常被截成整數或字串，Go 側要做型別判斷（如上）。要精確就把 tokens 乘以固定倍數存成整數。
- **時鐘來源**：用 Redis 伺服器時間（`redis.call("TIME")`）比用各客戶端的 `now` 更一致，避免客戶端時鐘漂移影響補充計算。
- **capacity 與 rate 的語義**：capacity 決定「最大突發量」，rate 決定「長期平均速率」，兩者分開調。突發敏感就把 capacity 壓小。

---

### 3.4 漏桶（Leaky Bucket）

#### 問題描述 / 解法原理

水（請求）倒進桶裡，桶以**恆定速率**從底部漏出（被處理）。桶滿則溢出（拒絕）。

和令牌桶的關鍵差別：**漏桶強制「恆定流出速率」，完全不容忍突發**——不管進來多猛，處理端永遠勻速。適合「保護一個吞吐固定的下游」（如寫入一個 QPS 上限硬性固定的第三方 API）。令牌桶容忍突發，漏桶抹平突發。

實作上常用一個佇列 + 勻速消費者；用 Redis 也可用「惰性計算剩餘水位」的方式（和令牌桶對偶）。

#### redis-cli / Lua（惰性水位版）

```lua
-- 漏桶：KEYS[1]=hash(water,last)
-- ARGV[1]=capacity 桶容量, ARGV[2]=leakRate 每秒漏出, ARGV[3]=now_ms
local capacity = tonumber(ARGV[1])
local leakRate = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])

local b = redis.call("HMGET", KEYS[1], "water", "last")
local water = tonumber(b[1]) or 0
local last  = tonumber(b[2]) or now

-- 依經過時間漏掉一部分水
local leaked = (now - last) / 1000.0 * leakRate
water = math.max(0, water - leaked)

local allowed = 0
if water + 1 <= capacity then
    water = water + 1  -- 這滴水裝得下
    allowed = 1
end
redis.call("HMSET", KEYS[1], "water", water, "last", now)
redis.call("PEXPIRE", KEYS[1], math.ceil(capacity / leakRate * 1000))
return allowed
```

#### Go 範例

```go
var leakyBucketScript = redis.NewScript(`
local capacity = tonumber(ARGV[1])
local leakRate = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local b = redis.call("HMGET", KEYS[1], "water", "last")
local water = tonumber(b[1]) or 0
local last  = tonumber(b[2]) or now
local leaked = (now - last) / 1000.0 * leakRate
water = math.max(0, water - leaked)
local allowed = 0
if water + 1 <= capacity then water = water + 1; allowed = 1 end
redis.call("HMSET", KEYS[1], "water", water, "last", now)
redis.call("PEXPIRE", KEYS[1], math.ceil(capacity / leakRate * 1000))
return allowed
`)

func AllowLeakyBucket(ctx context.Context, rdb *redis.Client, key string, capacity int, leakRate float64) (bool, error) {
	now := time.Now().UnixMilli()
	res, err := leakyBucketScript.Run(ctx, rdb, []string{"rl:lb:" + key}, capacity, leakRate, now).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}
```

#### 坑

- **漏桶不放行突發**：如果你的業務其實希望「平時攢額度、偶爾爆發」，漏桶會體驗很差，該選令牌桶。
- **純 Redis 惰性版其實逼近令牌桶**：真正「勻速流出到下游」需要一個實際的消費端隊列，Redis 只算了「能不能進桶」，出桶的勻速要由消費者保證。

---

### 3.5 四算法對比表 + 選型建議

| 算法 | 精度 | 記憶體 | 容忍突發 | 平滑度 | 實作複雜度 | 邊界問題 |
| --- | --- | --- | --- | --- | --- | --- |
| 固定窗口 Fixed Window | 低 | O(1) | 差（邊界雙倍） | 差 | ★ 極簡 | 有（雙倍突發） |
| 滑動窗口 Sliding Window | 高 | O(N) 隨量長 | 中 | 中 | ★★ | 無 |
| 令牌桶 Token Bucket | 高 | O(1) | **好**（可攢） | 好 | ★★★ | 無 |
| 漏桶 Leaky Bucket | 高 | O(1) | 差（強制勻速） | **最好** | ★★★ | 無 |

**選型建議**：

- **一般 API 限流、要容忍合理突發** → **令牌桶**（首選，記憶體 O(1) 又能攢額度）。
- **要求「絕對勻速流出、保護脆弱下游」** → **漏桶**。
- **要求精確「過去 N 秒內不超過 M 次」且量不大** → **滑動窗口 ZSet**。
- **內部服務、量大、可容忍邊界瑕疵、追求極簡** → **固定窗口**。
- 不確定就選令牌桶，它是綜合體驗最好的預設值。

---

## 4. Token / 認證

### 4.1 Token → User 映射（有狀態 session token）

#### 問題描述 / 解法原理

最傳統的認證：登入成功後產生一個不透明（opaque）的隨機 token，在 Redis 存 `token → userID`（+ 過期）。每次請求帶 token，服務端查 Redis 換出 userID。**登出 = 刪掉這個 key**，立刻失效，天然支援主動撤銷。

#### redis-cli

```bash
# 登入：token → userID，TTL 7 天
127.0.0.1:6379> SET session:eyJ...abc 10086 EX 604800
OK
# 驗證
127.0.0.1:6379> GET session:eyJ...abc
"10086"
# 登出：直接刪
127.0.0.1:6379> DEL session:eyJ...abc
(integer) 1
```

#### Go 範例

```go
package authkit

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrInvalidToken = errors.New("invalid or expired token")

func newToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// Login 產生 session token 並存映射。
func Login(ctx context.Context, rdb *redis.Client, userID string, ttl time.Duration) (string, error) {
	token := newToken()
	if err := rdb.Set(ctx, "session:"+token, userID, ttl).Err(); err != nil {
		return "", err
	}
	return token, nil
}

// Authenticate 用 token 換 userID，順手續期（sliding session）。
func Authenticate(ctx context.Context, rdb *redis.Client, token string, ttl time.Duration) (string, error) {
	key := "session:" + token
	userID, err := rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrInvalidToken
	}
	if err != nil {
		return "", err
	}
	rdb.Expire(ctx, key, ttl) // 每次活動續期
	return userID, nil
}

// Logout 刪除 session，token 立即失效。
func Logout(ctx context.Context, rdb *redis.Client, token string) error {
	return rdb.Del(ctx, "session:"+token).Err()
}
```

---

### 4.2 JWT 黑名單（撤銷已簽發的 JWT）

#### 問題描述

JWT 是**無狀態**的：伺服器不存 session，token 自帶簽名和過期時間，驗簽通過就放行。好處是水平擴展省儲存，壞處是——**沒辦法主動作廢**。使用者登出、帳號被封、密碼被改，但那個 JWT 在過期前依然合法。

#### 解法原理

引入一份**黑名單**：只存「被主動撤銷的」JWT（用其唯一 id `jti`），TTL 設為**該 token 的剩餘壽命**（`exp - now`）。驗證流程：驗簽 → 查黑名單 → 在黑名單裡就拒絕。

- **只存撤銷的**（不是全部 token），黑名單通常很小。
- **TTL = 剩餘壽命**：token 自然過期後，黑名單條目也隨之自動清除，不佔空間——因為過期後它本來就不合法了，不需要再黑它。

#### redis-cli

```bash
# 撤銷 jti=abc123 的 token，它還剩 3600 秒到期
127.0.0.1:6379> SET jwt:blacklist:abc123 1 EX 3600
OK
# 驗證時查
127.0.0.1:6379> EXISTS jwt:blacklist:abc123
(integer) 1        # 在黑名單 → 拒絕
```

#### Go 範例

```go
// RevokeJWT 把某 jti 加入黑名單，TTL = 剩餘壽命。
func RevokeJWT(ctx context.Context, rdb *redis.Client, jti string, exp time.Time) error {
	ttl := time.Until(exp)
	if ttl <= 0 {
		return nil // 已經過期，不需要黑
	}
	return rdb.Set(ctx, "jwt:blacklist:"+jti, 1, ttl).Err()
}

// IsRevoked 驗證流程中查黑名單。
func IsRevoked(ctx context.Context, rdb *redis.Client, jti string) (bool, error) {
	n, err := rdb.Exists(ctx, "jwt:blacklist:"+jti).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
```

> **坑**：JWT 的 `exp` 設太長，黑名單條目就得存很久；`exp` 太短又要頻繁刷新。實務上把 access token 設短（15 分鐘），撤銷靠「短壽命 + 不續發」自然生效，黑名單只擋這 15 分鐘窗口，配合下面的 refresh token 輪換使用。

---

### 4.3 Refresh Token 輪換 + 重放偵測

#### 問題描述 / 解法原理

雙 token 機制：**access token**（JWT，短壽命，15 分鐘）+ **refresh token**（長壽命，存 Redis，用來換新的 access token）。

**輪換（rotation）**：每次用 refresh token 換 access token 時，**同時作廢舊 refresh、發一個新的 refresh**。這樣每個 refresh token 只能用一次。

**重放偵測（replay detection）**：如果一個**已被輪換掉的舊 refresh token 又被拿來用**，代表它可能被竊取（合法使用者已經換了新的，舊的還在被用 = 有人偷了舊的在重放）。此時**立刻使該使用者整條 token 家族失效**，強制重新登入。

#### redis-cli

```bash
# 發 refresh token：存 token → userID，長 TTL
127.0.0.1:6379> SET refresh:rt_old 10086 EX 2592000
# 輪換：刪舊、發新（要原子，見 Lua）
127.0.0.1:6379> DEL refresh:rt_old
127.0.0.1:6379> SET refresh:rt_new 10086 EX 2592000
```

原子輪換 Lua（驗證舊 token 存在 → 刪舊 → 寫新，一步完成，防並發重放）：

```lua
-- KEYS[1]=舊 refresh key, KEYS[2]=新 refresh key
-- ARGV[1]=userID, ARGV[2]=新 token TTL 秒
local uid = redis.call("GET", KEYS[1])
if not uid then
    return -1              -- 舊 token 不存在 → 可能是重放！
end
redis.call("DEL", KEYS[1])                       -- 作廢舊的
redis.call("SET", KEYS[2], uid, "EX", ARGV[2])   -- 發新的
return 1
```

#### Go 範例

```go
var rotateScript = redis.NewScript(`
local uid = redis.call("GET", KEYS[1])
if not uid then return -1 end
redis.call("DEL", KEYS[1])
redis.call("SET", KEYS[2], uid, "EX", ARGV[1])
return uid
`)

var ErrReplayDetected = errors.New("refresh token replay detected: session revoked")

// RotateRefresh 用舊 refresh 換新 refresh，偵測重放。
func RotateRefresh(ctx context.Context, rdb *redis.Client, oldToken string, ttl time.Duration) (newToken, userID string, err error) {
	newToken = newToken()
	res, err := rotateScript.Run(ctx, rdb,
		[]string{"refresh:" + oldToken, "refresh:" + newToken},
		int(ttl.Seconds()),
	).Result()
	if err != nil {
		return "", "", err
	}
	if code, ok := res.(int64); ok && code == -1 {
		// 舊 token 不存在 = 已被輪換過還被拿來用 = 重放
		// 生產做法：查出該 token 屬於哪個 user/family，撤銷整個家族
		return "", "", ErrReplayDetected
	}
	userID = res.(string)
	return newToken, userID, nil
}
```

> **家族撤銷（token family）**：更完整的方案會給每次登入分配一個 `familyID`，所有由它輪換出的 refresh 都帶同一個 family。偵測到重放時，`DEL` 整個 family 的所有 token（可用 `SADD family:<id> <token>` 維護家族成員集合），一次踢掉所有相關 session。

#### 坑

- **輪換不原子**：查舊 + 刪舊 + 發新若分成多步，並發重放能鑽空子。務必 Lua 一步完成。
- **舊 token 立即刪 vs 短寬限期**：網路重試可能讓合法客戶端用同一個舊 token 打兩次，太嚴格會誤殺。實務可給舊 token 幾秒寬限（標記為「已輪換」而非立刻刪），寬限內同一 token 放行但不再發新的。
- **只黑 access 不管 refresh**：撤銷時 refresh 一定要一起作廢，否則對方拿 refresh 又換出新 access。

---

## 5. 其他實用模式

### 5.1 分散式 ID（INCR 分段發號）

**問題**：多節點要生成全域唯一、遞增的 ID。**解法**：Redis `INCR` 天然原子。但每次發號都打 Redis 太頻繁 → **分段（segment / step）**：一次 `INCRBY` 拿一批號段（如一次拿 1000 個），節點在本地記憶體慢慢分配，用完再拿下一段。

```bash
# 一次領 1000 個 ID，回傳的是這段的「末尾」
127.0.0.1:6379> INCRBY id:order 1000
(integer) 5000     # 本節點可用 4001~5000
```

```go
// Segment 本地號段，用完再向 Redis 領。
type Segment struct {
	mu      sync.Mutex
	cur     int64
	max     int64
	rdb     *redis.Client
	key     string
	step    int64
}

func (s *Segment) Next(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur >= s.max {
		end, err := s.rdb.IncrBy(ctx, s.key, s.step).Result()
		if err != nil {
			return 0, err
		}
		s.max = end
		s.cur = end - s.step
	}
	s.cur++
	return s.cur, nil
}
```

**坑**：Redis 重啟若沒持久化（AOF/RDB）會回退發重號 → 一定要開持久化。號段在記憶體，節點崩了會浪費掉整段（ID 不連續但不重複，通常可接受）。

---

### 5.2 延遲隊列（Delayed Queue，ZSet score=執行時間戳）

**問題**：訂單 30 分鐘未付款自動取消、延遲通知。**解法**：ZSet，**member = 任務內容，score = 應執行的 Unix 時間戳**。消費者輪詢 `ZRANGEBYSCORE key 0 <now>` 取出到期任務。

```bash
# 30 分鐘後執行 cancel_order:8899
127.0.0.1:6379> ZADD delay:queue 1751896200 "cancel_order:8899"
# 消費者：撈出所有到期（score <= now）的任務
127.0.0.1:6379> ZRANGEBYSCORE delay:queue 0 1751894400 LIMIT 0 10
```

```go
// pollDue：原子撈出並移除到期任務（避免多消費者重複領取）。
var popDueScript = redis.NewScript(`
local jobs = redis.call("ZRANGEBYSCORE", KEYS[1], 0, ARGV[1], "LIMIT", 0, ARGV[2])
if #jobs > 0 then
    redis.call("ZREM", KEYS[1], unpack(jobs))
end
return jobs
`)

func PollDue(ctx context.Context, rdb *redis.Client, n int) ([]string, error) {
	now := time.Now().Unix()
	return popDueScript.Run(ctx, rdb, []string{"delay:queue"}, now, n).StringSlice()
}
```

**坑**：撈取與刪除必須原子（Lua），否則多消費者重複執行。任務執行失敗要有重試 / 死信機制。輪詢間隔決定延遲精度。

---

### 5.3 排行榜 + 分頁（ZSet）

**問題**：遊戲積分榜、熱門排行，要即時排序 + 分頁。**解法**：ZSet，member=玩家、score=分數。`ZINCRBY` 加分，`ZREVRANGE` 取高分榜並分頁，`ZREVRANK` 查自己排名。

```bash
127.0.0.1:6379> ZINCRBY leaderboard 100 player:42
127.0.0.1:6379> ZREVRANGE leaderboard 0 9 WITHSCORES   # Top 10（第 1 頁）
127.0.0.1:6379> ZREVRANGE leaderboard 10 19 WITHSCORES # 第 2 頁
127.0.0.1:6379> ZREVRANK leaderboard player:42          # 我排第幾（0-based）
```

```go
// TopN 取第 page 頁（每頁 size 筆）。
func TopN(ctx context.Context, rdb *redis.Client, page, size int64) ([]redis.Z, error) {
	start := page * size
	stop := start + size - 1
	return rdb.ZRevRangeWithScores(ctx, "leaderboard", start, stop).Result()
}

// MyRank 取某玩家排名（+1 變成 1-based）。
func MyRank(ctx context.Context, rdb *redis.Client, member string) (int64, error) {
	rank, err := rdb.ZRevRank(ctx, "leaderboard", member).Result()
	if err != nil {
		return 0, err
	}
	return rank + 1, nil
}
```

**坑**：分數相同時排序不穩 → 可把 score 設計成 `分數 * 1e10 + (maxTime - 達成時間戳)`，讓「同分先達成者在前」。榜單巨大時 `ZREVRANGE` 深分頁仍是 O(log N + M)，可接受，但別一次拉幾萬筆。

---

### 5.4 布隆過濾器（RedisBloom 模組 vs bitmap 自建）

前面 2.1 已用過 RedisBloom（`BF.*`）。若**不能裝模組**，可用原生 `SETBIT`/`GETBIT` 自建一個簡易布隆：對元素做 k 個 hash，在 bitmap 上標記 k 個 bit。

```bash
# 自建：對 "user:10086" 算 3 個 hash 落在 bit 152、9981、44021
127.0.0.1:6379> SETBIT mybloom 152 1
127.0.0.1:6379> SETBIT mybloom 9981 1
127.0.0.1:6379> SETBIT mybloom 44021 1
# 查詢：3 個 bit 都是 1 才算「可能存在」
127.0.0.1:6379> GETBIT mybloom 152
```

```go
// 自建布隆：k 個雜湊 → k 個 bit。示範用雙雜湊法生成 k 個位置。
func bloomOffsets(item string, k, m uint64) []int64 {
	h := fnv.New64a()
	h.Write([]byte(item))
	h1 := h.Sum64()
	h2 := h1>>32 | h1<<32
	offs := make([]int64, k)
	for i := uint64(0); i < k; i++ {
		offs[i] = int64((h1 + i*h2) % m)
	}
	return offs
}

func BloomAdd(ctx context.Context, rdb *redis.Client, key, item string, k, m uint64) error {
	pipe := rdb.Pipeline()
	for _, off := range bloomOffsets(item, k, m) {
		pipe.SetBit(ctx, key, off, 1)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func BloomExists(ctx context.Context, rdb *redis.Client, key, item string, k, m uint64) (bool, error) {
	pipe := rdb.Pipeline()
	cmds := make([]*redis.IntCmd, 0, k)
	for _, off := range bloomOffsets(item, k, m) {
		cmds = append(cmds, pipe.GetBit(ctx, key, off))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	for _, c := range cmds {
		if c.Val() == 0 {
			return false, nil // 任一 bit 為 0 → 一定不存在
		}
	}
	return true, nil // 全 1 → 可能存在
}
```

**選型**：能裝模組就用 RedisBloom（自動算最優 k/m、支援 scaling）。自建 bitmap 適合輕量、可控、不想引模組的場景，但要自己算容量與誤判率、且無法動態擴容。

---

### 5.5 Pub/Sub vs Stream 選型

| 維度 | Pub/Sub | Stream |
| --- | --- | --- |
| 訊息持久化 | ❌ 不留存，發了就沒 | ✅ 存在 log 裡 |
| 離線訂閱者 | ❌ 訂閱者不在線就漏收 | ✅ 上線後可補讀 |
| 消費確認 (ACK) | ❌ 無 | ✅ `XACK` |
| 消費者群組 | ❌ 無（所有訂閱者都收到） | ✅ `XGROUP` 負載均衡 |
| 回溯歷史 | ❌ | ✅ 依 ID 任意讀 |
| 延遲 / 開銷 | 極低 | 略高 |
| 適用 | 即時廣播、可丟失的通知、快取失效廣播 | 可靠消息隊列、事件溯源、需要 ACK/重試 |

```bash
# Pub/Sub：發後即忘
127.0.0.1:6379> PUBLISH cache:invalidate "user:10086"
# Stream：持久化 + 消費者群組
127.0.0.1:6379> XADD orders '*' order_id 8899 amount 100
127.0.0.1:6379> XGROUP CREATE orders workers 0
127.0.0.1:6379> XREADGROUP GROUP workers w1 COUNT 10 STREAMS orders '>'
127.0.0.1:6379> XACK orders workers 1751894400000-0
```

**選型一句話**：**能丟失、要即時、要廣播 → Pub/Sub；不能丟失、要 ACK/重試/群組消費 → Stream。** 快取失效廣播（多級快取一致性）是 Pub/Sub 的經典場景；訂單、支付等業務事件用 Stream。

---

### 5.6 分散式 Session

**問題**：多台 Web 伺服器，使用者的 session 不能只存在單機記憶體（否則負載均衡到別台就掉登入）。**解法**：session 存 Redis，所有伺服器共享。用 Hash 存 session 欄位，方便部分更新。

```bash
127.0.0.1:6379> HSET session:abc123 user_id 10086 role admin csrf tk_xx
127.0.0.1:6379> EXPIRE session:abc123 1800          # 30 分鐘閒置過期
127.0.0.1:6379> HGETALL session:abc123
127.0.0.1:6379> HSET session:abc123 cart_count 3    # 部分更新，不動其他欄位
```

```go
type Session struct {
	UserID string
	Role   string
}

func SaveSession(ctx context.Context, rdb *redis.Client, sid string, s Session, ttl time.Duration) error {
	key := "session:" + sid
	pipe := rdb.Pipeline()
	pipe.HSet(ctx, key, "user_id", s.UserID, "role", s.Role)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func LoadSession(ctx context.Context, rdb *redis.Client, sid string, ttl time.Duration) (Session, error) {
	key := "session:" + sid
	m, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return Session{}, err
	}
	if len(m) == 0 {
		return Session{}, ErrInvalidToken
	}
	rdb.Expire(ctx, key, ttl) // 滑動過期
	return Session{UserID: m["user_id"], Role: m["role"]}, nil
}
```

**坑**：用 Hash 才能部分更新（用 String 存 JSON 每次要整包讀寫）。滑動過期每次都 `EXPIRE` 有寫入放大，量大時可考慮「懶續期」（快到期才續）。Redis 是 session 的唯一真相，務必高可用 + 持久化。

---

## 6. 練習題 + 檢查點 + 延伸閱讀

### 練習題

1. **穿透**：實作一個 `GetProduct`，同時具備空值快取（哨兵 + 60s TTL）和 RedisBloom 前置攔截。用 `id=-1` 狂打，確認 DB 查詢次數為 0。
2. **擊穿**：把某 key `DEL` 後同時發 5000 個並發讀，分別在「只有 singleflight」「只有分散式鎖」「兩者都有」三種配置下，統計 DB 實際被查幾次，解釋差異。
3. **邏輯過期**：實作 `GetLogical`，驗證「邏輯過期瞬間，讀請求延遲不受影響（永遠拿到舊值）」，並確認背景只有一個 goroutine 在重建。
4. **雪崩**：寫一段程式一次性 SET 一萬個 key，比較「固定 TTL」與「TTL + 20% 抖動」兩種下，到期時刻的分佈直方圖。
5. **一致性**：用兩個 goroutine 模擬「讀回填」與「寫刪除」的競態，重現「先刪 cache 後更 DB」的髒資料，再證明「先更 DB 後刪 cache + 延遲雙刪」能修正。
6. **限流**：把四種算法都實作出來，對同一串「前 1 秒爆發 200 次、之後勻速」的請求流跑一遍，畫出各算法的放行/拒絕曲線，親眼看令牌桶如何容忍突發、漏桶如何抹平。
7. **固定窗口邊界坑**：構造一個「跨窗口邊界 1 秒內放行 2 倍」的測試，證明固定窗口的缺陷。
8. **令牌桶**：把 Lua 裡的 `now` 改用 `redis.call("TIME")` 取伺服器時間，說明為何比客戶端傳入更穩。
9. **認證**：實作完整的 access + refresh 雙 token 流程，包含輪換與重放偵測；模擬「舊 refresh 被竊取重放」，確認整個 family 被撤銷。
10. **延遲隊列**：實作訂單 30 分鐘未支付自動取消，用 Lua 保證多消費者不重複取消同一單。

### 檢查點（能講清楚才算過關）

- [ ] 能分辨穿透 / 擊穿 / 雪崩三者的攻擊點與對應防法，不會混淆。
- [ ] 能解釋「空值快取的 TTL 為何要短」「哨兵值為何不能用空字串」。
- [ ] 能講清楚 singleflight 只擋單機、分散式鎖擋跨機，為何要兩層。
- [ ] 能說出邏輯過期「讀不阻塞」的代價（讀到舊值 + key 不物理過期）。
- [ ] 能推導「先更 DB 後刪 cache」比「先刪後更」好在哪，以及延遲雙刪補的是哪個窗口。
- [ ] 能回答「為何刪 cache 不更 cache」（省算力 + 防並發覆蓋）。
- [ ] 能複述固定窗口的邊界雙倍突發，並說明滑動窗口如何消除它。
- [ ] 能默寫令牌桶惰性補充的 Lua 核心邏輯，並說明 capacity/rate 各控制什麼。
- [ ] 能對四種限流算法按精度/記憶體/突發容忍度做選型。
- [ ] 能解釋 JWT 為何無法主動撤銷，黑名單「只存撤銷的 + TTL=剩餘壽命」的道理。
- [ ] 能講清楚 refresh token 輪換與重放偵測的完整時序，及為何輪換必須原子。
- [ ] 能對 Pub/Sub 與 Stream 按「能否丟失 / 要否 ACK」做選型。

### 延伸閱讀

- go-redis 官方文件與 `Script` / `Pipeline` API：<https://redis.uptrace.dev/>
- `golang.org/x/sync/singleflight` 原始碼（就一個檔案，值得讀完）。
- Redis 官方：Rate Limiting 模式、Bloom Filter（RedisBloom）、Streams 教學：<https://redis.io/docs/latest/develop/>
- 令牌桶 / 漏桶原始概念：Token Bucket 與 Leaky Bucket 演算法（網路 QoS 經典）。
- Cache Aside、延遲雙刪、Canal binlog 訂閱刪快取的一致性方案。
- OAuth 2.0 Refresh Token Rotation 與 Reuse Detection（RFC 6749 + OAuth Security BCP）。
- Stage 5 分散式鎖文件（本階段 `GetOrLoad` / refresh 輪換所依賴的鎖）。

---

> **本階段小結**：快取三大問題背下攻擊點與防法；限流四算法會選型、令牌桶會默寫 Lua；認證掌握「有狀態 token 天然可撤銷 vs JWT 靠黑名單 + refresh 輪換」；其餘小模式都是 ZSet / INCR / bitmap 的巧用。這些不是孤立技巧，生產系統裡它們常常疊在一起——一個讀請求可能同時經過限流、布隆攔截、多級快取、singleflight、分散式鎖。把它們組裝起來的能力，才是 Stage 6 真正要練的。
