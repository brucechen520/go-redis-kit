# Stage 3：持久化與耐久性（Persistence & Durability）

> 模組：`github.com/twteam/go-redis-kit`　·　Go 1.26.1　·　client：`github.com/redis/go-redis/v9`
> 本階段對應 `docker-compose.yml` 的 `single` profile（`redis:7`，已開 `--appendonly yes`）。

---

## 1. 本階段目標：資料會不會掉、怎麼恢復

Redis 是「記憶體資料庫」，所有資料預設都活在 RAM 裡。程序一旦結束（正常關閉、`kill -9`、OOM、機器斷電、Pod 被驅逐），RAM 內容就消失。**持久化（Persistence）** 的意義，就是把記憶體裡的狀態「落地（flush to disk）」成檔案，讓 Redis 下次啟動時能重新載入，把資料救回來。

這一章要能回答三個核心問題：

1. **會不會掉？** — 在某個時間點當機，最多會丟失多少資料（RPO，Recovery Point Objective）？
2. **怎麼恢復？** — 重啟後 Redis 從哪個檔案、用什麼順序把資料讀回記憶體？
3. **代價是什麼？** — 為了不掉資料，我要付出多少 CPU、記憶體、磁碟 I/O 與 TPS 的成本？

Redis 提供兩種持久化機制，外加一種混合模式：

| 機制 | 本質 | 恢復速度 | 資料安全性 | 檔案 |
| --- | --- | --- | --- | --- |
| **RDB** | 某一瞬間的記憶體「快照」（snapshot） | 快 | 較差（會丟最後一段） | `dump.rdb`（二進位） |
| **AOF** | 把每一條寫指令「追加」進日誌 | 慢（要重放指令） | 好（最多丟 1 秒） | `appendonlydir/`（7.x 起是目錄） |
| **混合（Hybrid）** | AOF rewrite 時前段用 RDB、後段用 AOF | 快 | 好 | AOF 檔內含 RDB preamble |

貫穿本章的關鍵詞是 **RPO（能容忍丟多少資料）** 與 **成本（CPU / I/O / TPS）** 的取捨。沒有「最好」的設定，只有「最適合這個服務」的設定。

---

## 2. RDB 快照（Snapshot）

RDB（Redis DataBase）會把「某個時間點」記憶體裡的所有資料，序列化成一個緊湊的二進位檔 `dump.rdb`。你可以把它想成「幫整個資料庫拍一張照片」。

### 2.1 SAVE vs BGSAVE

觸發快照有兩條指令：

| 指令 | 執行者 | 是否阻塞 | 使用時機 |
| --- | --- | --- | --- |
| `SAVE` | **主執行緒**同步執行 | **完全阻塞**，期間拒絕所有其他請求 | 幾乎不要用；只在維護、遷移、離線腳本中使用 |
| `BGSAVE` | **fork 出的子行程**背景執行 | 不阻塞（fork 那一瞬間除外） | 正式環境的標準做法 |

```bash
# 同步存檔：主執行緒被卡住，大資料集會讓整台 Redis「假死」數秒到數十秒
redis-cli SAVE

# 背景存檔：立刻回傳 "Background saving started"，由子行程負責寫檔
redis-cli BGSAVE

# 查看上一次成功存檔的 UNIX timestamp
redis-cli LASTSAVE
```

**關鍵觀念**：`SAVE` 會讓 Redis 在存檔期間對所有客戶端「無回應」，正式環境嚴禁。實務上永遠用 `BGSAVE`（Redis 內部的自動快照也是走 `BGSAVE`）。

### 2.2 fork + Copy-On-Write 原理，以及「記憶體翻倍」風險

`BGSAVE` 為什麼能不阻塞主流程？靠的是作業系統的 `fork()` + **Copy-On-Write（COW，寫時複製）**：

1. Redis 主行程呼叫 `fork()`，產生一個子行程。
2. 子行程「邏輯上」擁有一份和父行程一模一樣的記憶體，但**實體上不是真的複製**——父子兩邊的虛擬記憶體頁面（page）指向**同一批實體頁面**，並被標記為唯讀。
3. 子行程慢慢把這份「凍結」的記憶體快照寫到 `dump.rdb`。因為子行程看到的頁面不會變，所以快照是一致的（point-in-time consistent）。
4. **此時如果父行程要修改某個 key**：OS 會攔截這次寫入，先「複製」那個頁面（copy），讓父行程改副本，子行程繼續看舊的。這就是 Copy-On-Write。

```
fork() 當下：父子共用同一批 physical pages（唯讀）
      父行程 ──┐
               ├──► [page A][page B][page C]  ← 實體頁面（唯讀）
      子行程 ──┘

父行程改了 page B：OS 複製一份 B'
      父行程 ──► [page A][page B'][page C]
      子行程 ──► [page A][page B ][page C]  ← 子行程仍看舊的 B
```

**記憶體翻倍風險**：正常情況下 COW 讓額外記憶體開銷很小（只複製「存檔期間被寫到的頁面」）。但如果：

- Redis 的**寫入非常頻繁**（存檔那幾秒內大量 key 被改），
- 或資料集很大、存檔耗時很久，

那麼被複製的頁面會越來越多，最壞情況下父行程幾乎每個頁面都被寫過一次 → **實體記憶體用量接近兩倍**。若機器 RAM 不夠、又沒設好 `vm.overcommit_memory`，`fork()` 就可能失敗，或觸發 **OOM Killer** 把 Redis 殺掉（見第 7 節踩坑）。

> 建議：Linux 設定 `vm.overcommit_memory = 1`（`sysctl vm.overcommit_memory=1`），讓 `fork()` 不會因為「保守估計記憶體不足」而失敗。並確保實體記憶體預留足夠餘裕（一般抓資料集大小 + 額外 30~50% 的 headroom）。

### 2.3 save 觸發規則（config）

除了手動 `BGSAVE`，Redis 會依 `redis.conf` 裡的 `save` 規則自動觸發背景快照。規則語意是「在 M 秒內，若至少有 N 個 key 發生變更，就 `BGSAVE`」：

```conf
# redis.conf —— Redis 7 的預設值
save 3600 1        # 3600 秒內 至少 1 個 key 變更 → 存檔
save 300 100       # 300 秒內  至少 100 個 key 變更 → 存檔
save 60 10000      # 60 秒內   至少 10000 個 key 變更 → 存檔
```

多條規則是「或（OR）」關係，任一條滿足就存檔。愈頻繁的變更愈快觸發快照。

```bash
# 執行期動態查詢/修改（不用改檔重啟）
redis-cli CONFIG GET save
redis-cli CONFIG SET save "60 10000 300 100"

# 完全關掉自動 RDB 快照（純快取情境常這樣）
redis-cli CONFIG SET save ""
```

相關設定：

```conf
dbfilename dump.rdb          # RDB 檔名
dir /data                    # RDB / AOF 檔案存放目錄
rdbcompression yes           # 用 LZF 壓縮字串，省磁碟、費一點 CPU
rdbchecksum yes              # 檔尾加 CRC64 校驗碼，載入時可偵測毀損
stop-writes-on-bgsave-error yes  # 背景存檔失敗時，拒絕新的寫入（保護資料一致性）
```

### 2.4 RDB 檔結構概念

不需要背細節，但理解大致結構有助於除錯：

```
+-------------------+
| "REDIS"           |  ← 魔術字串（magic）
| 版本號 (4 bytes)   |  ← RDB version，例如 "0011"
+-------------------+
| AUX fields        |  ← 輔助資訊：redis-ver、redis-bits、ctime、used-mem…
+-------------------+
| SELECTDB 0        |  ← 選 DB
|   key1 → value1   |  ← 逐一儲存 key/value（含型別、過期時間、編碼）
|   key2 → value2   |
| SELECTDB 1        |
|   ...             |
+-------------------+
| EOF (0xFF)        |  ← 結束標記
| CRC64 checksum    |  ← 8 bytes 校驗碼（rdbchecksum yes 時）
+-------------------+
```

重點：RDB 是**緊湊的二進位格式**，同樣的資料通常比 AOF 小很多，載入也快很多（直接反序列化，不用重放指令）。

### 2.5 RDB 優缺點

**優點**

- 檔案小、緊湊，適合**備份**與**跨機遷移**（一個 `dump.rdb` 複製走即可）。
- **恢復速度快**：載入 RDB 遠快於重放 AOF。
- fork 出子行程存檔，**對主流程效能影響小**（相對 AOF `always`）。

**缺點**

- **會丟最後一段資料**：兩次快照之間當機，這段時間的寫入全部遺失。用 `save 60 10000` 這種規則，最壞可能丟掉將近 60 秒的資料。
- `fork()` 在大資料集上有成本（COW 記憶體翻倍風險、fork 本身的短暫停頓）。
- 舊版本間 RDB 格式可能不相容（跨大版本升級要注意）。

---

## 3. AOF 追加日誌（Append Only File）

AOF（Append Only File）換一個思路：不拍快照，而是**把每一條「會改變資料」的寫指令，用 Redis 協定（RESP）格式追加寫進日誌檔**。恢復時，Redis 從頭把這些指令重新執行一遍（replay），就能重建出當機前的狀態。

```bash
# 開啟 AOF（也可在 redis.conf 設 appendonly yes）
redis-cli CONFIG SET appendonly yes
```

> Redis 7.x 起，AOF 不再是單一 `appendonly.aof`，而是一個目錄 `appendonlydir/`，裡面用 **Multi-Part AOF**：一個 base 檔（可為 RDB 格式）、若干 incr 檔（增量指令）、加一個 manifest 檔索引。設定 `appenddirname` 可改目錄名。

### 3.1 appendfsync 三級：always / everysec / no

寫進 AOF 檔其實有兩步：先寫進 OS 的 **page cache（write buffer）**，再由 OS `fsync()` 真正刷到實體磁碟。`appendfsync` 決定「多久 `fsync()` 一次」，這是 **資料安全性 vs TPS** 的核心旋鈕：

| 設定 | fsync 頻率 | 最壞丟失 | 效能（TPS） | 適用 |
| --- | --- | --- | --- | --- |
| `always` | **每一條寫指令**都 fsync | 幾乎不丟（最多正在處理的那一筆） | **最差**，每次寫都等磁碟 | 金流、帳務等強持久 |
| `everysec` | **每秒**背景 fsync 一次 | 最多約 **1 秒** | 好，接近純記憶體 | **預設值、絕大多數場景** |
| `no` | 交給 **OS 自己決定**（通常 ~30 秒） | 可能丟很多（一整個 OS flush 週期） | 最好 | 幾乎不用；接受大量丟失才選 |

```conf
appendfsync everysec   # 預設，最推薦：安全性與效能的甜蜜點
# appendfsync always   # 最安全最慢
# appendfsync no       # 最快最不安全
```

**對 TPS 的影響**：`always` 每次寫都要等磁碟 `fsync()` 回來，磁碟延遲（尤其非 SSD 或雲端 EBS）會直接壓垮寫入吞吐，可能只剩 `everysec` 的幾分之一。`everysec` 因為 fsync 在背景每秒做一次，寫指令本身不必等磁碟，效能幾乎不受影響——這就是它作為預設值的原因。

### 3.2 AOF rewrite（BGREWRITEAOF）壓縮

AOF 是「只追加」的，時間久了會不斷膨脹。而且它記的是「過程」而非「結果」——例如：

```
INCR counter   (counter=1)
INCR counter   (counter=2)
...
INCR counter   (counter=100)
```

這 100 條指令，最終結果只是 `counter=100`。**AOF rewrite** 就是掃描目前記憶體的實際狀態，用「能重建目前狀態的最少指令」重寫一份新的 AOF，把上面 100 條壓成一條 `SET counter 100`。

```bash
# 手動觸發背景重寫（一樣是 fork 子行程，不阻塞主流程）
redis-cli BGREWRITEAOF
```

自動觸發規則：

```conf
# 當 AOF 檔大小 ≥ 上次 rewrite 後大小的 100%（即成長一倍），且
# 檔案 ≥ 64mb 時，自動觸發 rewrite
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
```

rewrite 期間，主流程新產生的寫指令會同時寫進「舊 AOF」和一個「rewrite 緩衝」，重寫完成後再把緩衝內容補進新 AOF，確保不丟資料。

### 3.3 aof-use-rdb-preamble

```conf
aof-use-rdb-preamble yes   # 預設 yes
```

這個設定控制 **AOF rewrite 產生的檔案格式**：

- `yes`（預設）：rewrite 時，把「現有資料」用**緊湊的 RDB 二進位格式**寫成 base，之後新來的寫指令才用 AOF 文字格式追加。→ 檔更小、載入更快。**這就是「混合持久化」的實作基礎**（見第 4 節）。
- `no`：rewrite 也全用純 AOF 文字指令格式（相容舊工具，但檔大、載入慢）。

---

## 4. 混合持久化（Hybrid，7.x 預設）

混合持久化不是第三種獨立機制，而是 **AOF 開啟 + `aof-use-rdb-preamble yes`** 的組合成果，Redis 4.0 引入、7.x 為預設。它同時吃到 RDB「載入快」與 AOF「丟得少」的優點。

### 4.1 結構：RDB 基底 + AOF 增量

每次 AOF rewrite 後，`appendonlydir/` 大致長這樣：

```
appendonlydir/
├── appendonly.aof.1.base.rdb    ← base：rewrite 當下的全量快照（RDB 二進位格式，小又快）
├── appendonly.aof.1.incr.aof    ← incr：rewrite 之後的新寫指令（AOF 文字格式）
└── appendonly.aof.manifest      ← manifest：索引，記錄目前有哪些 base / incr 檔
```

一句話：**「舊資料」用 RDB 存（省空間、載入快），「新資料」用 AOF 追加（丟得少）**。

### 4.2 恢復流程

Redis 啟動、且 `appendonly yes` 時：

```
1. 讀 manifest，找出 base 檔與 incr 檔清單
2. 載入 base（RDB preamble）→ 快速重建「上次 rewrite 時」的全量狀態
3. 依序重放 incr（AOF 指令）→ 補上 rewrite 之後的所有寫入
4. 完成，記憶體狀態 = 當機前最後一次 fsync 的狀態（everysec 最多丟 1 秒）
```

因為第 2 步是載入 RDB（快），只有較短的第 3 步要重放指令，所以恢復速度遠比「純 AOF 全靠重放」快，同時又保有 AOF「最多丟 1 秒」的安全性。

---

## 5. 取捨決策表

沒有萬用設定。先問「這個服務的資料，丟了會怎樣？」，再對號入座：

| 情境 | 資料性質 | 建議設定 | 最壞 RPO | 理由 |
| --- | --- | --- | --- | --- |
| **純快取** | 資料可從 DB / 上游重建，掉了自己回填 | **關閉持久化**：`save ""` + `appendonly no` | 全部（無所謂） | 省 CPU / I/O，追求極致效能；重啟就當冷啟動 |
| **有狀態服務** | Session、排行榜、購物車、限流計數等，掉了會影響體驗但非致命 | **AOF `everysec`**（+ 混合持久化） | 約 1 秒 | 安全性與效能的甜蜜點，絕大多數線上服務的預設選擇 |
| **強持久 / 金流** | 訂單、帳務、餘額，一筆都不能丟 | **AOF `always`** + **副本（replica）** + 定期 RDB 備份 | 幾乎 0 | 每筆落地才回應；再加副本與異地備份防單機故障 |

補充判斷：

- 純快取但「重建成本很高」（冷啟動打爆後端 DB）→ 升級成「有狀態服務」等級，開 AOF `everysec`，避免驚群（thundering herd）。
- 選了 `always` 一定要配 SSD / 低延遲磁碟，否則 TPS 會慘不忍睹。
- **無論哪一級，備份（`dump.rdb` 定期拷到異地）都不能省**——持久化保護的是「重啟」，備份保護的是「檔案毀損 / 誤刪 / 機器整台掛」。

---

## 6. 恢復流程與檔案修復

### 6.1 啟動時 AOF 優先

Redis 啟動載入資料的順序（記住這條，除錯必用）：

```
appendonly yes ？
   ├── 是 → 只從 appendonlydir/ 載入（AOF / 混合），完全忽略 dump.rdb
   └── 否 → 從 dump.rdb 載入
```

**AOF 優先於 RDB**。原因：AOF 通常比 RDB 更新（丟得少）。這也解釋了一個常見陷阱——你手動 `BGSAVE` 存了新的 `dump.rdb`，但因為開著 AOF，重啟時 Redis 根本不看 `dump.rdb`，而是讀 AOF。

### 6.2 檔案損壞的修復

當機（尤其 `kill -9` / 斷電）可能讓 AOF 或 RDB 檔尾寫到一半而毀損。Redis 附兩支修復工具：

```bash
# --- AOF 修復 ---
# 先檢查（不改檔）
redis-check-aof appendonlydir/appendonly.aof.manifest
# 確認毀損位置後，用 --fix 截斷掉尾端不完整的指令
redis-check-aof --fix appendonlydir/appendonly.aof.1.incr.aof

# --- RDB 檢查 ---
redis-check-rdb dump.rdb
```

相關保護設定：

```conf
# AOF 檔尾若不完整（常見於斷電），啟動時自動截斷到最後一條完整指令並繼續。
# yes（預設）= 容錯啟動；no = 拒絕啟動，要你手動 redis-check-aof --fix
aof-load-truncated yes
```

> `--fix` 的本質是「把壞掉的尾巴切掉」，代表你會**丟失尾端那幾筆**寫入——這正是 AOF `everysec` 語意下可接受的範圍。修復前務必先 `cp` 一份原始檔備份。

---

## 7. 踩坑清單

- **BGSAVE / BGREWRITEAOF 觸發 fork OOM**：大資料集 + 高寫入時 COW 讓記憶體逼近兩倍，`fork()` 失敗或被 OOM Killer 殺掉。→ 預留記憶體 headroom、設 `vm.overcommit_memory=1`、避開尖峰時段排程存檔。看 `INFO stats` 的 `latest_fork_usec` 監控 fork 耗時。
- **AOF `always` 拖垮 TPS**：每筆寫都等磁碟 fsync，在慢磁碟 / 雲端網路盤上寫入延遲爆增。→ 除非真的需要「零丟失」，否則用 `everysec`；要用 `always` 請上 SSD / NVMe。
- **`appendfsync no` 會丟很多**：交給 OS flush（約 30 秒一次），當機可能丟掉整個週期的資料。→ 幾乎沒有正當使用時機，不要為了那點效能選它。
- **只靠 replica 當備份**：主節點誤 `FLUSHALL` / 寫壞資料，會**即時同步**到所有副本，副本救不了你。replica 是**高可用（HA）**手段，不是**備份**。→ 一定要有獨立的、離線的 RDB 快照備份（且異地保存）。
- **主從（master/replica）都要各自開持久化**：常見誤解是「replica 有資料就好，master 可以關持久化」。危險場景：master 關了持久化、重啟後記憶體是空的，這份「空資料」會立刻同步覆蓋掉所有 replica → **全叢集資料清空**。→ master 與 replica 都要開持久化。
- **開了 AOF 卻以為 `dump.rdb` 會被載入**：重啟後資料「不見了」，其實是 Redis 走 AOF、忽略了你手動存的 RDB（見 6.1）。
- **`stop-writes-on-bgsave-error yes` 讓寫入全掛**：磁碟滿 / 存檔失敗時，Redis 為了保護一致性會拒絕所有寫入，服務看起來像「Redis 壞了」。→ 監控磁碟空間與 `rdb_last_bgsave_status`。
- **k8s CFS CPU 限流拖慢 fork / rewrite**：CPU limit 太低時 fork 後的存檔子行程被節流，rewrite 拖很久。（與本 repo memory 中「Go on k8s CFS throttle」同源）→ 提高 CPU requests、觀察 throttle 指標。

---

## 8. redis-cli 實驗：眼見為憑

用本 repo 的 `single` profile 實測「開 AOF vs 關持久化」在 `kill -9` 後的差別。

### 8.1 實驗 A：開 AOF → 寫資料 → kill -9 → 重啟看恢復

```bash
# 0) 用 compose 起單機（compose 已帶 --appendonly yes）
cd /Users/nan/Documents/code/yuan-jhen/go-redis-kit
docker compose --profile single up -d
docker compose --profile single ps    # 確認 redis 容器在跑

# 1) 確認 AOF 已開，fsync 策略為 everysec
docker compose --profile single exec redis redis-cli CONFIG GET appendonly
docker compose --profile single exec redis redis-cli CONFIG SET appendfsync everysec

# 2) 寫入一批資料
docker compose --profile single exec redis redis-cli SET durable:hello world
docker compose --profile single exec redis redis-cli MSET k1 v1 k2 v2 k3 v3
docker compose --profile single exec redis redis-cli DBSIZE      # 應為 4

# 3) 等超過 1 秒，讓 everysec 的 fsync 落地（保險起見也可手動 BGREWRITEAOF）
docker compose --profile single exec redis redis-cli BGREWRITEAOF
sleep 2

# 4) 模擬「非正常終止」：對容器內 redis-server 送 SIGKILL（不給它機會優雅存檔）
docker compose --profile single exec redis sh -c 'kill -9 1'
# 容器若設了 restart policy 會自動重啟；否則手動 start：
docker compose --profile single up -d

# 5) 重啟後檢查：資料應該還在（AOF 重放回來了）
docker compose --profile single exec redis redis-cli DBSIZE          # 仍為 4
docker compose --profile single exec redis redis-cli GET durable:hello   # "world"
```

預期結果：**資料完整保留**（最壞情況下只可能丟掉 kill 前不到 1 秒、還沒 fsync 的那幾筆）。

### 8.2 實驗 B：關掉持久化，對比損失

```bash
# 1) 關掉兩種持久化
docker compose --profile single exec redis redis-cli CONFIG SET appendonly no
docker compose --profile single exec redis redis-cli CONFIG SET save ""

# 2) 清空並重寫一批資料
docker compose --profile single exec redis redis-cli FLUSHALL
docker compose --profile single exec redis redis-cli MSET a 1 b 2 c 3
docker compose --profile single exec redis redis-cli DBSIZE      # 3

# 3) kill -9（沒有任何持久化落地）
docker compose --profile single exec redis sh -c 'kill -9 1'
docker compose --profile single up -d

# 4) 重啟後檢查：資料全沒了
docker compose --profile single exec redis redis-cli DBSIZE      # 0（或只剩 up 前的殘留）
```

預期結果：**資料全部遺失**。兩個實驗並排看，就能直觀體會持久化到底在保護什麼。

> 小抄：想直接進互動式 CLI，用 `docker compose --profile single exec redis redis-cli` 不帶指令即可。

---

## 9. Go 範例：用 go-redis 觸發存檔並讀 INFO persistence

以下片段用 `github.com/redis/go-redis/v9`，可放進 `labs/` 底下當可執行程式（`package main`）。示範兩件事：**主動觸發 BGSAVE**、**讀取 `INFO persistence` 監控指標**。

```go
package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379", // 對應 compose single profile 的 6379:6379
	})
	defer rdb.Close()

	// 1) 主動觸發背景快照（等同 redis-cli BGSAVE）
	if err := rdb.BgSave(ctx).Err(); err != nil {
		log.Fatalf("BGSAVE failed: %v", err)
	}
	fmt.Println("BGSAVE triggered")

	// 也可觸發 AOF 背景重寫：
	// rdb.BgRewriteAOF(ctx)

	// 2) 讀取 LASTSAVE：上一次成功存檔的時間點
	last, err := rdb.LastSave(ctx).Result()
	if err != nil {
		log.Fatalf("LASTSAVE failed: %v", err)
	}
	fmt.Printf("last save time: %s\n", time.Unix(last, 0).Format(time.RFC3339))

	// 3) 讀 INFO persistence，解析關鍵監控指標
	info, err := rdb.Info(ctx, "persistence").Result()
	if err != nil {
		log.Fatalf("INFO persistence failed: %v", err)
	}
	metrics := parseInfo(info)

	// 常用指標：
	fmt.Println("aof_enabled            =", metrics["aof_enabled"])            // 1 = AOF 已開
	fmt.Println("rdb_last_save_time     =", metrics["rdb_last_save_time"])     // 上次 RDB 存檔 unix time
	fmt.Println("rdb_bgsave_in_progress =", metrics["rdb_bgsave_in_progress"]) // 1 = 存檔進行中
	fmt.Println("rdb_last_bgsave_status =", metrics["rdb_last_bgsave_status"]) // ok / err
	fmt.Println("aof_last_write_status  =", metrics["aof_last_write_status"])  // ok / err
	fmt.Println("aof_last_bgrewrite_status =", metrics["aof_last_bgrewrite_status"])
	fmt.Println("latest_fork_usec       =", metrics["latest_fork_usec"])       // 上次 fork 耗時(µs)，監控 COW 停頓

	// 4) 簡易健康檢查：存檔失敗要告警
	if metrics["rdb_last_bgsave_status"] != "ok" {
		log.Println("[ALERT] last BGSAVE failed — 檢查磁碟空間 / 記憶體")
	}
	if t, _ := strconv.ParseInt(metrics["rdb_last_save_time"], 10, 64); t > 0 {
		age := time.Since(time.Unix(t, 0))
		fmt.Printf("距離上次 RDB 存檔已過 %s\n", age.Round(time.Second))
	}
}

// parseInfo 把 INFO 的 "key:value\r\n" 文字解析成 map。
func parseInfo(info string) map[string]string {
	m := make(map[string]string)
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue // 跳過空行與 section 標題（# Persistence）
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			m[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return m
}
```

`INFO persistence` 常用監控指標速查：

| 指標 | 意義 | 告警條件 |
| --- | --- | --- |
| `aof_enabled` | AOF 是否開啟（1/0） | 該開卻是 0 |
| `rdb_last_save_time` | 上次 RDB 成功存檔的 unix time | 距今過久 |
| `rdb_bgsave_in_progress` | 是否正在背景存檔（1/0） | 長時間卡在 1 |
| `rdb_last_bgsave_status` | 上次 BGSAVE 結果 | 非 `ok` |
| `aof_last_write_status` | 上次 AOF 寫入結果 | 非 `ok`（常為磁碟滿） |
| `aof_last_bgrewrite_status` | 上次 AOF rewrite 結果 | 非 `ok` |
| `aof_rewrite_in_progress` | 是否正在 rewrite（1/0） | 頻繁觸發 |
| `latest_fork_usec` | 上次 fork 耗時（微秒） | 過高 = COW 停頓大 |

---

## 10. 專案流程說明：為服務選持久化策略

### 10.1 決策步驟

替一個新服務決定持久化策略時，照這個順序問：

1. **這份資料掉了會怎樣？** 能從別處重建（純快取）？影響體驗但不致命（有狀態）？一筆都不能丟（金流）？
2. **能容忍丟多少（RPO）？** 幾秒可接受 → `everysec`；一筆都不行 → `always`；全丟無妨 → 關持久化。
3. **磁碟撐得住嗎？** 選 `always` 前先確認底層是 SSD / NVMe，並壓測寫入延遲。
4. **記憶體夠 fork 嗎？** 資料集大小 + 30~50% headroom；設 `vm.overcommit_memory=1`。
5. **對照第 5 節決策表落地設定**，寫進 `redis.conf` 或 compose `command`。
6. **接上監控**（第 9 節指標）：`rdb_last_bgsave_status`、`aof_last_write_status`、`latest_fork_usec` 進告警。
7. **規劃備份**（見下）——持久化 ≠ 備份。

對應到本 repo：`single` profile 已用 `--appendonly yes` 落地在「有狀態服務」這一級（AOF + 混合、`everysec` 預設），是最通用的起點。

### 10.2 備份排程概念

持久化保護「重啟」，備份保護「檔案毀損 / 誤刪 / 整台機器沒了」。一個務實的備份排程：

```
每日 03:00（離峰）：
  1. redis-cli BGSAVE            # 產生最新 dump.rdb
  2. 等 rdb_bgsave_in_progress=0 且 rdb_last_bgsave_status=ok
  3. cp dump.rdb  →  帶時間戳命名（dump-20260707.rdb）
  4. 上傳異地（物件儲存 / 另一台機房），保留 N 天輪替
  5. 定期做「還原演練」：把備份載入一台空 Redis，驗證能起得來
```

要點：**備份要異地**（同機磁碟壞了就一起沒了）、**要輪替保留多份**（防「昨天的資料就已經壞了」）、**要定期演練還原**（沒驗過的備份等於沒有備份）。

---

## 11. 練習題 + 檢查點 + 延伸閱讀

### 練習題

1. 把 `single` 的 `appendfsync` 分別設成 `always` / `everysec` / `no`，用 `redis-benchmark -n 100000 -t set` 各跑一次，記錄 TPS 差異並解釋原因。
2. 寫一批資料後，故意在 `BGSAVE` 進行中（`rdb_bgsave_in_progress=1`）持續高速寫入，觀察 `used_memory` 與 `latest_fork_usec` 如何變化，體會 COW 記憶體翻倍。
3. 開著 AOF 時，手動改一份 `appendonly.aof.*.incr.aof`（尾端刪幾個 byte 模擬毀損），重啟看行為，再用 `redis-check-aof --fix` 修復。
4. 設 `appendonly no` + `save ""`，重現第 8.2 節的資料全失；再逐一打開，找出「最少要開什麼」才能保住資料。
5. 用第 9 節的 Go 程式，做一個每 10 秒輪詢 `INFO persistence`、在 `rdb_last_bgsave_status != ok` 時印告警的小監控。

### 檢查點（讀完你應該能回答）

- [ ] `SAVE` 和 `BGSAVE` 差在哪？為什麼正式環境不用 `SAVE`？
- [ ] 用一句話講清楚 fork + Copy-On-Write，以及「記憶體翻倍」怎麼發生。
- [ ] `appendfsync` 三級各自最多丟多少資料、對 TPS 的影響？預設是哪個、為什麼？
- [ ] AOF rewrite 在壓縮什麼？`aof-use-rdb-preamble` 和「混合持久化」的關係？
- [ ] 同時開 RDB 和 AOF，重啟時 Redis 讀哪一個？為什麼？
- [ ] 為什麼「只靠 replica」不算備份？master 關持久化重啟為什麼可能清空整個叢集？
- [ ] 純快取 / 有狀態 / 金流，三種情境各該怎麼設？

### 延伸閱讀

- Redis 官方文件：Persistence — https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
- Redis 官方文件：`INFO` 指令與 persistence section — https://redis.io/docs/latest/commands/info/
- Redis 官方文件：Config 參數（`save`、`appendfsync`、`auto-aof-rewrite-*`）— https://redis.io/docs/latest/operate/oss_and_stack/management/config/
- Multi-Part AOF（Redis 7.0 release notes）— https://github.com/redis/redis/blob/7.0/redis.conf
- go-redis 文件 — https://redis.uptrace.dev/
- 本 repo 相關備忘：Go on k8s CFS throttle（CPU limit 過低會拖慢 fork / rewrite）。
