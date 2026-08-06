package rediskit

import (
	"strings"
)

// keySep 是 key 各段之間的分隔符。Redis 社群慣例是冒號，
// RedisInsight / redis-cli 也靠它做樹狀分組。
const keySep = ':'

// KeyBuilder 把業務片段組成 namespace:entity:id 格式的 key。
//
// lib 內所有模組（cache / lock / ratelimit / tokenstore）都必須經過它產 key，
// 業務碼禁止手拼字串——否則 A 服務寫 user:123、B 服務寫 users/123，維運翻 key 翻到死。
//
// 零值可用（namespace 為空時直接以第一段開頭，不會產生開頭多餘的冒號）。
type KeyBuilder struct {
	ns string
}

// NewKeyBuilder 建一個 namespace 為 ns 的 KeyBuilder。
//
// ns 會被去空白並轉小寫：namespace 是部署時決定的固定值，統一大小寫可避免
// "App" 與 "app" 在同一台 Redis 上分裂成兩組 key。
// 注意 Build 的各段「不會」轉小寫——id / token 是大小寫敏感的，動它會製造碰撞。
func NewKeyBuilder(ns string) KeyBuilder {
	return KeyBuilder{ns: strings.ToLower(strings.TrimSpace(ns))}
}

// Namespace 回傳正規化後的 namespace。
func (k KeyBuilder) Namespace() string {
	return k.ns
}

// Build 把各段以冒號相連，前面補上 namespace。空字串的段會被略過。
//
//	NewKeyBuilder("app").Build("user", "123") // "app:user:123"
//	NewKeyBuilder("").Build("user", "123")    // "user:123"
//
// 段內的冒號與百分號會被百分號跳脫（":" → "%3A"、"%" → "%25"），
// 確保 Build("user:1", "2") 與 Build("user", "1:2") 不會撞成同一把 key。
func (k KeyBuilder) Build(parts ...string) string {
	var b strings.Builder
	b.Grow(k.size(parts))

	if k.ns != "" {
		b.WriteString(k.ns)
	}
	for _, p := range parts {
		if p == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(keySep)
		}
		writeEscaped(&b, p)
	}
	return b.String()
}

// Qualify 把 namespace 接在一把「已經組好」的 key 前面，不做任何跳脫。
//
//	NewKeyBuilder("app").Qualify("user:123") // "app:user:123"
//	NewKeyBuilder("").Qualify("user:123")    // "user:123"
//
// 與 Build 的分工：Build 負責「把多段安全地組成一把 key」（段內冒號會跳脫），
// Qualify 負責「把 namespace 補上去」（key 內的冒號是結構的一部分，保留）。
// lib 內各模組（cache/lock/ratelimit/tokenstore）都用 Qualify 產最終 key；
// 呼叫端要組含動態 id 的 key 片段時用 Build。
func (k KeyBuilder) Qualify(key string) string {
	if k.ns == "" {
		return key
	}
	return k.ns + string(keySep) + key
}

// size 估算 key 長度，讓 strings.Builder 一次配置到位（跳脫時可能不夠，會自動長大）。
func (k KeyBuilder) size(parts []string) int {
	n := len(k.ns)
	for _, p := range parts {
		n += len(p) + 1
	}
	return n
}

// writeEscaped 把 s 寫進 b，並跳脫會破壞 key 結構的字元。
// 沒有要跳脫的字元時走 fast path，一次寫入、零額外配置。
func writeEscaped(b *strings.Builder, s string) {
	if !strings.ContainsAny(s, ":%") {
		b.WriteString(s)
		return
	}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case keySep:
			b.WriteString("%3A")
		case '%':
			b.WriteString("%25")
		default:
			b.WriteByte(s[i])
		}
	}
}
