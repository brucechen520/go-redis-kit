package rediskit

import (
	"github.com/redis/go-redis/v9"
)

// Raw 是逃生艙：回傳底層的 go-redis client。
//
// rediskit 的四個意圖型別涵蓋不了所有 Redis 能力（bitmap、Stream、PubSub、
// SCAN、pipeline…）。需要那些時走這裡，而不是繞過 rediskit 另建連線——
// 至少連線池、觀測 Hook 還是共享的。
//
// 使用守則：
//   - 業務碼禁止直接呼叫。要用就包在自己的基礎設施層裡（像 labs/06 的
//     BloomFilter 那樣包成型別），讓 Raw 的呼叫點集中、可 review。
//   - 經由 Raw 的操作沒有 rediskit 的保證：錯誤不會映射（會漏 redis.Nil）、
//     key 不會帶 namespace、值不會過 Serializer。全部自己來。
//   - 每個新呼叫點都該在 code review 被質疑一次：「這真的進不了意圖 API？」
func (c *Client) Raw() redis.UniversalClient { return c.rdb }
