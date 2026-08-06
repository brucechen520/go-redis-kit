package rediskit

import (
	"encoding/json"
)

// Serializer 決定值進出 Redis 時如何編碼。
//
// 抽成 interface 是為了讓效能敏感時能換 msgpack、跨語言時能換 protobuf，
// 而呼叫端零改動：
//
//	client, _ := rediskit.New(
//	    rediskit.WithAddr("localhost:6379"),
//	    rediskit.WithSerializer(msgpackSerializer{}),
//	)
//
// 實作必須是併發安全的：同一個 Serializer 會被所有 goroutine 共用。
type Serializer interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONSerializer 是預設實作。可讀、免額外相依、跨語言，
// 代價是比 msgpack 慢且配置較多——換之前先用 benchmark 量，別憑感覺。
type JSONSerializer struct{}

// 編譯期確認 JSONSerializer 有實作 Serializer。
var _ Serializer = JSONSerializer{}

func (JSONSerializer) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONSerializer) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
