package rediskit_test

import (
	"testing"

	"github.com/twteam/go-redis-kit/rediskit"
)

type user struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestJSONSerializer_RoundTripsStruct(t *testing.T) {
	s := rediskit.JSONSerializer{}
	want := user{ID: "1", Name: "Ada", Age: 36}

	b, err := s.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal(%+v) 回傳非預期錯誤: %v", want, err)
	}
	var got user
	if err := s.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal(%q) 回傳非預期錯誤: %v", b, err)
	}

	if got != want {
		t.Errorf("round-trip = %+v, want %+v", got, want)
	}
}

func TestJSONSerializer_MarshalProducesJSONEncoding(t *testing.T) {
	s := rediskit.JSONSerializer{}

	b, err := s.Marshal(user{ID: "1", Name: "Ada", Age: 36})
	if err != nil {
		t.Fatalf("Marshal 回傳非預期錯誤: %v", err)
	}

	if want := `{"id":"1","name":"Ada","age":36}`; string(b) != want {
		t.Errorf("Marshal = %s, want %s", b, want)
	}
}

func TestJSONSerializer_MarshalReportsUnsupportedValue(t *testing.T) {
	s := rediskit.JSONSerializer{}

	_, err := s.Marshal(make(chan int))

	if err == nil {
		t.Error("Marshal(chan int) = nil error, want 錯誤")
	}
}

func TestJSONSerializer_UnmarshalReportsMalformedInput(t *testing.T) {
	s := rediskit.JSONSerializer{}

	var got user
	err := s.Unmarshal([]byte("{not json"), &got)

	if err == nil {
		t.Error("Unmarshal(\"{not json\") = nil error, want 錯誤")
	}
}
