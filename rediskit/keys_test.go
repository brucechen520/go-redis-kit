package rediskit_test

import (
	"testing"

	"github.com/twteam/go-redis-kit/rediskit"
)

func TestBuild_PrependsNamespace(t *testing.T) {
	kb := rediskit.NewKeyBuilder("app")

	got := kb.Build("user", "123")

	if want := "app:user:123"; got != want {
		t.Errorf("Build(\"user\", \"123\") = %q, want %q", got, want)
	}
}

func TestBuild_OmitsLeadingSeparatorWhenNamespaceEmpty(t *testing.T) {
	kb := rediskit.NewKeyBuilder("")

	got := kb.Build("user", "123")

	if want := "user:123"; got != want {
		t.Errorf("Build with empty namespace = %q, want %q", got, want)
	}
}

func TestBuild_UsableFromZeroValue(t *testing.T) {
	var kb rediskit.KeyBuilder

	got := kb.Build("session", "abc")

	if want := "session:abc"; got != want {
		t.Errorf("zero-value KeyBuilder Build = %q, want %q", got, want)
	}
}

func TestBuild_SkipsEmptyParts(t *testing.T) {
	kb := rediskit.NewKeyBuilder("app")

	got := kb.Build("user", "", "123")

	if want := "app:user:123"; got != want {
		t.Errorf("Build with empty part = %q, want %q", got, want)
	}
}

func TestBuild_ReturnsNamespaceAloneWhenNoParts(t *testing.T) {
	kb := rediskit.NewKeyBuilder("app")

	got := kb.Build()

	if want := "app"; got != want {
		t.Errorf("Build() = %q, want %q", got, want)
	}
}

func TestBuild_EscapesSeparatorInsidePart(t *testing.T) {
	kb := rediskit.NewKeyBuilder("app")

	got := kb.Build("user", "a:b")

	if want := "app:user:a%3Ab"; got != want {
		t.Errorf("Build(\"user\", \"a:b\") = %q, want %q", got, want)
	}
}

func TestBuild_EscapesPercentInsidePart(t *testing.T) {
	kb := rediskit.NewKeyBuilder("app")

	got := kb.Build("user", "100%")

	if want := "app:user:100%25"; got != want {
		t.Errorf("Build(\"user\", \"100%%\") = %q, want %q", got, want)
	}
}

// 跳脫的重點不是好看，是不同輸入不能撞成同一把 key。
func TestBuild_DistinguishesPartsThatWouldCollideUnescaped(t *testing.T) {
	kb := rediskit.NewKeyBuilder("app")

	withColonInFirst := kb.Build("user:1", "2")
	withColonInSecond := kb.Build("user", "1:2")

	if withColonInFirst == withColonInSecond {
		t.Errorf("Build(\"user:1\", \"2\") and Build(\"user\", \"1:2\") both = %q, want different keys",
			withColonInFirst)
	}
}

func TestQualify_PrependsNamespaceWithoutEscaping(t *testing.T) {
	kb := rediskit.NewKeyBuilder("app")

	got := kb.Qualify("user:123")

	if want := "app:user:123"; got != want {
		t.Errorf("Qualify(\"user:123\") = %q, want %q", got, want)
	}
}

func TestQualify_ReturnsKeyUnchangedWhenNamespaceEmpty(t *testing.T) {
	kb := rediskit.NewKeyBuilder("")

	got := kb.Qualify("user:123")

	if want := "user:123"; got != want {
		t.Errorf("Qualify with empty namespace = %q, want %q", got, want)
	}
}

func TestNewKeyBuilder_LowercasesAndTrimsNamespace(t *testing.T) {
	kb := rediskit.NewKeyBuilder("  App  ")

	got := kb.Namespace()

	if want := "app"; got != want {
		t.Errorf("Namespace() = %q, want %q", got, want)
	}
}

// id / token 大小寫敏感，KeyBuilder 動它會製造碰撞。
func TestBuild_PreservesCaseOfParts(t *testing.T) {
	kb := rediskit.NewKeyBuilder("app")

	got := kb.Build("User", "AbC")

	if want := "app:User:AbC"; got != want {
		t.Errorf("Build(\"User\", \"AbC\") = %q, want %q", got, want)
	}
}
