package operator_test

import (
	"context"
	"errors"
	"testing"

	"github.com/amiri/sms-gateway/internal/domain/messaging/operator"
)

type stubAdapter struct {
	name string
	err  error
	seen string
}

func (s *stubAdapter) Name() string { return s.name }
func (s *stubAdapter) Send(_ context.Context, to, _, _ string) error {
	s.seen = to
	return s.err
}

func TestRouterLongestPrefix(t *testing.T) {
	fallback := &stubAdapter{name: "default"}
	mci := &stubAdapter{name: "mci"}
	irancell := &stubAdapter{name: "irancell"}
	r := operator.NewRouter(fallback, []operator.Adapter{fallback, mci, irancell}, []operator.RouteRule{
		{Prefix: "+98", Adapter: "default"},
		{Prefix: "+9891", Adapter: "mci"},
		{Prefix: "+9890", Adapter: "irancell"},
	})

	if got := r.Resolve("+989121234567").Name(); got != "mci" {
		t.Fatalf("got %s want mci", got)
	}
	if got := r.Resolve("+989012345678").Name(); got != "irancell" {
		t.Fatalf("got %s want irancell", got)
	}
	if got := r.Resolve("+15551234567").Name(); got != "default" {
		t.Fatalf("got %s want default", got)
	}
}

func TestRouterSendPropagatesError(t *testing.T) {
	fail := &stubAdapter{name: "mci", err: errors.New("down")}
	r := operator.NewRouter(fail, []operator.Adapter{fail}, []operator.RouteRule{
		{Prefix: "+9891", Adapter: "mci"},
	})
	name, err := r.Send(context.Background(), "+989121234567", "hi", "normal")
	if name != "mci" || err == nil {
		t.Fatalf("name=%s err=%v", name, err)
	}
}
