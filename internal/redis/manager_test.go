package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"exchangeManager/internal/types"

	goredis "github.com/redis/go-redis/v9"

	"github.com/alicebob/miniredis/v2"
)

// newTestManager spins up a miniredis server and returns a RedisManager
// connected to it, plus a cleanup function.
func newTestManager(t *testing.T) (*RedisManager, *miniredis.Miniredis) {
	t.Helper()
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	rm, err := NewRedisManager(client)
	if err != nil {
		t.Fatalf("failed to create RedisManager: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
		s.Close()
	})
	return rm, s
}

func TestPushMessage(t *testing.T) {
	rm, s := newTestManager(t)
	ctx := context.Background()

	msg := types.DbMessage{
		Type: "TRADE_ADDED",
		Data: json.RawMessage(`{"id":"1","price":"100"}`),
	}
	rm.PushMessage(ctx, msg)

	// Verify the message landed in the "db_processor" list.
	items, err := s.List("db_processor")
	if err != nil {
		t.Fatalf("failed to read list: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item in list, got %d", len(items))
	}

	var got types.DbMessage
	if err := json.Unmarshal([]byte(items[0]), &got); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if got.Type != "TRADE_ADDED" {
		t.Errorf("expected type TRADE_ADDED, got %s", got.Type)
	}
}

func TestPublishAndSubscribe(t *testing.T) {
	rm, _ := newTestManager(t)
	ctx := context.Background()

	channel := "test-channel"
	msgCh := rm.Subscribe(ctx, channel)

	// Give the subscription a moment to register.
	time.Sleep(100 * time.Millisecond)

	// Publish a WsMessage.
	wsMsg := types.WsMessage{
		Stream: "depth@sol_usdc",
		Data:   json.RawMessage(`{"bids":[],"asks":[]}`),
	}
	rm.PublishMessage(ctx, channel, wsMsg)

	// Wait for the message to arrive.
	select {
	case msg := <-msgCh:
		var got types.WsMessage
		if err := json.Unmarshal([]byte(msg.Payload), &got); err != nil {
			t.Fatalf("failed to unmarshal payload: %v", err)
		}
		if got.Stream != "depth@sol_usdc" {
			t.Errorf("expected stream depth@sol_usdc, got %s", got.Stream)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for published message")
	}
}

func TestSubscribeIdempotent(t *testing.T) {
	rm, _ := newTestManager(t)
	ctx := context.Background()

	ch1 := rm.Subscribe(ctx, "ch")
	ch2 := rm.Subscribe(ctx, "ch")

	// Both calls should return the same underlying channel.
	if ch1 != ch2 {
		t.Error("expected Subscribe to return the same channel on duplicate calls")
	}

	// Only one PubSub should be tracked internally.
	rm.mu.Lock()
	count := len(rm.subscriptions)
	rm.mu.Unlock()
	if count != 1 {
		t.Errorf("expected 1 subscription, got %d", count)
	}
}

func TestUnsubscribe(t *testing.T) {
	rm, _ := newTestManager(t)
	ctx := context.Background()

	rm.Subscribe(ctx, "ch")
	time.Sleep(50 * time.Millisecond)

	rm.Unsubscribe(ctx, "ch")

	rm.mu.Lock()
	count := len(rm.subscriptions)
	rm.mu.Unlock()
	if count != 0 {
		t.Errorf("expected 0 subscriptions after unsubscribe, got %d", count)
	}
}

func TestUnsubscribeNonExistent(t *testing.T) {
	rm, _ := newTestManager(t)
	ctx := context.Background()

	// Should not panic or error.
	rm.Unsubscribe(ctx, "does-not-exist")
}

func TestSendToApi(t *testing.T) {
	rm, _ := newTestManager(t)
	ctx := context.Background()

	clientId := "client-123"
	msgCh := rm.Subscribe(ctx, clientId)
	time.Sleep(100 * time.Millisecond)

	apiMsg := types.MessageToApi{
		Type: "ORDER_PLACED",
		Data: json.RawMessage(`{"orderId":"abc"}`),
	}
	rm.SendToApi(ctx, clientId, apiMsg)

	select {
	case msg := <-msgCh:
		var got types.MessageToApi
		if err := json.Unmarshal([]byte(msg.Payload), &got); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if got.Type != "ORDER_PLACED" {
			t.Errorf("expected ORDER_PLACED, got %s", got.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for API message")
	}
}
