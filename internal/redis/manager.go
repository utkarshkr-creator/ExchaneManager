package redis

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"

	"exchangeManager/internal/types"

	"github.com/redis/go-redis/v9"
)

type RedisManager struct {
	client        *redis.Client
	subscriptions map[string]*redis.PubSub // channel -> PubSub handle
	mu            sync.Mutex               // guards subscriptions map
}

var (
	instance *RedisManager
	once     sync.Once
)

// NewRedisManager creates a RedisManager with the given client.
// Useful for testing with a custom (e.g. miniredis) client.
func NewRedisManager(client *redis.Client) (*RedisManager, error) {
	return &RedisManager{
		client:        client,
		subscriptions: make(map[string]*redis.PubSub),
	}, nil
}

func GetInstance() *RedisManager {
	once.Do(func() {
		addr := os.Getenv("REDIS_ADDR")
		if addr == "" {
			addr = "localhost:6379"
		}
		client := redis.NewClient(&redis.Options{
			Addr: addr,
		})
		instance = &RedisManager{
			client:        client,
			subscriptions: make(map[string]*redis.PubSub),
		}
	})
	return instance
}

// PushMessage pushes a DbMessage onto the "db_processor" Redis list.
func (rm *RedisManager) PushMessage(ctx context.Context, message types.DbMessage) {
	data, err := json.Marshal(message)
	if err != nil {
		log.Printf("RedisManager: failed to marshal DbMessage: %v", err)
		return
	}
	rm.client.LPush(ctx, "db_processor", string(data))
}

// PublishMessage publishes a WsMessage to the given Redis pub/sub channel.
func (rm *RedisManager) PublishMessage(ctx context.Context, channel string, message types.WsMessage) {
	data, err := json.Marshal(message)
	if err != nil {
		log.Printf("RedisManager: failed to marshal WsMessage: %v", err)
		return
	}
	rm.client.Publish(ctx, channel, string(data))
}

// SendToApi publishes a MessageToApi to the client's Redis pub/sub channel.
func (rm *RedisManager) SendToApi(ctx context.Context, clientId string, message types.MessageToApi) {
	data, err := json.Marshal(message)
	if err != nil {
		log.Printf("RedisManager: failed to marshal MessageToApi: %v", err)
		return
	}
	rm.client.Publish(ctx, clientId, string(data))
}

// Subscribe subscribes to a Redis pub/sub channel and returns the message
// channel for consuming incoming messages. If already subscribed, it returns
// the existing message channel.
func (rm *RedisManager) Subscribe(ctx context.Context, channel string) <-chan *redis.Message {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if ps, ok := rm.subscriptions[channel]; ok {
		return ps.Channel()
	}

	ps := rm.client.Subscribe(ctx, channel)
	rm.subscriptions[channel] = ps
	log.Printf("RedisManager: subscribed to channel %s", channel)
	return ps.Channel()
}

// Unsubscribe unsubscribes from a Redis pub/sub channel and closes the
// underlying PubSub connection.
func (rm *RedisManager) Unsubscribe(ctx context.Context, channel string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	ps, ok := rm.subscriptions[channel]
	if !ok {
		return
	}

	if err := ps.Unsubscribe(ctx, channel); err != nil {
		log.Printf("RedisManager: failed to unsubscribe from %s: %v", channel, err)
	}
	if err := ps.Close(); err != nil {
		log.Printf("RedisManager: failed to close PubSub for %s: %v", channel, err)
	}
	delete(rm.subscriptions, channel)
	log.Printf("RedisManager: unsubscribed from channel %s", channel)
}
