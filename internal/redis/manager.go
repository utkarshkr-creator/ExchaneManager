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
	client *redis.Client
}

var (
	instance *RedisManager
	once     sync.Once
)

func GetInstance() *RedisManager {
	once.Do(func() {
		addr := os.Getenv("REDIS_ADDR")
		if addr == "" {
			addr = "localhost:6379"
		}
		client := redis.NewClient(&redis.Options{
			Addr: addr,
		})
		instance = &RedisManager{client: client}
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
