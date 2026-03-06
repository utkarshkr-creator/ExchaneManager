package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"exchangeManager/internal/engine"
	"exchangeManager/internal/types"

	"github.com/redis/go-redis/v9"
)

type ApiMessageWrapper struct {
	Message  types.MessageFromApi `json:"message"`
	ClientId string               `json:"clientId"`
}

func main() {
	// 1. Initialize Engine
	exchangeEngine, err := engine.NewEngine()
	if err != nil {
		log.Fatalf("Failed to initialize engine: %v", err)
	}

	// 2. Initialize Redis Client
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis at %s: %v", redisAddr, err)
	}
	log.Printf("Connected to Redis at %s", redisAddr)
	log.Println("Exchange Engine started. Listening for messages...")

	// 3. Infinite loop to consume messages from the API
	for {
		// BRPop blocks indefinitely until a message is available in the "messages" list
		result, err := client.BRPop(ctx, 0, "messages").Result()
		if err != nil {
			log.Printf("Error popping message from Redis: %v", err)
			continue
		}

		// Result is a slice: [listName, element]
		if len(result) < 2 {
			continue
		}

		msgData := result[1]

		// 4. Parse the message wrapper
		var wrapper ApiMessageWrapper
		if err := json.Unmarshal([]byte(msgData), &wrapper); err != nil {
			log.Printf("Failed to unmarshal message: %v. Raw data: %s", err, msgData)
			continue
		}

		// 5. Spawn a goroutine to process the message concurrently
		// The engine's Orderbook channels and Mutexes handle the concurrency safely.
		go exchangeEngine.Process(wrapper.Message, wrapper.ClientId)
	}
}
