package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	// Connect to Redis running on localhost:6379
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	// Ping to verify connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	fmt.Println("✓ Connected to Redis")

	// Set a key, read it back
	if err := rdb.Set(ctx, "hello", "world", 0).Err(); err != nil {
		log.Fatalf("set failed: %v", err)
	}

	val, err := rdb.Get(ctx, "hello").Result()
	if err != nil {
		log.Fatalf("get failed: %v", err)
	}
	fmt.Printf("✓ Got back: hello = %s\n", val)
}
