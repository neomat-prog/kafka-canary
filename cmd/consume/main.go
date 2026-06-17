package main

import (
	"context"
	"log"

	"github.com/neomat-prog/kafka-canary/consumer"
)

func main() {
	c, err := consumer.New([]string{"localhost:9092"}, "canary-group", "__strimzi_canary")
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	if err := c.Run(context.Background()); err != nil {
		log.Fatalf("run: %v", err)
	}
}
