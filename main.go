package main

import (
	"log"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/neomat-prog/kafka-canary/internal/message"
)

func main() {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true          // For SyncProducer
	cfg.Producer.RequiredAcks = sarama.WaitForAll // canary measures real delivery

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, cfg)
	if err != nil {
		log.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	probe := message.New(uuid.NewString())
	b, _ := probe.Encode()
	topic := "__strimzi_canary"

	// change topic name
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("send: %v", err)
	}
	log.Printf("sent to partition=%d offset=%d", partition, offset)
}
