package main

import (
	"log"

	"github.com/IBM/sarama"
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

	// change topic name
	msg := &sarama.ProducerMessage{
		Topic: "__strimzi_canary",
		Value: sarama.StringEncoder("hello canary"),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("send: %v", err)
	}
	log.Printf("sent to partition=%d offset=%d", partition, offset)
}
