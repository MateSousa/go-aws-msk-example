package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

var BrokerURLs = []string{
	"b-1.kafka.us-east-1.amazonaws.com:9094",
	"b-2.kafka.us-east-1.amazonaws.com:9094",
	"b-3.kafka.us-east-1.amazonaws.com:9094",
}

func getKafkaReader(topic, groupID string) *kafka.Reader {
	dialer := &kafka.Dialer{
		TLS: &tls.Config{},
	}

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  BrokerURLs,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
		Dialer:   dialer,
	})
}

func main() {
	topic := "topicTest4"
	groupID := "testConsumer2121"

	reader := getKafkaReader(topic, groupID)

	defer reader.Close()

	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("message at partition:%v offset:%v   %s \n", m.Partition, m.Offset, string(m.Key))
	}
}
