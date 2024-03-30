package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

var BrokerURLs = []string{
	"b-1.kafka.us-east-1.amazonaws.com:9094",
	"b-2.kafka.us-east-1.amazonaws.com:9094",
	"b-3.kafka.us-east-1.amazonaws.com:9094",
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	dialer := &kafka.Dialer{
		TLS: &tls.Config{},
	}

	kafkaConfig := kafka.WriterConfig{
		Brokers:  BrokerURLs,
		Topic:    topic,
		Balancer: &kafka.Hash{},
		Dialer:   dialer,
	}
	return kafka.NewWriter(kafkaConfig)
}

func createKafkaTopic(kafkaURL, topic string) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       &tls.Config{},
	}

	conn, err := dialer.DialContext(context.Background(), "tcp", kafkaURL)
	if err != nil {
		fmt.Println("error1")
		panic(err.Error())
	}
	defer conn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 3,
	}

	err = conn.CreateTopics(topicConfig)
	if err != nil {
		panic(err.Error())
	}
}

func main() {
	kafkaURL := BrokerURLs[1]
	fmt.Println(kafkaURL)
	topic := "topicTest4"

	createKafkaTopic(kafkaURL, topic)

	writer := newKafkaWriter(kafkaURL, topic)
	defer writer.Close()
	fmt.Println("start producing ... !!")

	for i := 0; ; i++ {
		keyval := rand.Intn(3)
		key := fmt.Sprintf("Key-%d", keyval)
		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(fmt.Sprint(uuid.New())),
		}
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("produced", key)
		}
		time.Sleep(1 * time.Second)
	}
}
