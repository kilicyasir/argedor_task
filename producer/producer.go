package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func initTopic(brokers string, topic string, partitionCount int) {
	/*
		Inits the topic if it doesn't exist
	*/
	conn, err := kafka.Dial("tcp", brokers)
	if err != nil {
		log.Fatalf("Couldn't connect to kafka broker: %v", err)
		panic(err.Error())
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	//check the partitions' topics
	for _, p := range partitions {
		if p.Topic == topic {
			log.Printf("Topic %v already exists\n", topic)
			return
		}
	}

	// get controller to create topic
	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{{Topic: topic, NumPartitions: partitionCount, ReplicationFactor: 1}}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
	log.Printf("Topic %v is created\n", topic)
}

func main() {
	KAFKA_BROKER := os.Getenv("KAFKA_BROKER")
	TOPIC := os.Getenv("TOPIC")

	log.Println("Producer started running")
	log.Printf("Connecting to brokers: %v and topic: %v\n", KAFKA_BROKER, TOPIC)

	initTopic(KAFKA_BROKER, TOPIC, 5) // hardcoded 5 partition

	w := &kafka.Writer{
		Addr:  kafka.TCP(KAFKA_BROKER),
		Topic: TOPIC,
	}
	defer w.Close()

	log.Println("Starting to publish messages")

	for i := 0; ; i++ {
		time.Sleep(time.Second) // wait for a second
		err := w.WriteMessages(context.Background(),
			kafka.Message{
				Value: []byte(fmt.Sprintf("Hello World %v", i)),
			})

		if err != nil {
			log.Printf("Couldn't publish the message.\n%v\n", err.Error())
		}
	}
}
