package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

var wg sync.WaitGroup

func consumerLoop(broker string, topic string, thread_id int) {
	//signal end of routine
	defer wg.Done()

	//init reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		GroupID:  "base-consumer",
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6, // 10MB
	})

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Couldn't read message:\n%v\n", err.Error())
		}

		//wait 5 to 10 secs
		sec := rand.Intn(10 - 5 + 1)
		time.Sleep(time.Duration(5+sec) * time.Second)

		log.Printf("Message: %v, Thread: %v\n", string(msg.Value), thread_id)
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func main() {
	KAFKA_BROKER := os.Getenv("KAFKA_BROKER")
	TOPIC := os.Getenv("TOPIC")
	THREAD_COUNT, err := strconv.Atoi(os.Getenv("THREAD_COUNT"))

	if err != nil {
		log.Fatalf("Invalid THREAD_COUNT: %v", os.Getenv("THREAD_COUNT"))
	}

	log.Println("Consumer started running")
	log.Printf("Connecting to brokers: %v and topic: %v\n", KAFKA_BROKER, TOPIC)

	wg.Add(THREAD_COUNT - 1) //exclude current thread

	for i := 0; i < THREAD_COUNT-1; i++ {
		go consumerLoop(KAFKA_BROKER, TOPIC, i)
	}

	consumerLoop(KAFKA_BROKER, TOPIC, THREAD_COUNT-1)
	wg.Wait()
}
