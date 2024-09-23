package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Message struct {
	ID        int64
	Topic     string
	Partition int
	Payload   string
	Timestamp string
}

type Broker struct {
	db *sql.DB
	mu sync.Mutex
}

// initiate a broker
func NewBroker(url string) (*Broker, error) {
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	return &Broker{db: db}, nil
}

// create message table
func (b *Broker) CreateTables() error {
	_, err := b.db.Exec(`
			CREATE TABLE IF NOT EXISTS messages (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			topic VARCHAR(255) NOT NULL,
			partitions INT NOT NULL,
			payload TEXT NOT NULL,
			timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			INDEX idx_topic_partition (topic, partitions) );
	`)
	return err
}

// produce messages
func (b *Broker) Produce(topic string, partition int, payload string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, err := b.db.Exec("INSERT INTO messages (topic, partitions, payload) VALUES (?, ?, ?)", topic, partition, payload)
	return err
}

// consume messages
func (b *Broker) Consume(topic string, partition int, lastOffset int64) ([]Message, error) {
	rows, err := b.db.Query("SELECT id, topic, partitions, payload, timestamp FROM messages WHERE topic = ? AND partitions = ? AND id > ? ORDER BY id LIMIT 10", topic, partition, lastOffset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var m Message
		err := rows.Scan(&m.ID, &m.Topic, &m.Partition, &m.Payload, &m.Timestamp)
		if err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}

	return messages, nil
}

func main() {
	log.Println("Kafka broker SQL")

	// initialize broker
	broker, err := NewBroker("root:123456@tcp(localhost:3306)/kafkabroker")
	if err != nil {
		log.Fatal(err)
	}
	// create table to push messages
	err = broker.CreateTables()
	if err != nil {
		log.Fatal(err)
	}

	// producer
	go func() {
		for i := 0; i < 100; i++ {
			err := broker.Produce("test-topic", i%3, fmt.Sprintf("Message %d", i))
			if err != nil {
				log.Printf("Error producing message: %v", err)
			}
			time.Sleep(time.Millisecond * 100)
		}
	}()

	// consumer
	go func() {
		var lastOffset int64 = 0 // last offset same like kafka, updating lastOffest after rows consumption

		// check for new message on every 1 second interval and update the lastOffset
		for {
			messages, err := broker.Consume("test-topic", 1, lastOffset)
			if err != nil {
				log.Printf("Error consuming messages: %v", err)
				continue
			}

			for _, msg := range messages {
				fmt.Printf("Consumed: %+v\n", msg)
				lastOffset = msg.ID
			}
			time.Sleep(time.Second)
		}
	}()

	// block main thread to consume message in realtime
	select {}
}
