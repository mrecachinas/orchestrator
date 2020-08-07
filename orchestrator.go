package main

import (
	"encoding/json"
	//"bytes"
	"fmt"
	flag "github.com/spf13/pflag"
	"github.com/streadway/amqp"
	"log"
	"strings"
	"time"
)

type JSONTime time.Time

func (j JSONTime) MarshalJSON() ([]byte, error) {
	stamp := fmt.Sprintf("\"%s\"", time.Time(j).Format("2006-01-02T15:04:05.000"))
	return []byte(stamp), nil
}

func (j *JSONTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	t, err := time.Parse("2006-01-02T15:04:05.000", s)
	if err != nil {
		return err
	}
	*j = JSONTime(t)
	return nil
}

type SubRecord struct {
	StartTime JSONTime `json:"start_time"`
	StopTime  JSONTime `json:"stop_time"`
}

func (s *SubRecord) MarshalJSON() ([]byte, error) {

}

func (s *SubRecord) UnmarshalJSON(b []byte) error {

}

type MessageType struct {
	StartTime JSONTime    `json:"start_time"`
	StopTime  JSONTime    `json:"stop_time"`
	UID1      string      `json:"uid1"`
	UID2      string      `json:"uid2"`
	Sub       []SubRecord `json:"sub"`
}

type SpecialKey struct {
	UID1 string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func orchestrate() {

}

func handleMessages(msgs <-chan amqp.Delivery, exchange string) {
	bucketMap := make(map[SpecialKey]chan MessageType)
	for msg := range msgs {
		log.Printf("Received a message: %s", msg.Body)
		// Deserialize message body to our MessageType struct
		var message MessageType
		unmarshalErr := json.Unmarshal(msg.Body, &message)
		if unmarshalErr != nil {
			log.Fatal("Oh noes")
		}

		// Dump into corresponding subrecord
		key := SpecialKey{
			UID1: message.UID1,
		}
		bucketMap[key] = make(chan MessageType, 10)
		bucketMap[key] <- message

		err := ch.Publish(
			exchange, // exchange
			"",       // routing key
			false,    // mandatory
			false,    // immediate
			amqp.Publishing{
				ContentType: "text/json",
				Body:        []byte(body),
			})
		failOnError(err, "Failed to publish a message")
		ackErr := msg.Ack(false)
		if ackErr != nil {
			failOnError(err, "Error ACK-ing message")
		}
	}
}
func main() {
	amqpHost := flag.StringP("amqp-host", "i", "localhost", "Host for RMQ")
	amqpPort := flag.IntP("amqp-port", "p", 5672, "Port for RMQ")
	amqpUser := flag.StringP("amqp-user", "u", "guest", "User for RMQ")
	amqpPass := flag.StringP("amqp-pass", "P", "guest", "Password for RMQ")
	amqpInQueue := flag.StringP("amqp-in-queue", "q", "test", "Queue name")
	amqpOutExchange := flag.StringP("amqp-out-exchange", "e", "test", "Exchange name")
	flag.Parse()

	amqpUri := fmt.Sprintf("amqp://%s:%s@%s:%d", *amqpUser, *amqpPass, *amqpHost, *amqpPort)
	fmt.Println(amqpUri)

	conn, err := amqp.Dial(amqpUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		*amqpInQueue, // name
		true, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		amqp.Table{
			"x-message-ttl": 3600000,
		}, // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	err = ch.ExchangeDeclare(
		*amqpOutExchange, // name
		"topic", // type
		true, // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil, // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	msgs, err := ch.Consume(
		q.Name, // queue
		"", // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go handleMessages(msgs, *amqpOutExchange)

	log.Printf("Starting orchestrator")
	<-forever
}
