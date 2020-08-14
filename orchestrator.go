package main

import (
	"encoding/json"
	//"bytes"
	"fmt"
	"log"
	"strings"
	"time"

	flag "github.com/spf13/pflag"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type JSONTime time.Time

func (j JSONTime) MarshalJSON() ([]byte, error) {
	stamp := time.Time(j).Format("2006-01-02T15:04:05.000000")
	return []byte(stamp), nil
}

func (j *JSONTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	t, err := time.Parse("2006-01-02T15:04:05.000000", s)
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

type Message struct {
	StartTime JSONTime    `json:"start_time"`
	StopTime  JSONTime    `json:"stop_time"`
	UID1      string      `json:"uid1"`
	UID2      string      `json:"uid2"`
	Sub       []SubRecord `json:"sub"`
}

// BucketKey is a unique key that
//
type BucketKey struct {
	UID1 string `json:"uid1"`
}

type MessageHandler struct {
	Channel   chan Message
	Messages  []Message
	IsRunning bool
}

type OutputMessage struct {
	Location string   `json:"location"`
	Files    []string `json:"files"`
}

func (handler MessageHandler) StartProcessing(logger zap.Logger, outputChan *amqp.Channel, exchange string) {
	// var outputMessage OutputMessage
	for {
		select {
		case msg := <-handler.Channel:
			logger.Info("Received", zap.String("uid1", msg.UID1))
		default:
			// if len(handler.Messages) > 0 && (time.Now() - handler.Messages[0].StartTime) > 5min {
			// }
		}
	}

	// // If we're done, publish, then clear state, and return
	// err := outputChan.Publish(
	// 	exchange, // exchange
	// 	"",       // routing key
	// 	false,    // mandatory
	// 	false,    // immediate
	// 	amqp.Publishing{
	// 		ContentType: "text/json",
	// 		Body:        []byte(body),
	// 	})
	// failOnError(err, "Failed to publish a message")
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{
		Channel: make(chan Message, 10),
	}
}

func failOnError(logger *zap.Logger, err error, msg string) {
	if err != nil {
		logger.Fatal(msg, zap.Error(err))
	}
}

func handleMessages(logger zap.Logger, channel *amqp.Channel, msgs <-chan amqp.Delivery, exchange string) {
	// Setup our data structure that will hold the messages,
	// channel, and goroutines
	bucketMap := make(map[BucketKey]MessageHandler)

	for msg := range msgs {
		logger.Info("Received a message", zap.ByteString("body", msg.Body))

		// Deserialize message body to our Message struct
		var message Message
		unmarshalErr := json.Unmarshal(msg.Body, &message)
		if unmarshalErr != nil {
			logger.Error("Error unmarshalling message", zap.Error(unmarshalErr), zap.ByteString("body", msg.Body))
			continue
		}

		// Convert message fields into key into bucketMap
		key := BucketKey{
			UID1: message.UID1,
		}
		startTime, _ := message.StartTime.MarshalJSON()
		logger.Info(
			"Looking for key in bucketMap",
			zap.String("uid1", key.UID1),
			zap.ByteString("start_time", startTime),
		)

		// Check if `key` is already in `bucketMap`:
		// if not, add it as a new key and initiate the processing;
		if _, ok := bucketMap[key]; !ok {
			logger.Info("Key not in bucketMap... creating", zap.String("uid1", key.UID1))
			bucketMap[key] = NewMessageHandler()
			go bucketMap[key].StartProcessing(logger, channel, exchange)
		}

		// Now that bucketMap[key] exists and is
		// processing, push the message onto the channel
		bucketMap[key].Channel <- message

		// If we've made it this far, ACK the message
		ackErr := msg.Ack(false)
		if ackErr != nil {
			logger.Error("Error ACK-ing message", zap.Error(ackErr), zap.ByteString("body", msg.Body))
			continue
		}
	}
}
func main() {
	// Setup and parse the command-line flags
	amqpHost := flag.StringP("amqp-host", "i", "localhost", "Host for RMQ")
	amqpPort := flag.IntP("amqp-port", "p", 5672, "Port for RMQ")
	amqpUser := flag.StringP("amqp-user", "u", "guest", "User for RMQ")
	amqpPass := flag.StringP("amqp-pass", "P", "guest", "Password for RMQ")
	amqpInQueue := flag.StringP("amqp-in-queue", "q", "test_queue", "Queue name")
	amqpOutExchange := flag.StringP("amqp-out-exchange", "e", "test_exchange", "Exchange name")
	flag.Parse()

	// Setup logging
	logger, zapErr := zap.NewProduction()
	if zapErr != nil {
		log.Fatalf("Failed to instantiate Zap Logging: %s\n", zapErr)
	}

	// Setup AMQP connection
	amqpUri := fmt.Sprintf("amqp://%s:%s@%s:%d", *amqpUser, *amqpPass, *amqpHost, *amqpPort)
	logger.Info("Connecting to AMQP", zap.String("amqpUri", amqpUri))

	conn, err := amqp.Dial(amqpUri)
	failOnError(logger, err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	logger.Info("Opening channel", zap.String("amqpUri", amqpUri))

	ch, err := conn.Channel()
	failOnError(logger, err, "Failed to open a channel")
	defer ch.Close()

	logger.Info("Opening channel", zap.String("amqpUri", amqpUri))

	q, err := ch.QueueDeclare(
		*amqpInQueue, // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		amqp.Table{
			"x-message-ttl": 3600000,
		}, // arguments
	)
	failOnError(logger, err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(logger, err, "Failed to set QoS")

	err = ch.ExchangeDeclare(
		*amqpOutExchange, // name
		"topic",          // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	failOnError(logger, err, "Failed to declare an exchange")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(logger, err, "Failed to register a consumer")

	forever := make(chan bool)

	go handleMessages(*logger, ch, msgs, *amqpOutExchange)

	logger.Info("Starting orchestrator")
	<-forever
}
