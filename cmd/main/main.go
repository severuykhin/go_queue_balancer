package main

import (
	"encoding/json"
	"os"
	"queue_balancer/internal/constants"
	"queue_balancer/internal/types"
	"queue_balancer/pkg/logging"
	"queue_balancer/pkg/queue_service"

	"github.com/streadway/amqp"
)

var (
	logger = logging.NewLogFmt(os.Stderr, os.Stdout, "queue_balancer")
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	if err != nil {
		logger.Fatal(123, "app", err.Error())
	}

	defer conn.Close()

	qs := queue_service.New(conn)

	qs.Init()

	go func() {
		listenControlQueue(conn, qs)
	}()

	forever := make(chan bool)

	<-forever

}

func listenControlQueue(conn *amqp.Connection, qs *queue_service.QueueService) {

	channelControlQueue, err := conn.Channel()

	if err != nil {
		logger.Fatal(123, "app", err.Error())
	}

	channelControlQueue.QueueDeclare(
		"control_queue", // queue name
		true,            // durable
		false,           // auto delete
		false,           // exclusive
		false,           // no wait
		nil,             // arguments
	)

	messages, err := channelControlQueue.Consume(
		"control_queue", // queue name
		"",              // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no local
		false,           // no wait
		nil,             // arguments
	)

	if err != nil {
		logger.Fatal(123, "app", err.Error())
	}

	logger.Info(123, "control_queue", "Start to listen control queue")

	forever := make(chan bool)

	go func() {
		for message := range messages {

			if message.Headers["type"] == nil {
				// TODO some stuff
				continue
			}

			switch messageType := message.Headers["type"]; messageType {

			case constants.DECALRE_QUEUE:
				var decalreQueueTask types.CreateQueueTask
				_ = json.Unmarshal(message.Body, &decalreQueueTask)

			case constants.SLOW_DOWN_QUEUE:
				var slowDownQueueTask types.SlowDownQueue
				_ = json.Unmarshal(message.Body, &slowDownQueueTask)
				qs.SlowDownQueue(slowDownQueueTask.GroupId, slowDownQueueTask.Ratio)
			}

			message.Ack(false)
		}
	}()

	<-forever

}
