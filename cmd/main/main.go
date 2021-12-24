package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"queue_balancer/internal/adapters/consumer"
	"queue_balancer/internal/adapters/publisher"
	"queue_balancer/internal/adapters/storage/group"
	"queue_balancer/internal/adapters/storage/limits"
	"queue_balancer/pkg/logging"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	consumerId        = os.Getenv("CONSUMER_ID")
	streamName        = os.Getenv("STREAM_NAME")
	monitorStreamName = os.Getenv("MONITOR_STREAM_NAME")

	redisDeliveryReadCaheDb = os.Getenv("REDIS_ACCOUNTS_LIMITS_DB")
	redisHost               = os.Getenv("REDIS_HOST")
	redisPassword           = os.Getenv("REDIS_PASSWORD")

	logger = logging.NewLogFmt(os.Stderr, os.Stdout, "queue_balancer_"+consumerId)
)

func main() {

	if consumerId == "" {
		logger.Fatal(6201, "main", "CONSUMER_ID env varible not provided")
	}

	if streamName == "" {
		logger.Fatal(6201, "main", "STREAM_NAME env variable not provided")
	}

	if monitorStreamName == "" {
		logger.Fatal(6201, "main", "MONITOR_STREAM_NAME env variable not provided")
	}

	if redisDeliveryReadCaheDb == "" {
		logger.Fatal(6201, "main", "REDIS_ACCOUNTS_LIMITS_DB env variable not provided")
	}

	if redisHost == "" {
		logger.Fatal(6201, "main", "REDIS_HOST env variable not provided")
	}

	if redisPassword == "" {
		logger.Fatal(6201, "main", "REDIS_PASSWORD env variable not provided")
	}

	limitsDbId, err := strconv.Atoi(redisDeliveryReadCaheDb)

	if err != nil {
		logger.Fatal(6201, "main", "error parsing redisDeliveryReadCaheDb variable. actual value is "+redisDeliveryReadCaheDb)
	}

	natsConnection, err := nats.Connect(nats.DefaultURL)

	if err != nil {
		logger.Fatal(6202, "app_run", "cannot connect to nats")
	}

	jetStream, err := natsConnection.JetStream(nats.PublishAsyncMaxPending(256))

	if err != nil {
		logger.Fatal(6203, "app_run", "cannot connect to nats jetstream")
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	groupStorage := group.NewStubStorage()

	limitsStorage := limits.NewStorage(limits.StorageConfig{
		DB:       limitsDbId,
		Addr:     redisHost,
		Password: redisPassword,
	})

	messagePublisher := publisher.NewStubPublisher()

	cons := consumer.NewNatsConsumer(
		consumerId,
		streamName,
		monitorStreamName,
		jetStream,
		groupStorage,
		limitsStorage,
		messagePublisher,
		logger,
	)

	ctx, cancel := context.WithCancel(context.Background())

	go cons.Run(ctx)

	<-signals

	cancel()
	cons.Close()

	time.Sleep(time.Second * 2)

	printReport(messagePublisher)

	fmt.Println("Shutdown")
}

func printReport(mp *publisher.StubPublisher) {
	var totalValue int

	rep := mp.GetInfo()

	fmt.Println("=======================")
	for key := range rep {
		totalValue += rep[key]
		fmt.Printf("Group: %d | Messages: %d \n", key, rep[key])
		fmt.Println("-------------------------")
	}

	fmt.Println("========================")
	fmt.Printf("Total value: %d\n", totalValue)
	fmt.Println("========================")

}
