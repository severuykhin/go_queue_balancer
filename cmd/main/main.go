package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"queue_balancer/internal/adapters/consumer"
	"queue_balancer/internal/adapters/storage/group"
	"queue_balancer/internal/adapters/storage/limits"
	"queue_balancer/pkg/logging"
	"strconv"
	"syscall"
	"time"
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

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	groupStorage := group.NewStubStorage()
	limitsStorage := limits.NewStorage(limits.StorageConfig{
		DB:       limitsDbId,
		Addr:     redisHost,
		Password: redisPassword,
	})

	cons := consumer.NewNatsConsumer(
		consumerId,
		streamName,
		monitorStreamName,
		groupStorage,
		limitsStorage,
		logger,
	)

	ctx, cancel := context.WithCancel(context.Background())

	go cons.Run(ctx)

	<-signals

	cancel()
	cons.Close()

	time.Sleep(time.Second * 5)

	fmt.Println("Shutdown")
}
