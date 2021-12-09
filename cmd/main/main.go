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
	"syscall"
	"time"
)

var (
	logger     = logging.NewLogFmt(os.Stderr, os.Stdout, "queue_balancer")
	consumerId = os.Getenv("CONSUMER_ID")
)

func main() {

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	groupStorage := group.NewStorage()
	limitsStorage := limits.NewStorage()
	cons := consumer.NewNatsConsumer(consumerId, groupStorage, limitsStorage)

	ctx, cancel := context.WithCancel(context.Background())

	go cons.Run(ctx)

	<-signals

	cancel()
	cons.Close()

	time.Sleep(time.Second * 5)

	fmt.Println("Shutdown")
}
