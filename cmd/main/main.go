package main

import (
	"os"
	"queue_balancer/internal/connectors/consumer"
	"queue_balancer/pkg/logging"
)

var (
	logger = logging.NewLogFmt(os.Stderr, os.Stdout, "queue_balancer")
)

func main() {

	forever := make(chan os.Signal, 1)

	c := consumer.NewNatsConsumer()

	go func() {
		c.ConsumeMainStream()
	}()

	c.Close()

	<-forever
}
