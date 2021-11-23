package main

import (
	"fmt"
	"os"
	"queue_balancer/pkg/logging"

	"github.com/nats-io/nats.go"
)

var (
	logger = logging.NewLogFmt(os.Stderr, os.Stdout, "queue_balancer")
)

func main() {

	forever := make(chan os.Signal)

	// Connect to NATS
	nc, _ := nats.Connect(nats.DefaultURL)

	fmt.Println(nc.Status())
	fmt.Println("=============")
	fmt.Printf("%+v\n", nc.Stats())
	fmt.Println("=============")
	fmt.Println(nc.Servers())

	// Create JetStream Context
	js, _ := nc.JetStream(nats.PublishAsyncMaxPending(256))

	// Simple Async Ephemeral Consumer
	subs, err := js.Subscribe("BOTS.*", func(m *nats.Msg) {
		fmt.Printf("Received a JetStream message: %s with subject: %s\n", string(m.Data), m.Subject)
	})

	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("%+v\n", subs)

	<-forever

}
