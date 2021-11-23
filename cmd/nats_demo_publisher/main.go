package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	streamName     = "BOTS"
	streamSubjects = "BOTS.*"
)

func main() {

	nc, _ := nats.Connect(nats.DefaultURL)
	js, _ := nc.JetStream(nats.PublishAsyncMaxPending(256))

	createStream(js)

	i := 1
	for {
		msg := fmt.Sprintf("hello %d", i)
		pubAck, err := js.Publish("BOTS.group_1", []byte(fmt.Sprintf("hello %d", i)))
		fmt.Println(msg)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(pubAck)

		time.Sleep(time.Second)
		i += 1

	}

}

// createStream creates a stream by using JetStreamContext
func createStream(js nats.JetStreamContext) error {
	// Check if the ORDERS stream already exists; if not, create it.
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		log.Println(err)
	}
	if stream == nil {
		log.Printf("creating stream %q and subjects %q", streamName, streamSubjects)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubjects},
		})
		if err != nil {
			return err
		}
	}
	return nil
}
