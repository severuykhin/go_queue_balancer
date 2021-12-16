package main

import (
	"fmt"
	"log"
	"queue_balancer/internal/constants"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {

	/*

		Исключительно для локального тестирования
		Неприменять в продакшене

	*/

	nc, _ := nats.Connect(nats.DefaultURL)
	js, _ := nc.JetStream(nats.PublishAsyncMaxPending(256))

	deleteStreamIfExists(js, constants.MAIN_STREAM_NAME)
	deleteStreamIfExists(js, constants.MONITOR_STREAM_NAME)

	log.Printf("creating stream %q and subjects %q", constants.MAIN_STREAM_NAME, constants.MAIN_STREAM_SUBJECTS)

	mainStream, err := js.AddStream(&nats.StreamConfig{
		Name:       constants.MAIN_STREAM_NAME,
		Subjects:   []string{constants.MAIN_STREAM_SUBJECTS},
		Storage:    nats.FileStorage,
		Retention:  nats.WorkQueuePolicy,
		Duplicates: time.Minute,
	})

	if err != nil {
		fmt.Println(err)
	}

	log.Printf("creating monitor stream %q and subjects %q", constants.MONITOR_STREAM_NAME, constants.MONITOR_STREAM_SUBJECTS)

	monitorStream, err := js.AddStream(&nats.StreamConfig{
		Name:       constants.MONITOR_STREAM_NAME,
		Subjects:   []string{constants.MONITOR_STREAM_SUBJECTS},
		Storage:    nats.FileStorage,
		Retention:  nats.WorkQueuePolicy,
		Duplicates: time.Minute,
	})

	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("%+v", mainStream)
	fmt.Printf("%+v", monitorStream)
}

func deleteStreamIfExists(js nats.JetStreamContext, streamName string) {
	stream, err := js.StreamInfo(streamName)

	if err != nil {
		fmt.Println("err: ", err)
	}

	if stream != nil {
		err := js.DeleteStream(streamName)
		if err != nil {
			fmt.Println(err)
		}
	}
}
