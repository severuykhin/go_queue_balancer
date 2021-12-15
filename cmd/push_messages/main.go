package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	nc, _ := nats.Connect(nats.DefaultURL)
	js, _ := nc.JetStream(nats.PublishAsyncMaxPending(256))

	var i int16
	var run bool

	metaData := map[int]map[string]int{}

	run = true

	for run {

		groupId := rand.Intn(1000-1) + 1
		streamName := fmt.Sprintf("BOST_GROUP_%d", groupId)
		streamSubject := streamName + ".*"
		streamSubjects := []string{streamSubject}

		err := assertStream(js, streamName, streamSubjects)

		if err != nil {
			fmt.Println(err)
			continue
		}

		msgId := fmt.Sprintf("%d", i)
		data := fmt.Sprintf("hello %d", i)

		headers := map[string][]string{}
		headers["UserId"] = []string{"3"}

		msg := nats.Msg{
			Subject: streamSubject,
			Header:  headers,
			Data:    []byte(data),
		}

		pubRes, err := js.PublishMsg(&msg, nats.MsgId(msgId))

		fmt.Printf("%+v\n", msg)

		if err != nil {
			fmt.Println(err)
		}

		fmt.Printf("res: %+v\n", pubRes)

		if val, ok := metaData[groupId]; ok {
			val["published"] += 1
		} else {
			metaData[groupId] = map[string]int{
				"published": 1,
			}
		}

		time.Sleep(time.Millisecond * 10)
		i += 1

		select {
		case <-signals:
			run = false
		default:
			// noop
		}
	}

	fmt.Printf("%+v", metaData)

}

func assertStream(js nats.JetStreamContext, streamName string, streamSubjects []string) error {

	stream, err := js.StreamInfo(streamName)

	if err != nil && stream == nil {
		_, err := js.AddStream(&nats.StreamConfig{
			Name:       streamName,
			Subjects:   streamSubjects,
			Storage:    nats.FileStorage,
			Retention:  nats.WorkQueuePolicy,
			Duplicates: time.Minute, // в целом принимете любой time.Duration
		})

		if err != nil {
			return err
		}
	}

	return nil

}
