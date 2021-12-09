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

	i := 1

	metaData := map[int]map[string]int{}

	run := true

	for run == true {

		groupId := rand.Intn(6-1) + 1

		fmt.Println(groupId)

		msg := fmt.Sprintf("group_id:%d hello %d", groupId, i)
		subj := fmt.Sprintf("BOTS.group_%d", groupId)

		pubRes, err := js.Publish(subj, []byte(msg))

		fmt.Println(msg)

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

		time.Sleep(time.Millisecond * 100)
		i += 1

		select {
		case <-signals:
			run = false
		default:
			// noop
		}
	}

	fmt.Printf("%v", metaData)

}
