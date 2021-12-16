package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
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

		groupId := rand.Intn(10000-1) + 1

		fmt.Println(groupId)

		data := fmt.Sprintf("group_id:%d hello %d", groupId, i)
		subj := fmt.Sprintf("BOTS.group_%d", groupId)
		subj2 := fmt.Sprintf("BOTS_MONITOR.group_%d", groupId)

		headers := map[string][]string{}

		headers["GroupId"] = []string{strconv.Itoa(groupId)}

		msg1 := nats.Msg{
			Subject: subj,
			Data:    []byte(data),
			Header:  headers,
		}

		msg2 := nats.Msg{
			Subject: subj2,
			Data:    []byte(data),
			Header:  headers,
		}

		pubRes, err := js.PublishMsg(&msg1)
		pubRes2, err2 := js.PublishMsg(&msg2)

		fmt.Println(data)

		if err != nil {
			fmt.Println(err)
		}

		if err2 != nil {
			fmt.Println(err2)
		}

		fmt.Printf("res: %+v\n", pubRes)
		fmt.Printf("res: %+v\n", pubRes2)

		if val, ok := metaData[groupId]; ok {
			val["published"] += 1
		} else {
			metaData[groupId] = map[string]int{
				"published": 1,
			}
		}

		time.Sleep(time.Millisecond * 200)
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
