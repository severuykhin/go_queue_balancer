package consumer

import (
	"context"
	"fmt"
	"log"
	"queue_balancer/internal/adapters/storage/group"
	"queue_balancer/internal/adapters/storage/limits"
	groupDomain "queue_balancer/internal/domain/group"
	"time"

	"github.com/nats-io/nats.go"
)

type ConsumerGroupMetaData struct {
	stopChannel      *chan int
	ratelimitChannel *chan int
}

type NatsConsumer struct {
	id             string
	natsConnection *nats.Conn
	natsJetStream  nats.JetStreamContext
	groupStorage   group.Storage
	limitsStorage  limits.Storage
	metaData       map[int]ConsumerGroupMetaData
}

func NewNatsConsumer(
	id string,
	groupStorage group.Storage,
	limitsStorage limits.Storage,
) *NatsConsumer {
	nc, err := nats.Connect(nats.DefaultURL)

	if err != nil {
		log.Fatal(err)
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))

	if err != nil {
		log.Fatal(err)
	}

	return &NatsConsumer{
		id:             id,
		natsConnection: nc,
		natsJetStream:  js,
		groupStorage:   groupStorage,
		metaData:       map[int]ConsumerGroupMetaData{},
	}
}

func (nc *NatsConsumer) Run(ctx context.Context) {
	// nc.reconnectGroups()
	go func() {
		sub, err := nc.natsJetStream.PullSubscribe("BOTS.*", "bots_main_consumer")
		if err != nil {
			fmt.Println(err)
		}
		for {
			msgs, err := sub.Fetch(1)

			if err != nil {
				fmt.Println("some err: ", err)
			}

			for _, msg := range msgs {
				fmt.Println(msg.Subject, string(msg.Data))
				msg.Ack()
			}

			time.Sleep(time.Second)

		}
	}()

	// go func() {
	// 	sub, err := nc.natsJetStream.PullSubscribe("BOTS.*", "bots_mirror_consumer")

	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	for {
	// 		msgs, err := sub.Fetch(1)

	// 		if err != nil {
	// 			fmt.Println(err)
	// 		}

	// 		for _, msg := range msgs {
	// 			fmt.Println(msg.Subject, string(msg.Data))
	// 			msg.Ack()
	// 		}

	// 		time.Sleep(time.Second * 2)

	// 	}
	// }()

}

func (nc *NatsConsumer) CreateGroupConsumer(group groupDomain.Group) {

	stopChannel := make(chan int, 1)
	ratelimitChannel := make(chan int, 1)
	heartbeatChannel := make(chan bool, 1)

	nc.metaData[group.GroupId] = ConsumerGroupMetaData{
		stopChannel:      &stopChannel,
		ratelimitChannel: &ratelimitChannel,
	}

	go func(stopChannel *chan int, ratelimitChannel *chan int, heartbeatChannel *chan bool) {

		defer func() {
			fmt.Println("SOME deffer stuff - cleanup cache")
		}()

		subject := fmt.Sprintf("BOTS.group_%d", group.GroupId)
		durableName := fmt.Sprintf("group_%d", group.GroupId)

		rateLimit := 10                 // msg per second
		pullTimeout := 1000 / rateLimit // ms

		sub, err := nc.natsJetStream.PullSubscribe(subject, durableName)

		count := 0

		fmt.Printf("create subscriber for subject: %s, durable name: %s\n", subject, durableName)

		if err != nil {
			fmt.Println("ERR: ", err)
			return
		}

		for {

			msgs, err := sub.Fetch(1)

			if err != nil {
				fmt.Println(err)
			}

			if len(msgs) > 0 {
				// active = true
			}

			for _, msg := range msgs {
				fmt.Println(string(msg.Data))
				count += 1
				msg.Ack()
			}

			select {
			case <-time.After(time.Millisecond * time.Duration(pullTimeout)):
				continue
			case <-*ratelimitChannel:
				// тут можно менять rateLimit
			case <-*stopChannel:
				fmt.Println("STOP GROUP")
				fmt.Printf("group_id: %d  count:%d \n", group.GroupId, count)
				err := sub.Drain()
				if err != nil {
					fmt.Println(err)
				}
				return
			}

		}

	}(&stopChannel, &ratelimitChannel, &heartbeatChannel)
}

func (nc *NatsConsumer) ConsumeMainStream() {

}

func (nc *NatsConsumer) Close() {
	fmt.Println(nc.metaData)
	for _, v := range nc.metaData {
		*v.stopChannel <- 1
	}

}

// Запустить процесс переподключения с очередям всех сообществ
func (nc *NatsConsumer) reconnectGroups() {

	groups := nc.groupStorage.GetByOffset(0, 1)

	for _, group := range groups {
		go nc.CreateGroupConsumer(group)
	}
}
