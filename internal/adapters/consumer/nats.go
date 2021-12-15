package consumer

import (
	"context"
	"fmt"
	"log"
	"queue_balancer/internal/adapters/storage/group"
	"queue_balancer/internal/adapters/storage/limits"
	groupDomain "queue_balancer/internal/domain/group"
	"queue_balancer/pkg/logging"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type GroupConsumerData struct {
	stopChannel      *chan int
	ratelimitChannel *chan int
}

type NatsConsumer struct {
	// Конфигурируемые поля
	id                string
	monitorStreamName string
	groupStorage      group.Storage
	limitsStorage     limits.Storage
	logger            logging.Logger

	// Внутренние поля
	natsConnection     *nats.Conn
	natsJetStream      nats.JetStreamContext
	groupConsumersData map[int]GroupConsumerData
	mutex              sync.Mutex
}

// todo может стоит использовать rw mutex

func NewNatsConsumer(
	id string,
	monitorStreamName string,
	groupStorage group.Storage,
	limitsStorage limits.Storage,
	logger logging.Logger,
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
		id:                 id,
		natsConnection:     nc,
		natsJetStream:      js,
		monitorStreamName:  monitorStreamName,
		groupStorage:       groupStorage,
		logger:             logger,
		groupConsumersData: map[int]GroupConsumerData{},
	}
}

func (nc *NatsConsumer) Run(ctx context.Context) {
	go nc.initGroupsSubscribers()
	// go nc.runMonitorSubscriber()
}

func (nc *NatsConsumer) runMonitorSubscriber() {

	monitorSubscriberSubject := nc.monitorStreamName + ".*"
	monitorSubscriberQueueGroup := nc.monitorStreamName + "_consumer"

	sub, err := nc.natsJetStream.QueueSubscribe(monitorSubscriberSubject, monitorSubscriberQueueGroup, func(msg *nats.Msg) {
		fmt.Printf("MONITOR: GroupId:%s UserId:%s Data:%s \n", msg.Subject, msg.Header.Get("UserId"), string(msg.Data))
	}, nats.MaxAckPending(-1))

	if err != nil {
		nc.logger.Fatal(1101, "nats_consumer", err.Error())
		return
	}

	if sub != nil {
		msg := fmt.Sprintf(
			"created monitor queue_subscriber:%s for subject:%s in node:%s",
			monitorSubscriberQueueGroup,
			monitorSubscriberSubject,
			nc.id)
		nc.logger.Debug(1102, "nats_consumer", msg)
	}

}

func (nc *NatsConsumer) createGroupConsumer(group groupDomain.Group) {

	stopChannel := make(chan int, 1)
	ratelimitChannel := make(chan int, 1)

	nc.mutex.Lock()
	nc.groupConsumersData[group.GroupId] = GroupConsumerData{
		stopChannel:      &stopChannel,
		ratelimitChannel: &ratelimitChannel,
	}
	nc.mutex.Unlock()

	go func(stopChannel *chan int, ratelimitChannel *chan int) {

		defer func() {
			fmt.Println("SOME deffer stuff - cleanup cache")
		}()

		subject := fmt.Sprintf("BOTS.group_%d", group.GroupId)
		durableName := fmt.Sprintf("group_%d", group.GroupId)

		fmt.Println(subject)
		fmt.Println(durableName)

		rateLimit := 10                // msg per second
		pullTimeout := 100 / rateLimit // ms

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
				// sub.Drain()
				return
			}

		}

	}(&stopChannel, &ratelimitChannel)
}

func (nc *NatsConsumer) ConsumeMainStream() {

}

func (nc *NatsConsumer) Close() {
	for _, v := range nc.groupConsumersData {
		*v.stopChannel <- 1
	}

}

// Запустить процесс переподключения с очередям всех сообществ
func (nc *NatsConsumer) initGroupsSubscribers() {

	streams := nc.natsJetStream.StreamsInfo()

	var c int16

	for s := range streams {
		c++
		if s != nil {
			fmt.Printf("#%d | %s | leader: %s\n", c, s.Config.Name, s.Cluster.Leader)
		}
		time.Sleep(time.Millisecond * 10)
	}

	// streamNames := nc.natsJetStream.StreamNames()

	// var c int16

	// for name := range streamNames {
	// 	c++
	// 	fmt.Printf("#%d | %s \n", c, name)
	// 	time.Sleep(time.Millisecond * 10)
	// }

	// offsetId := 1
	// limit := 10000

	// for {

	// 	groups := nc.groupStorage.GetMany(offsetId, limit)

	// 	groupsReceivedCount := len(groups)

	// 	if groupsReceivedCount == 0 {
	// 		nc.logger.Debug(1103, "nats_consumer", "finished groups initialization")
	// 		break
	// 	}

	// 	for _, group := range groups {
	// 		fmt.Println(group)
	// 		go nc.createGroupConsumer(group)
	// 	}

	// 	offsetId = groups[groupsReceivedCount-1].GroupId

	// 	time.Sleep(time.Millisecond * 1000)
	// }

}
