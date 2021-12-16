package consumer

import (
	"context"
	"fmt"
	"log"
	"queue_balancer/internal/adapters/storage/group"
	"queue_balancer/internal/adapters/storage/limits"
	groupDomain "queue_balancer/internal/domain/group"
	"queue_balancer/pkg/logging"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	/*
		Параметр для инициализирующего подключения подписчиков
		Сколько запросов в секунду можно сделать
	*/
	initialSubscribersConnectionPerSecond int = 4096

	/*
		Параметр для инициализирующего подключения подписчиков
		На сколько притормозить выполнение очередной горутины для подключения
	*/
	initialSubscribersConnectionRateLimit time.Duration = time.Second / time.Duration(initialSubscribersConnectionPerSecond)
)

type GroupSubscriberData struct {
	stopChannel      *chan int
	ratelimitChannel *chan int
}

type NatsConsumer struct {
	id                 string
	natsConnection     *nats.Conn
	streamName         string
	monitorStreamName  string
	natsJetStream      nats.JetStreamContext
	groupStorage       group.Storage
	limitsStorage      limits.Storage
	logger             logging.Logger
	groupConsumersData map[int]GroupSubscriberData
	mutex              sync.Mutex
}

// todo может стоит использовать rw mutex

func NewNatsConsumer(
	id string,
	streamName string,
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
		streamName:         streamName,
		monitorStreamName:  monitorStreamName,
		groupStorage:       groupStorage,
		logger:             logger,
		groupConsumersData: map[int]GroupSubscriberData{},
	}
}

func (nc *NatsConsumer) Run(ctx context.Context) {
	go nc.reconnectGroups()
	go nc.runMonitorSubscriber()
}

func (nc *NatsConsumer) runMonitorSubscriber() {

	monitorSubscriberSubject := nc.monitorStreamName + ".*"
	monitorSubscriberQueueGroup := nc.monitorStreamName + "_monitor"

	sub, err := nc.natsJetStream.QueueSubscribe(monitorSubscriberSubject, monitorSubscriberQueueGroup, func(msg *nats.Msg) {

		groupIdStr := msg.Header.Get("GroupId")

		if groupIdStr == "" {
			errMsg := fmt.Sprintf("nats monitor consumer - no GroupId header in message %+v", msg)
			nc.logger.Error(6203, "nats_consumer", errMsg)
			msg.Ack()
			return
		}

		groupId, err := strconv.Atoi(groupIdStr)

		if err != nil {
			errMsg := fmt.Sprintf("nats monitor consumer: GroupId header has invalid value: %s", groupIdStr)
			nc.logger.Error(6203, "nats_consumer", errMsg)
			msg.Ack()
			return
		}

		nc.mutex.Lock()
		if _, ok := nc.groupConsumersData[groupId]; ok {
			// group already has a subscriber
			nc.mutex.Unlock()
			msg.Ack()
			return
		}
		nc.mutex.Unlock()

		group, err := nc.groupStorage.GetOne(groupId)

		if err != nil {
			errMsg := fmt.Sprintf("nats monitor consumer: error while fetching group data - %s", err.Error())
			nc.logger.Error(6203, "nats_consumer", errMsg)
			msg.Ack() // nack maybe?? if the database is unavailable
			return
		}

		fmt.Printf("NEW SUBSCRIBER FOR GROUP %d \n", groupId)

		go nc.runGroupSubscriber(group)

		msg.Ack()

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

func (nc *NatsConsumer) resolveGroupSubscriberFromMonitor() {

}

func (nc *NatsConsumer) runGroupSubscriber(group *groupDomain.Group) {

	stopChannel := make(chan int, 1)
	ratelimitChannel := make(chan int, 1)

	nc.mutex.Lock()
	nc.groupConsumersData[group.GroupId] = GroupSubscriberData{
		stopChannel:      &stopChannel,
		ratelimitChannel: &ratelimitChannel,
	}
	nc.mutex.Unlock()

	go func(stopChannel *chan int, ratelimitChannel *chan int) {

		defer func() {
			fmt.Println("SOME deffer stuff - cleanup cache")
		}()

		subject := fmt.Sprintf("%s.group_%d", nc.streamName, group.GroupId)
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
				switch err {
				case nats.ErrTimeout:
					// Do nothing
				default:
					nc.logger.Error(6202, "nats_consumer", err.Error())
				}
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

// Запустить процесс переподключения к консьюмерам сообществ
func (nc *NatsConsumer) reconnectGroups() {

	offsetId := 1

	for {

		groups, err := nc.groupStorage.GetMany(offsetId, initialSubscribersConnectionPerSecond)

		if err != nil {
			log.Fatal(err)
		}

		groupsReceivedCount := len(groups)

		if groupsReceivedCount == 0 {
			break
		}

		for _, group := range groups {
			go nc.runGroupSubscriber(group)
			time.Sleep(initialSubscribersConnectionRateLimit)
		}

		offsetId = groups[groupsReceivedCount-1].GroupId

		time.Sleep(time.Second)
	}

	nc.logger.Debug(1103, "nats_consumer", "finished groups initialization")

}
