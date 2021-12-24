package consumer

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"queue_balancer/internal/adapters/publisher"
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
	initialSubscribersConnectionPerSecond int = 1024

	/*
		Параметр для инициализирующего подключения подписчиков
		На сколько притормозить выполнение очередной горутины для подключения
	*/
	initialSubscribersConnectionRateLimit time.Duration = time.Second / time.Duration(initialSubscribersConnectionPerSecond)

	/*
		На сколько притормозить получение очередного сообщения при ошибке доступа к хранилище лимитов
	*/
	timeoutOnStorageAccessError time.Duration = time.Second * time.Duration(10)

	/*
		На сколько притормозить получение очередного сообщения при окончании дневного лимита
	*/
	timeoutOnAccountLimitExceded time.Duration = time.Second * time.Duration(10)

	/*
		Таймаут ожидания активности консьюмера сообщества
	*/
	timeoutWaitingForActivity time.Duration = time.Second * time.Duration(10)

	/*
		Таймаут в случае ошибки публикации сообщения
	*/
	timeoutOnPublishError time.Duration = time.Second * time.Duration(5)

	/*
		Таймаут получения нового сообщения по умолчанию
	*/
	defaultPullTimeout time.Duration = time.Millisecond * time.Duration(5)
)

type GroupSubscriberData struct {
	stopChannel chan bool
	// ratelimitChannel *chan int
}

type NatsConsumer struct {
	id                string
	streamName        string
	monitorStreamName string

	natsJetStream nats.JetStreamContext

	groupStorage  group.Storage
	limitsStorage limits.Storage

	messagePublisher publisher.Publisher

	groupSubscriberData map[int]GroupSubscriberData
	mutex               sync.Mutex

	logger logging.Logger
}

// todo может стоит использовать rw mutex

func NewNatsConsumer(
	id string,
	streamName string,
	monitorStreamName string,
	jetStream nats.JetStreamContext,
	groupStorage group.Storage,
	limitsStorage limits.Storage,
	messagePublisher publisher.Publisher,
	logger logging.Logger,
) *NatsConsumer {

	return &NatsConsumer{
		id:                  id,
		natsJetStream:       jetStream,
		streamName:          streamName,
		monitorStreamName:   monitorStreamName,
		groupStorage:        groupStorage,
		logger:              logger,
		limitsStorage:       limitsStorage,
		messagePublisher:    messagePublisher,
		groupSubscriberData: map[int]GroupSubscriberData{},
	}
}

func (nc *NatsConsumer) Run(ctx context.Context) {

	// ci, err := nc.natsJetStream.AddConsumer(nc.streamName, &nats.ConsumerConfig{
	// 	Durable:       "bots_main",
	// 	FilterSubject: "BOTS.*",
	// 	AckPolicy:     nats.AckExplicitPolicy,
	// })

	// if err != nil {
	// 	fmt.Println("ERR creating consumer: ", err)
	// }

	// fmt.Println(ci)

	// sub, err := nc.natsJetStream.QueueSubscribe()

	// sub, err := nc.natsJetStream.Subscribe("BOTS.*", func(msg *nats.Msg) {

	// }, nats.Bind("BOTS", "bots_main"))

	sub, err := nc.natsJetStream.PullSubscribe("BOTS.*", "bots_main")

	if err != nil {
		fmt.Println("ERR creating consumer: ", err)
	}

	fmt.Println(sub)

	// go nc.reconnectGroups()
	// go nc.runMonitorSubscriber(ctx)
}

func (nc *NatsConsumer) runMonitorSubscriber(ctx context.Context) {

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

		if nc.groupHasSubscriber(groupId) {
			// errMsg := fmt.Sprintf("monitor consumer - group %d already has subs", groupId)
			// nc.logger.Debug(6203, "nats_consumer", errMsg)
			msg.Ack()
			return
		}

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

	if sub == nil {
		nc.logger.Fatal(6205, "nats_consumer", "no subscriber defined")
	}

	msg := fmt.Sprintf(
		"created monitor queue_subscriber:%s for subject:%s in node:%s",
		monitorSubscriberQueueGroup,
		monitorSubscriberSubject,
		nc.id)

	nc.logger.Debug(6202, "nats_consumer", msg)

	<-ctx.Done()

	nc.logger.Debug(6206, "nats_consumer", "unsubscribing monitor consumer")

	err = sub.Unsubscribe()

	if err != nil {
		fmt.Println(err)
	}
}

func (nc *NatsConsumer) runGroupSubscriber(group *groupDomain.Group) {

	stopChannel := make(chan bool)

	nc.mutex.Lock()
	nc.groupSubscriberData[group.GroupId] = GroupSubscriberData{
		stopChannel: stopChannel,
	}
	nc.mutex.Unlock()

	go func() {

		subscriberSubject := fmt.Sprintf("BOTS.group_%d", group.GroupId)
		subscriberQueueGroup := fmt.Sprintf("group_%d", group.GroupId)

		fmt.Println(subscriberQueueGroup, subscriberSubject)

		_, err := nc.natsJetStream.QueueSubscribe(subscriberSubject, subscriberQueueGroup, func(msg *nats.Msg) {

			nc.messagePublisher.Publish(group.GroupId, msg.Data, msg.Header)
			msg.Ack()

		}, nats.MaxAckPending(1))

		if err != nil {
			log.Println("ERR creating subscriber: ", err)
			return
		}

		fmt.Printf("create subscriber for subject: %s, durable name: %s\n", subscriberSubject, subscriberQueueGroup)
	}()

}

// func (nc *NatsConsumer) runGroupSubscriber(group *groupDomain.Group, ctx string) {

// 	stopChannel := make(chan bool)
// 	// ratelimitChannel := make(chan int, 1)

// 	nc.mutex.Lock()
// 	nc.groupSubscriberData[group.GroupId] = GroupSubscriberData{
// 		stopChannel: stopChannel,
// 		// ratelimitChannel: &ratelimitChannel,
// 	}
// 	nc.mutex.Unlock()

// 	go func() {

// 		grID := randomString(12)

// 		subject := fmt.Sprintf("%s.group_%d", nc.streamName, group.GroupId)
// 		durableName := fmt.Sprintf("group_%d", group.GroupId)

// 		pullTimeout := defaultPullTimeout

// 		sub, err := nc.natsJetStream.PullSubscribe(subject, durableName)

// 		fmt.Printf("create subscriber for subject: %s, durable name: %s\n", subject, durableName)

// 		if err != nil {
// 			fmt.Println("PULL SUBSCRIBE ERR: ", err)
// 			return
// 		}

// 		active := false

// 		go func() {
// 			time.Sleep(timeoutWaitingForActivity)
// 			if !active {
// 				close(stopChannel)
// 			}
// 		}()

// 		for {

// 			msgs, err := sub.Fetch(1)

// 			if err != nil {
// 				switch err {
// 				case nats.ErrTimeout:
// 					// Do nothing
// 				default:
// 					nc.logger.Error(6202, "nats_consumer", err.Error())
// 				}
// 			}

// 			for _, msg := range msgs {

// 				currentAccountLimit, err := nc.limitsStorage.GetAccountOperationsDailyLimit(group.UserId)

// 				if err != nil {
// 					fmt.Println(err)
// 					/*Oшибка в процессе доступа к счетчику\хранилищу лимитов аккаунта
// 					- Хранилище недоступно
// 					- Счетчик не найден
// 					*/
// 					pullTimeout = timeoutOnStorageAccessError
// 					msg.Nak()

// 				} else if currentAccountLimit == 0 {
// 					/*
// 						Лимит действий для аккаунта исчерпан
// 					*/
// 					fmt.Println(grID+" stuck. waiting:", string(msg.Data), len(msgs), ctx)
// 					pullTimeout = timeoutOnAccountLimitExceded
// 					msg.Nak()

// 				} else {
// 					err := nc.messagePublisher.Publish(group.GroupId, msg.Data, msg.Header)

// 					if err != nil {
// 						msg.Nak()
// 						pullTimeout = timeoutOnPublishError
// 					} else {
// 						msg.Ack()
// 						nc.limitsStorage.DecreaseAccountOperationsDailyLimit(group.UserId, 1)
// 						pullTimeout = defaultPullTimeout
// 					}

// 					// TODO - push to another queue
// 				}

// 				active = true
// 			}

// 			select {
// 			case <-time.After(pullTimeout):
// 				continue
// 			// case <-*ratelimitChannel:
// 			// 	continue
// 			case _, ok := <-stopChannel:
// 				if !ok {
// 					nc.unsetGroupSubscriber(group.GroupId)
// 					stopMsg := fmt.Sprintf("STOP GROUP: %d", group.GroupId)
// 					nc.logger.Debug(6206, "nats_consumer", stopMsg)
// 				}
// 				return
// 			}
// 		}

// 	}()
// }

func (nc *NatsConsumer) Close() {
	for groupId := range nc.groupSubscriberData {
		close(nc.groupSubscriberData[groupId].stopChannel)
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

			if nc.groupHasSubscriber(group.GroupId) {
				errMsg := fmt.Sprintf("groups initialization: group %d already has subs", group.GroupId)
				nc.logger.Debug(6204, "nats_consumer", errMsg)
				continue
			}

			go nc.runGroupSubscriber(group)
			time.Sleep(initialSubscribersConnectionRateLimit)
		}

		offsetId = groups[groupsReceivedCount-1].GroupId

		time.Sleep(time.Second)
	}

	nc.logger.Debug(6203, "nats_consumer", "finished groups initialization")

}

// Попробовать объеденить логику проверки и создания кэшируемой структуры
func (nc *NatsConsumer) resolveGroupSubscriber() {

}

// Удалени информации о подписчике сообщества из внутреннего кэша
func (nc *NatsConsumer) unsetGroupSubscriber(groupId int) {
	nc.mutex.Lock()
	delete(nc.groupSubscriberData, groupId)
	nc.mutex.Unlock()
}

// Проверка - существует ли подписчик для сообщества во внутреннем кэше
func (nc *NatsConsumer) groupHasSubscriber(groupId int) bool {
	nc.mutex.Lock()

	defer nc.mutex.Unlock()

	if _, ok := nc.groupSubscriberData[groupId]; ok {
		return true
	}

	return false
}

func randomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[:length]
}
