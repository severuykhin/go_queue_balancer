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
		Таймаут получения нового сообщения по умолчанию
	*/
	defaultPullTimeout time.Duration = time.Millisecond * time.Duration(5)
)

type GroupSubscriberData struct {
	stopChannel      *chan bool
	ratelimitChannel *chan int
}

type NatsConsumer struct {
	id                  string
	natsConnection      *nats.Conn
	streamName          string
	monitorStreamName   string
	natsJetStream       nats.JetStreamContext
	groupStorage        group.Storage
	limitsStorage       limits.Storage
	logger              logging.Logger
	groupSubscriberData map[int]GroupSubscriberData
	mutex               sync.Mutex
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
		id:                  id,
		natsConnection:      nc,
		natsJetStream:       js,
		streamName:          streamName,
		monitorStreamName:   monitorStreamName,
		groupStorage:        groupStorage,
		logger:              logger,
		limitsStorage:       limitsStorage,
		groupSubscriberData: map[int]GroupSubscriberData{},
	}
}

func (nc *NatsConsumer) Run(ctx context.Context) {
	go nc.reconnectGroups()
	go nc.runMonitorSubscriber(ctx)
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

	stopChannel := make(chan bool, 1)
	ratelimitChannel := make(chan int, 1)

	nc.mutex.Lock()
	nc.groupSubscriberData[group.GroupId] = GroupSubscriberData{
		stopChannel:      &stopChannel,
		ratelimitChannel: &ratelimitChannel,
	}
	nc.mutex.Unlock()

	go func(group *groupDomain.Group, stopChannel *chan bool, ratelimitChannel *chan int, logger logging.Logger, lStorage limits.Storage) {

		defer func() {
			nc.unsetGroupSubscriber(group.GroupId)
			stopMsg := fmt.Sprintf("STOP GROUP: %d", group.GroupId)
			logger.Debug(6206, "nats_consumer", stopMsg)
		}()

		subject := fmt.Sprintf("%s.group_%d", nc.streamName, group.GroupId)
		durableName := fmt.Sprintf("group_%d", group.GroupId)

		pullTimeout := defaultPullTimeout

		sub, err := nc.natsJetStream.PullSubscribe(subject, durableName)

		fmt.Printf("create subscriber for subject: %s, durable name: %s\n", subject, durableName)

		if err != nil {
			fmt.Println("ERR: ", err)
			return
		}

		active := false

		go func() {
			time.Sleep(timeoutWaitingForActivity)
			if !active {
				*stopChannel <- true
			}
		}()

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

				currentAccountLimit, err := lStorage.GetAccountOperationsDailyLimit(group.UserId)
				// fmt.Println("NEW MESSAGE: ", currentAccountLimit, len(msgs))

				/*
					Ошибка в процессе доступа к счетчику\хранилищу лимитов аккаунта
					- Хранилище недоступно
					- Счетчик не найден
				*/
				if err != nil {
					pullTimeout = timeoutOnStorageAccessError
					msg.InProgress()
				} else if currentAccountLimit == 0 {
					pullTimeout = timeoutOnAccountLimitExceded
					msg.InProgress()
				} else {
					fmt.Println(string(msg.Data))
					msg.Ack()
					lStorage.DecreaseAccountOperationsDailyLimit(group.UserId, 1)
					pullTimeout = defaultPullTimeout
					// TODO - push to another queue
				}

				active = true
			}

			select {
			case <-time.After(pullTimeout):
				continue
			case <-*ratelimitChannel:
				continue
			case <-*stopChannel:
				return
			}

		}

	}(group, &stopChannel, &ratelimitChannel, nc.logger, nc.limitsStorage)
}

func (nc *NatsConsumer) Close() {
	for _, v := range nc.groupSubscriberData {
		*v.stopChannel <- true
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
