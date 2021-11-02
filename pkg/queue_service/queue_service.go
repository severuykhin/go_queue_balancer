package queue_service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"queue_balancer/pkg/logging"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

var logger = logging.NewLogFmt(os.Stderr, os.Stdout, "queue_balancer")

type Queue struct {
	Name string `json:"name"`
}

type QueueListApiResponse struct {
	FilterdCount int `json:"filtered_count"`
	ItemCount    int `json:"item_count"`
	Page         int `json:"page"`
	PageCount    int `json:"page_count"`
	PageSize     int `json:"page_size"`
	TotalCount   int `json:"total_count"`
	Items        []Queue
}

type QueueService struct {
	AmqpConnection  *amqp.Connection
	AmqpChannel     *amqp.Channel
	ActiveConsumers map[string]Consumer
}

type QueueChannelTask struct {
	Type  string
	Ratio int
}

type Consumer struct {
	Channel   chan QueueChannelTask
	QueueName string
}

func New(connection *amqp.Connection) *QueueService {

	channelControlQueue, err := connection.Channel()

	if err != nil {
		// todo - log
		fmt.Println(err)
		os.Exit(1)
	}

	return &QueueService{
		AmqpConnection:  connection,
		AmqpChannel:     channelControlQueue,
		ActiveConsumers: make(map[string]Consumer),
	}
}

func (qs *QueueService) Init() {
	queueList, err := qs.getQueueList()

	if err != nil {
		fmt.Println(err)
	}

	for _, queue := range queueList {
		qs.ConsumeQueue(queue)
	}

}

func (qs *QueueService) DeclareQueue(q Queue) {

}

func (qs *QueueService) ConsumeQueue(q Queue) {

	go func(qs *QueueService) {

		qs.AmqpChannel.QueueDeclare(
			q.Name, // queue name
			true,   // durable
			false,  // auto delete
			false,  // exclusive
			false,  // no wait
			nil,    // arguments
		)

		messages, err := qs.AmqpChannel.Consume(
			q.Name, // queue name
			"",     // consumer
			false,  // auto-ack
			false,  // exclusive
			false,  // no local
			false,  // no wait
			nil,    // arguments
		)

		if err != nil {
			fmt.Println(err)
			return
		}

		pollDuration := time.Duration(0*100) * time.Millisecond
		qChannel := make(chan QueueChannelTask)

		qs.ActiveConsumers[q.Name] = Consumer{
			QueueName: q.Name,
			Channel:   qChannel,
		}

		go func() {
			for signal := range qChannel {
				switch signalType := signal.Type; signalType {
				case "slow_down_queue":
					pollDuration = time.Duration(signal.Ratio*100) * time.Millisecond
				}

			}
		}()

		for message := range messages {

			fmt.Println(pollDuration)
			logger.Debug(123, "queue_service", string(message.Body))
			// в этом месте просто перенаправялем сообщение куда нужно
			time.Sleep(pollDuration)
			message.Ack(false)
		}

	}(qs)
}

func (qs *QueueService) SlowDownQueue(queueId int, ratio int) {
	go func() {
		queueName := fmt.Sprintf("group_%d", queueId)
		if consumer, ok := qs.ActiveConsumers[queueName]; ok {

			consumer.Channel <- QueueChannelTask{ // struct channel для удобаства - чтобы не плодить кучу каналов под разные задачи
				Type:  "slow_down_queue",
				Ratio: ratio, // Общий набор полей для всех типов задач. Если значения нет - инициализируется пустыми значениями
			}

		} else {
			fmt.Println("NO CONSUMER")
		}
	}()
}

func (qs *QueueService) SpeedUpQueue() {

}

func (qs *QueueService) StopQueue() {

}

func (qs *QueueService) getQueueList() ([]Queue, error) {
	url := "http://guest:guest@localhost:15672/api/queues?name=group&use_regex=true&page=1&page_size=500"
	client := &http.Client{}
	req, _ := http.NewRequest("GET", url, nil)
	resp, _ := client.Do(req)

	var result QueueListApiResponse

	buf := new(strings.Builder)
	io.Copy(buf, resp.Body)

	json.Unmarshal([]byte(buf.String()), &result)

	return result.Items, nil
}
