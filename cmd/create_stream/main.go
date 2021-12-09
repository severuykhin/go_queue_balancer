package main

import (
	"fmt"
	"log"
	"queue_balancer/internal/constants"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {

	nc, _ := nats.Connect(nats.DefaultURL)
	js, _ := nc.JetStream(nats.PublishAsyncMaxPending(256))

	deleteStreamIfExists(js, constants.MAIN_STREAM_NAME)
	deleteStreamIfExists(js, constants.ASSISTANT_STREAM_NAME)

	log.Printf("creating stream %q and subjects %q", constants.MAIN_STREAM_NAME, constants.MAIN_STREAM_SUBJECTS)

	// Создание стрима
	// Во-первых задается конфиграция стрима, во-вторых некие опции
	// Описание параметров конфигурации https://docs.nats.io/nats-concepts/jetstream/streams
	// из интересного
	// NoAck - Отключает подтверждающие сообщения, полученные Stream
	// Retention - сколько хранить сообщение и по какому приницпу удалять
	//    - LimitsPolicy - означает, что сообщения сохраняются до тех пор, пока не будет достигнут заданный лимит на хранение
	//	  - InterestPolicy - означает, что сообщение может быть удалено только в том случае, если все подписанные наблюдатели подтвердять получение
	//    - WorkQueuePolicy - означает, что сообщение может быть удалено, если хотя бы один подписчик подтвердли сообщение
	// Storage - тип хранения (File, Memory)
	// Replicas - фактор репликации
	// Duplicates - окно, в котором можно отслеживать повторяющиеся сообщения, выраженное в наносекундах

	streamS, err := js.AddStream(&nats.StreamConfig{
		Name:       constants.MAIN_STREAM_NAME,
		Subjects:   []string{constants.MAIN_STREAM_SUBJECTS},
		Storage:    nats.FileStorage,
		Retention:  nats.WorkQueuePolicy,
		Duplicates: time.Millisecond, // в целом принимете любой time.Duration
	})

	if err != nil {
		fmt.Println(err)
	}

	// streamA, err := js.AddStream(&nats.StreamConfig{
	// 	Name: constants.ASSISTANT_STREAM_NAME,
	// 	// Subjects:  []string{constants.ASSISTANT_STREAM_SUBJECTS},
	// 	Storage:   nats.FileStorage,
	// 	Retention: nats.WorkQueuePolicy,
	// 	Mirror: &nats.StreamSource{
	// 		Name: constants.MAIN_STREAM_NAME,
	// 	},
	// })

	// if err != nil {
	// 	fmt.Println(err)
	// }

	// Получим структуру с опциями
	// &{Config:{
	//	Name:SOME_STREAM
	//	Description:
	//	Subjects:[SOME_STREAM.*]
	//	Retention:WorkQueue
	//	MaxConsumers:-1  - неограниченное количество
	//	MaxMsgs:-1 - неограниченное количество
	//	MaxBytes:-1  - неограниченное количество
	//	Discard:DiscardOld
	//	MaxAge:0s
	//	MaxMsgsPerSubject:0
	//	MaxMsgSize:-1
	//	Storage:File
	//	Replicas:1
	//	NoAck:false
	//	Template:
	//	Duplicates:1m0s
	//	Placement:<nil>
	//	Mirror:<nil>
	//	Sources:[]
	//	Sealed:false
	//	DenyDelete:false
	//	DenyPurge:false
	//	AllowRollup:false}
	//	Created:2021-11-28 09:38:39.960459126 +0000 UTC
	//	State:{
	//		Msgs:0
	//		Bytes:0
	//		FirstSeq:0
	//		FirstTime:0001-01-01 00:00:00 +0000 UTC
	//		LastSeq:0
	//		LastTime:0001-01-01 00:00:00 +0000 UTC Consumers:0}
	//		Cluster:0x14000216040 Mirror:<nil> Sources:[]}
	fmt.Printf("%+v\n", streamS)
	// fmt.Printf("%+v\n", streamA)
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
