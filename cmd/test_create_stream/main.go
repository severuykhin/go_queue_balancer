package main

import (
	"fmt"
	"log"
	"queue_balancer/internal/constants"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	// Создаем подключение
	// Подключение представляет собой простое соединение с натс-сервером.
	// Оно может отправлять и получать плозную нагрузку(сообщения/ивенты) в виде массива байт ([]byte)
	// Соединение безопасно для одновременного использования в нескольких горутинах (то есть оно потокобезопасное)
	// connect поддерживает вебсокеты кстати
	// To connect to a NATS Server's websocket port, use the `ws` or `wss` scheme, such as
	// `ws://localhost:8080`. Note that websocket schemes cannot be mixed with others (nats/tls).
	nc, _ := nats.Connect(nats.DefaultURL)

	// Какие вообще могут быть опции при подключении

	// NoEcho - false по умолчанию
	// NoEcho configures whether the server will echo back messages
	// that are sent on this connection if we also have matching subscriptions.
	// Note this is supported on servers >= version 1.2. Proto 1 or greater.

	// JetStreamContext - интерфейс для обмена сообщениями и упраления стримом
	// Принимает параметры типа JsOpt
	// JsOpt - PublishAsyncMaxPending - устанавливает максимальное количество асинхронных публикаций,
	//         которые будут обрабатывать одновременно
	//
	js, _ := nc.JetStream(nats.PublishAsyncMaxPending(256))

	// Вернет информацию о стриме. Если стрим не найден - вернет ошибку
	// В этом случае мы можем создать стрим из кода
	stream, err := js.StreamInfo(constants.StreamName)

	if err != nil {
		fmt.Println("err: ", err)
	}

	if stream == nil {
		log.Printf("creating stream %q and subjects %q", constants.StreamName, constants.StreamSubjects)

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
		stream, err = js.AddStream(&nats.StreamConfig{
			Name:       constants.StreamName,
			Subjects:   []string{constants.StreamSubjects},
			Storage:    nats.FileStorage,
			NoAck:      false,
			Retention:  nats.WorkQueuePolicy,
			Duplicates: time.Millisecond, // в целом принимете любой time.Duration
		})

		if err != nil {
			fmt.Println(err)
		}

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
		fmt.Printf("%+v\n", stream)
	}
}
