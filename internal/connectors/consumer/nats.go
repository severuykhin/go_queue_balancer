package consumer

import (
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

type GroupMetaData struct {
	GroupId int
}

type GroupMetaDataMap map[int]GroupMetaData

type NatsConsumer struct {
	groupMetaDataMap GroupMetaDataMap
	natsConnection   *nats.Conn
	natsJetStream    nats.JetStreamContext
}

func NewNatsConsumer() *NatsConsumer {
	nc, err := nats.Connect(nats.DefaultURL)

	if err != nil {
		log.Fatal(err)
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(nc.Status())
	fmt.Println("=============")
	fmt.Printf("%+v\n", nc.Stats())
	fmt.Println("=============")
	fmt.Println(nc.Servers())

	return &NatsConsumer{
		natsConnection:   nc,
		groupMetaDataMap: GroupMetaDataMap{},
		natsJetStream:    js,
	}
}

func (nc *NatsConsumer) ConsumeMainStream() {

	// Simple Async Ephemeral Consumer
	subs, err := nc.natsJetStream.Subscribe("BOTS.*", func(msg *nats.Msg) {
		fmt.Println(string(msg.Data))
		msg.Ack()
	}, nats.DeliverSubject("*"))

	if err != nil {
		log.Fatal("ddd", err)
	}

	fmt.Printf("%+v\n", subs)

	// for {
	// 	m, err := subs.NextMsg(1000)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	fmt.Printf("%+v\n", m)

	// 	select {
	// 	case <-time.After(time.Second):
	// 		//
	// 	}
	// }

}

func (nc *NatsConsumer) Close() {

}
