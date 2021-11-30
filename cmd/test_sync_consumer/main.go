package main

import "github.com/nats-io/nats.go"

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)
	js, _ := nc.JetStream(nats.PublishAsyncMaxPending(256))

	// простой синхронный durable консьюмер
	// Если приложение желает возобновить прием сообщений с того места, где оно было ранее остановлено,
	// ему необходимо создать постоянную подписку.
	// Он делает это, предоставляя постоянное имя, которое сочетается с идентификатором клиента,
	// предоставленным, когда клиент создал свое соединение.
	// Затем сервер поддерживает состояние этой подписки даже после закрытия клиентского соединения.
	sub, err := js.SubscribeSync("ORDERS.*", nats.Durable("MONITOR"), nats.MaxDeliver(3))
	m, err := sub.NextMsg(timeout)
}
