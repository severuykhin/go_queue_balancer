package main

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	streamName     = "SOME_STREAM"
	streamSubjects = "SOME_STREAM.*"
)

func main() {

	nc, _ := nats.Connect(nats.DefaultURL)
	js, _ := nc.JetStream(nats.PublishAsyncMaxPending(256))

	i := 1
	for {
		msg := fmt.Sprintf("hello %d", i)
		// msg := "hello"
		subj := fmt.Sprintf("%s.group_1", streamName)

		// Процесс публикации
		// subj - строка
		// data - массив байт
		// opts - опции публикации - их не так много
		// из опций интересно
		// MsgId - sets the message ID used for de-duplication. Работает. Нужно определить что использовать в качестве id
		//
		// Если указать просто имя сабжетка без префикса стрима, то натс попытается опубликовать сообщение следую политике ретраев
		// И если все таки не сможет, то вернт nil
		// Если уаказать просто имя стрима - будет примерно то же самое
		// можно указать wildcard
		pubRes, err := js.Publish(subj, []byte(msg))

		// В целом логично, что все настройки публикации задаются на уровне создания стрима
		// А при публикации указывается минимум настрек

		// под капотом выполняется
		// m := nats.NewMsg(subj)
		// pubRes, err := js.PublishMsg(m)

		fmt.Println(msg)

		if err != nil {
			fmt.Println(err)
		}
		//  {
		//	  Stream:SOME_STREAM
		//	  Sequence:8
		//	  Duplicate:false
		//	  Domain:
		//  }
		fmt.Printf("res: %+v\n", pubRes)

		time.Sleep(time.Second)
		i += 1

	}

}
