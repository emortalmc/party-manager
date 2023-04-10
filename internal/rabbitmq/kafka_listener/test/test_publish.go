package main

import (
	"context"
	"github.com/emortalmc/proto-specs/gen/go/message/common"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"time"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	msg := common.PlayerConnectMessage{
		PlayerUsername: "notmattw",
		PlayerId:       "aceb326f-da15-45bc-bf2f-11940c21780c",
		ServerId:       "velocity-sucks",
	}

	bytes, err := proto.Marshal(&msg)
	if err != nil {
		panic(err)
	}

	err = ch.PublishWithContext(context.Background(), "", "party-manager", false, false, amqp.Publishing{
		ContentType: "application/x-protobuf",
		Timestamp:   time.Now(),
		Type:        string(msg.ProtoReflect().Descriptor().FullName()),
		Body:        bytes,
	})
	if err != nil {
		panic(err)
	}
}
