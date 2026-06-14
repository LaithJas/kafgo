package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/laithjas/kafgo/broker"
	"github.com/laithjas/kafgo/consumer"
	"github.com/laithjas/kafgo/producer"
	"github.com/laithjas/kafgo/proto"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	myBroker := broker.NewBroker()
	c := make(chan os.Signal, 1)
	fmt.Print("broker is starting... ")
	go func() {
		err := myBroker.Start(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}()
	go func() {
		<-c
		cancel()
	}()
	fmt.Print("broker is running... ")

	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	// wait for broker
	time.Sleep(2 * time.Second)

	p, err := producer.NewProducer(":8080")
	if err != nil {
		fmt.Println(err)
	}
	p.CreateTopic(ctx, "laith")
	data := []byte("DATA IS HERE")
	p.Publish(ctx, "laith", data)
	// wait for producer
	time.Sleep(1 * time.Second)

	conce, err := consumer.NewConsumer(":8080")
	if err != nil {
		fmt.Println(err)
	}
	tmpfunc := func(d *proto.ConsumeMsgsData) error {
		fmt.Println("data from consumer", string(d.Data), d.Id, d.Topic)
		return nil
	}
	go conce.Consume(ctx, "laith", tmpfunc)
	// wait for consumer
	time.Sleep(1 * time.Second)

	<-ctx.Done()
}
