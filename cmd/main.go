package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/laithjas/kafgo/broker"
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
	<-ctx.Done()
}
