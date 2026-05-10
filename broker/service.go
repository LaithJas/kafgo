// Package broker is used to crate a message queue broker and provde
// services to producers and consuumbers to use that broker
package broker

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/laithjas/kafgo/proto"
	"google.golang.org/grpc"
)

type broker struct {
	proto.UnimplementedBrokerServiceServer
	cache *cache
}

func NewBroker() *broker {
	return &broker{
		cache: newCache(),
	}
}

// starts a grpc server that will serve using a TCP conneection
func (b *broker) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		return fmt.Errorf("tcp server listener failed")
	}
	s := grpc.NewServer()
	proto.RegisterBrokerServiceServer(s, b)

	ch := make(chan error, 1)
	go func() {
		err2 := s.Serve(ln)
		if err2 != nil {
			ch <- err2
		}
	}()
	select {
	case <-ctx.Done():
		s.GracefulStop()
		return nil
	case v := <-ch:
		return fmt.Errorf("connection failed: %v", v)
	}
}

func (b *broker) CreateTopic(ctx context.Context, request *proto.CreateTopicRequest) (*proto.CreateTopicResponse, error) {
	response := proto.CreateTopicResponse{}
	err := b.cache.create(request.Topic)
	if err != nil {
		response.Success = false
		response.StatusCode = err.Error()
		return &response, err
	}
	response.Success = true
	response.StatusCode = "OK"
	response.Topic = request.Topic

	return &response, nil
}

// first open a service that cosumers can connect to send their data
// service should be concurrent and can handle multiple requests
//
// for the queue data structre we're going to use the container/list package
// for protecting the concuurent process we use sync.RWMutex
// to generate universally unique IDs we use google/uuid
// for Observability we'd need to track:
// - total message recieived
// - total message package
// - current queue size per topic
//
//need multiple topics that are mapped in a map.
// the cache sturct storage field would be a map where:
// - topic name is the key
// - value is the message list
