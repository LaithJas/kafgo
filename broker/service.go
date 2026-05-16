// Package broker is used to crate a message queue broker and provde
// services to producers and consuumbers to use that broker
package broker

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/google/uuid"
	"github.com/laithjas/kafgo/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
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

// Start starts a grpc server that will serve using a TCP conneection
// it will return an error if tcp socket is not created or if
// connection faild
// it creates a tcp socket that Listen for all incomming connections
// it creats a grpc server, and register that server to the socket
// it will handle all incomming connections using go routines and channels
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

// CreateTopic uses the protobuff interfaces to create a new topic
// in the broker instance
// it returns a response struct and error if create method faild
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

// SubscribeTopic uses the protobuff interfaces to subsribe to a topic
// in the broker instance
// it returns a response struct and error if subsribe method faild
// or if context doesn't contain data
func (b *broker) SubscribeTopic(ctx context.Context, request *proto.SubscribeTopicRequest) (*proto.SubscribeTopicResponse, error) {
	response := proto.SubscribeTopicResponse{}
	p, ok := peer.FromContext(ctx)
	if !ok {
		response.StatusCode = "CAS007AM"
		return &response, errors.New(response.StatusCode)
	}
	err := b.cache.subscribe(request.Topic, p.Addr.String())
	if err != nil {
		response.StatusCode = "CAS001AN"
		return &response, err
	}
	response.StatusCode = "SUC001AN_" + p.Addr.String()
	response.Topic = request.Topic

	return &response, nil
}

// AckMessage acknowledge the message that are consumed by consumers
// it will return a AckMsgResponse struct and an error if the
// message is not acked
func (b *broker) AckMessage(ctx context.Context, request *proto.AckMsgRequets) (*proto.AckMsgResponse, error) {
	response := proto.AckMsgResponse{}
	parsed, err := uuid.Parse(request.Id)
	if err != nil {
		response.Success = false
		return &response, err
	}
	err = b.cache.ack(request.Topic, parsed)
	if err != nil {
		response.Success = false
		return &response, err
	}
	response.Success = true
	response.Id = request.Id

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
