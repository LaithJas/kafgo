// Package broker is used to crate a message queue broker and provde
// services to producers and consuumbers to use that broker
package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/laithjas/kafgo/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

type broker struct {
	proto.UnimplementedBrokerServiceServer
	cache  *cache
	notify chan struct{}
}

func NewBroker() *broker {
	return &broker{
		cache:  newCache(),
		notify: make(chan struct{}, 1),
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

// SetPublishData It's the method producers call to send messages to the broker
// It's a client streaming method — the producer streams multiple messages to the broker one by one
// The broker receives each message, stores it in the cache, and when the producer is
// done sends back a single response confirming success
func (b *broker) SetPublishData(stream grpc.ClientStreamingServer[proto.PublishMsgsData, proto.PublishMsgsResponse]) error {
	var tmpTopic string
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		msg := message{}
		msg.id = uuid.New()
		msg.data = req.Data
		msg.topic = req.Topic
		msg.receivedAt = time.Now()
		err = b.cache.store(msg.topic, &msg)
		if err != nil {
			return err
		}
		tmpTopic = msg.topic
		select {
		case b.notify <- struct{}{}:
		default:
		}
	}
	res := &proto.PublishMsgsResponse{Topic: tmpTopic, Success: true}
	err := stream.SendAndClose(res)
	if err != nil {
		return err
	}
	return nil
}

// SetConsumerData is the method called by consumers to consume data
// it's a server streaming method that takes one message and send a stream
// of messages to the topic that it received
func (b *broker) SetConsumerData(request *proto.ConsumeMsgsRequest, stream grpc.ServerStreamingServer[proto.ConsumeMsgsData]) error {
	ctx := stream.Context()
	for {
		select {
		case <-b.notify:
			response := &proto.ConsumeMsgsData{}
			msg, err := b.cache.retrieve(request.Topic)
			if err != nil {
				return err
			}
			response.Topic = request.Topic
			data, ok := msg.data.([]byte)
			if !ok {
				return fmt.Errorf("CAS004AM")
			}
			response.Data = data
			response.Id = msg.id.String()
			err = stream.Send(response)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}
