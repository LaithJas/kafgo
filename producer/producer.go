// Package producer pacakge contains the def and logic of how a producer operatis
// it should be used by producers who are sending data to the kafgo broker
package producer

import (
	"context"

	uuid "github.com/google/uuid"
	proto "github.com/laithjas/kafgo/proto"
	grpc "google.golang.org/grpc"
	insecure "google.golang.org/grpc/credentials/insecure"
)

type Producer struct {
	sc         proto.BrokerServiceClient
	producerID uuid.UUID
}

// NewProducer is the constructor to connect a producer instance to a broker
// given the brokers IP
// returns a producer instance and err if the connection fails
func NewProducer(ip string) (*Producer, error) {
	// TODO: only for testing. need to change the credentials to use TLS for encryption
	conn, err := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	svc := proto.NewBrokerServiceClient(conn)
	producer := &Producer{
		sc:         svc,
		producerID: uuid.New(),
	}

	return producer, nil
}

// Publish is used by producers to send message streams to the broekr
// it takes a topic name and a slice of data in bytes to send to broker
// returns a message response from the broker and an error if something goes wrong
func (p *Producer) Publish(ctx context.Context, topic string, data []byte) (*proto.PublishMsgsResponse, error) {
	stream, err := p.sc.SetPublishData(ctx)
	if err != nil {
		return nil, err
	}
	request := &proto.PublishMsgsData{}
	request.Topic = topic
	request.Data = data
	err = stream.Send(request)
	if err != nil {
		return nil, err
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (p *Producer) CreateTopic(ctx context.Context, topic string) (*proto.CreateTopicResponse, error) {
	request := &proto.CreateTopicRequest{}
	request.Topic = topic
	response, err := p.sc.CreateTopic(ctx, request)
	if err != nil {
		return nil, err
	}
	return response, nil
}
