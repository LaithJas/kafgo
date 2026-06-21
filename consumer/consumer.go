// Package consumer defines the logic of a data consumer
// that is consuming data from a kafgo broker
package consumer

import (
	"context"
	"fmt"
	"io"

	uuid "github.com/google/uuid"
	proto "github.com/laithjas/kafgo/proto"
	grpc "google.golang.org/grpc"
	insecure "google.golang.org/grpc/credentials/insecure"
)

type Consumer struct {
	sc         proto.BrokerServiceClient
	consumerID uuid.UUID
}

// NewConsumer is the constructor to connect a consumer instance
// to a broker given the broker's ip
// retuns a Consumer instance and err if the connection fials
func NewConsumer(ip string) (*Consumer, error) {
	conn, err := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	svc := proto.NewBrokerServiceClient(conn)
	consumer := &Consumer{
		sc:         svc,
		consumerID: uuid.New(),
	}
	return consumer, nil
}

// Consume does multiple things. it subscribe to a topic, consumes data from that
// topic and let the user decide what to do with the data by prividing a handler func
// and then acks the messages
// returns err if anything fails
func (c *Consumer) Consume(ctx context.Context, topic string, handler func(cm *proto.ConsumeMsgsData) error) error {
	subTopicReq := &proto.SubscribeTopicRequest{
		Topic:      topic,
		ConsumerId: c.consumerID.String(),
	}
	sub, err := c.sc.SubscribeTopic(ctx, subTopicReq)
	if err != nil {
		return err
	}

	request := &proto.ConsumeMsgsRequest{Topic: sub.Topic}
	stream, err := c.sc.SetConsumerData(ctx, request)
	if err != nil {
		return err
	}
	for {
		ackMsgReq := &proto.AckMsgRequets{
			Topic:      sub.Topic,
			ConsumerId: c.consumerID.String(),
		}
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		err = handler(msg)
		if err != nil {
			return err
		}
		ackMsgReq.Id = msg.GetId()
		ackMsgRes, err := c.sc.AckMessage(ctx, ackMsgReq)
		if err != nil {
			return err
		}
		if !ackMsgRes.Success {
			return fmt.Errorf("message is not received")
		}
	}

	return nil
}
