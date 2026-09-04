package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/mddfaisal/quash/config"
	quash_proto "github.com/mddfaisal/quash/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var lock = &sync.Mutex{}

func SetKV(ctx context.Context, req *quash_proto.SetKVRequest) (*quash_proto.SetKVResponse, error) {
	lock.Lock()
	conn, err := grpc.Dial(config.QuashDb, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return nil, err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	resp, err := client.SetKV(ctx, req)
	return resp, err
}

func GetKV(ctx context.Context, req *quash_proto.GetKVRequest) (*quash_proto.GetKVResponse, error) {
	lock.Lock()
	conn, err := grpc.Dial(config.QuashDb, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return nil, err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	resp, err := client.GetKV(ctx, req)
	return resp, err
}

func DeleteKV(ctx context.Context, req *quash_proto.DeleteKVRequest) (*quash_proto.DeleteKVResponse, error) {
	lock.Lock()
	conn, err := grpc.Dial(config.QuashDb, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return nil, err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	resp, err := client.DeleteKV(ctx, req)
	return resp, err
}

func CreateTopic(ctx context.Context, req *quash_proto.CreateTopicRequest) (*quash_proto.CreateTopicResponse, error) {
	lock.Lock()
	conn, err := grpc.Dial(config.QuashDb, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return nil, err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	resp, err := client.CreateTopic(ctx, req)
	return resp, err
}

func DeleteTopic(ctx context.Context, req *quash_proto.RemoveTopicRequest) (*quash_proto.RemoveTopicResponse, error) {
	lock.Lock()
	conn, err := grpc.Dial(config.QuashDb, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return nil, err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	resp, err := client.RemoveTopic(ctx, req)
	return resp, err
}

func PushQueue(ctx context.Context, req *quash_proto.PushIntoQueueRequest) (*quash_proto.PushIntoQueueResponse, error) {
	lock.Lock()
	conn, err := grpc.Dial(config.QuashDb, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return nil, err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	resp, err := client.PushQueue(ctx, req)
	return resp, err
}

func PopQueue(queueName string, data chan interface{}) error {
	lock.Lock()
	conn, err := grpc.Dial(config.QuashDb, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	stream, err := client.PopQueue(context.Background(), &quash_proto.PopFromQueueRequest{
		QueueName: queueName,
	})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		data <- resp.Response
	}
}

func QueryTopicList(ctx context.Context, req *quash_proto.QueryTopicListRequest) (*quash_proto.QueueTopicListResponse, error) {
	lock.Lock()
	conn, err := grpc.Dial(config.QuashDb, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return nil, err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	resp, err := client.QueryTopicList(ctx, req)
	return resp, err
}

func QueryTopicMetric(data chan *quash_proto.QueueTopicMetricResponse) error {
	lock.Lock()
	conn, err := grpc.Dial(config.QuashDb, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	stream, err := client.QueryTopicMetric(context.Background(), &quash_proto.QueryTopicMetricRequest{})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		data <- resp
		fmt.Printf("Queue Metrics: %v\n", resp.QueueMetric)
	}
}
