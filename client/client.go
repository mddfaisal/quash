package client

import (
	"context"
	"fmt"
	"sync"

	quash_proto "github.com/mddfaisal/quash/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const addr = "0.0.0.0:6300"

var lock = &sync.Mutex{}

func SetKV(ctx context.Context, req *quash_proto.SetKVRequest) (*quash_proto.SetKVResponse, error) {
	lock.Lock()
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func CreateQueue(ctx context.Context, req *quash_proto.CreateQueueRequest) (*quash_proto.CreateQueueResponse, error) {
	lock.Lock()
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return nil, err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	resp, err := client.CreateQueue(ctx, req)
	return resp, err
}

func DeleteQueue(ctx context.Context, req *quash_proto.DeleteQueueRequest) (*quash_proto.DeleteQueueResponse, error) {
	lock.Lock()
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return nil, err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	resp, err := client.DeleteQueue(ctx, req)
	return resp, err
}

func PushQueue(ctx context.Context, req *quash_proto.PushIntoQueueRequest) (*quash_proto.PushIntoQueueResponse, error) {
	lock.Lock()
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func QueryQueueList(ctx context.Context, req *quash_proto.QueryQueueListRequest) (*quash_proto.QueueListResponse, error) {
	lock.Lock()
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return nil, err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	resp, err := client.QueryQueueList(ctx, req)
	return resp, err
}

func QueryQueueMetric(data chan *quash_proto.QueueMetricResponse) error {
	lock.Lock()
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer func() {
		conn.Close()
		lock.Unlock()
	}()
	if err != nil {
		return err
	}
	client := quash_proto.NewQuashServiceClient(conn)
	stream, err := client.QueryQueueMetric(context.Background(), &quash_proto.QueryQueueMetricRequest{})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		data <- resp
		fmt.Printf("Queue Metrics: %v\n", resp.QueueMatric)
	}
}
