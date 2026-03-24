package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mddfaisal/quash/proto"
	quash_proto "github.com/mddfaisal/quash/proto"
)

type kv struct {
	name     string
	key      string
	value    string
	duration int64
}

func Test_SetKV(t *testing.T) {

	for i := 0; i <= 10; i++ {
		t.Run("Test: "+fmt.Sprintf("Add %v", i), func(t *testing.T) {
			resp, err := SetKV(context.Background(), &proto.SetKVRequest{
				Key:             fmt.Sprintf("%v", i),
				Value:           fmt.Sprintf("%v", i),
				TimeOutDuration: 300,
			})
			if err != nil {
				t.Error(err)
			} else {
				t.Log(resp)
			}
		})
		time.Sleep(10 * time.Millisecond)
	}
}

func Test_GetKV(t *testing.T) {
	tests := []struct {
		name string
	}{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {})
	}
}

func Test_CreateQueue(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		req  *quash_proto.CreateQueueRequest
		resp *quash_proto.CreateQueueResponse
		err  error
	}{
		{
			name: "Create Queue",
			ctx:  context.Background(),
			req: &quash_proto.CreateQueueRequest{
				QueueName: "test_queue",
			},
			resp: &quash_proto.CreateQueueResponse{
				Response: "Queue created",
			},
			err: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := CreateQueue(tt.ctx, tt.req)
			if err != tt.err {
				t.Errorf("Expected error: %v, got: %v", tt.err, err)
			} else if resp.Response != tt.resp.Response {
				t.Errorf("Expected response: %v, got: %v", tt.resp.Response, resp.Response)
			} else {
				t.Logf("Test passed with response: %v", resp.Response)
			}
		})
	}
}

func Test_DeleteQueue(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		req  *quash_proto.DeleteQueueRequest
		resp *quash_proto.DeleteQueueResponse
		err  error
	}{
		{
			name: "Delete Queue",
			ctx:  context.Background(),
			req: &quash_proto.DeleteQueueRequest{
				QueueName: "test_queue",
			},
			resp: &quash_proto.DeleteQueueResponse{
				Response: "Queue deleted",
			},
			err: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			CreateQueue(context.Background(), &quash_proto.CreateQueueRequest{
				QueueName: tt.req.QueueName,
			})
			resp, err := DeleteQueue(tt.ctx, tt.req)
			if err != tt.err {
				t.Errorf("Expected error: %v, got: %v", tt.err, err)
			} else if resp.Response != tt.resp.Response {
				t.Errorf("Expected response: %v, got: %v", tt.resp.Response, resp.Response)
			} else {
				t.Logf("Test passed with response: %v", resp.Response)
			}
		})
	}
}
func Test_PushQueue(t *testing.T) {
	_, err := CreateQueue(context.Background(), &quash_proto.CreateQueueRequest{
		QueueName: "test_queue",
	})
	if err != nil {
		t.Errorf("Error creating queue: %v", err)
	}
	tests := []struct {
		name string
		ctx  context.Context
		req  *quash_proto.PushIntoQueueRequest
		resp *quash_proto.PushIntoQueueResponse
		err  error
	}{
		{
			name: "Push to Queue",
			ctx:  context.Background(),
			req: &quash_proto.PushIntoQueueRequest{
				QueueName: "test_queue",
				Value:     "Hello, World!",
			},
			resp: &quash_proto.PushIntoQueueResponse{
				Response: "Pushed",
			},
			err: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := PushQueue(tt.ctx, tt.req)
			if err != tt.err {
				t.Errorf("Expected error: %v, got: %v", tt.err, err)
			} else if resp.Response != tt.resp.Response {
				t.Errorf("Expected response: %v, got: %v", tt.resp.Response, resp.Response)
			} else {
				t.Logf("Test passed with response: %v", resp.Response)
			}
		})
	}
}

func Test_QueryQueueList(t *testing.T) {
	_, err := CreateQueue(context.Background(), &quash_proto.CreateQueueRequest{
		QueueName: "test_queue",
	})
	if err != nil {
		t.Errorf("Error creating queue: %v", err)
	}
	tests := []struct {
		name string
		ctx  context.Context
		req  *quash_proto.QueryQueueListRequest
		resp *quash_proto.QueueListResponse
		err  error
	}{
		{
			name: "Query Queue List",
			ctx:  context.Background(),
			req:  &quash_proto.QueryQueueListRequest{},
			resp: &quash_proto.QueueListResponse{QueueList: []string{"test_queue"}},
			err:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := QueryQueueList(tt.ctx, tt.req)
			if err != tt.err {
				t.Errorf("Expected error: %v, got: %v", tt.err, err)
			} else if len(resp.QueueList) != len(tt.resp.QueueList) {
				t.Errorf("Expected queue list length: %v, got: %v", len(tt.resp.QueueList), len(resp.QueueList))
			} else {
				for i, queue := range resp.QueueList {
					if queue != tt.resp.QueueList[i] {
						t.Errorf("Expected queue: %v, got: %v", tt.resp.QueueList[i], queue)
					}
				}
				t.Logf("Test passed with queue list: %v", resp.QueueList)
			}
		})
	}
}

func Test_PopQueue(t *testing.T) {
	_, err := CreateQueue(context.Background(), &quash_proto.CreateQueueRequest{
		QueueName: "test_queue",
	})
	if err != nil {
		t.Errorf("Error creating queue: %v", err)
	}
	_, err = PushQueue(context.Background(), &quash_proto.PushIntoQueueRequest{
		QueueName: "test_queue",
		Value:     "Hello, World!",
	})
	if err != nil {
		t.Errorf("Error pushing to queue: %v", err)
	}
	data := make(chan interface{})
	go func() {
		err := PopQueue("test_queue", data)
		if err != nil {
			t.Errorf("Error popping from queue: %v", err)
		}
	}()
	select {
	case resp := <-data:
		if resp != "Hello, World!" {
			t.Errorf("Expected response: %v, got: %v", "Hello, World!", resp)
		} else {
			t.Logf("Test passed with response: %v", resp)
		}
	case <-time.After(5 * time.Second):
		t.Error("Test timed out while waiting for response")
	}
}

func Test_QueryQueueMetric(t *testing.T) {
	_, err := CreateQueue(context.Background(), &quash_proto.CreateQueueRequest{
		QueueName: "test_queue",
	})
	if err != nil {
		t.Errorf("Error creating queue: %v", err)
	}
	data := make(chan *quash_proto.QueueMetricResponse)
	go func() {
		err := QueryQueueMetric(data)
		if err != nil {
			t.Errorf("Error querying queue metrics: %v", err)
		}
	}()
	select {
	case resp := <-data:
		if resp == nil {
			t.Error("Expected response, got nil")
		} else {
			t.Logf("Test passed with response: %v", resp.QueueMatric)
		}
		close(data)
	case <-time.After(5 * time.Second):
		t.Error("Test timed out while waiting for response")
	}
}
