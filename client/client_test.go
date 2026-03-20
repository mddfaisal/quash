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
func Test_PushQueue(t *testing.T)      {}
func Test_QueryQueueList(t *testing.T) {}
