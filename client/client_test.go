package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mddfaisal/quash/proto"
)

type kv struct {
	name     string
	key      string
	value    string
	duration int64
}

func Test_SetKV(t *testing.T) {

	for i := 0; i <= 100000000000; i++ {
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
