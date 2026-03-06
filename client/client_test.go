package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/mddfaisal/quash/proto"
)

type kv struct {
	name     string
	key      string
	value    string
	duration int64
}

func Test_SetKV(t *testing.T) {
	var tests []kv

	for i := 0; i <= 100000; i++ {
		tests = append(tests, kv{
			name:     fmt.Sprintf("%v", i),
			key:      fmt.Sprintf("%v", i),
			value:    fmt.Sprintf("%v", i),
			duration: 300,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := SetKV(context.Background(), &proto.SetKVRequest{
				Key:             tt.key,
				Value:           tt.value,
				TimeOutDuration: tt.duration,
			})
			if err != nil {
				t.Error(err)
			} else {
				t.Log(resp)
			}
		})
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
