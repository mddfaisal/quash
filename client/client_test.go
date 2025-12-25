package client

import (
	"context"
	"testing"

	"github.com/mddfaisal/quash/proto"
)

func Test_SetKV(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    string
		duration int64
	}{
		{
			name:     "test_1",
			key:      "one",
			value:    "1",
			duration: 300,
		},
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
