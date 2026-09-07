package structs

import (
	"time"
)

type KVValue struct {
	Value    string
	TimeOut  time.Duration
	InitTime time.Time
}

type Topic string
type SubscriptionID string

type SrvData struct {
	KVMapLength int64                                    `json:"kv_map_length"`
	Brokers     map[Topic]map[SubscriptionID]chan string `json:"brokers"`
}

type AdminResponse struct {
	QueueMetric SrvData `json:"queue_metric"`
}
