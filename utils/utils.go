package utils

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

const (
	QuashDb              = "0.0.0.0:6300"
	QuashTelemetry       = ":6301"
	SubscriberBufferSize = 64
)

func SubscriptionID() string {
	b := make([]byte, 16)
	rand.Read(b)
	now := []byte(time.Now().Format("20060102150405"))
	return hex.EncodeToString(append(b, now...))
}
