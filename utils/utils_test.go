package utils

import (
	"fmt"
	"testing"
)

func Test_SubscriptionID(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Run("Test: "+fmt.Sprintf("Generate Subscription ID %v", i), func(t *testing.T) {
			id := SubscriptionID()
			if len(id) != 32 {
				t.Errorf("Expected length of 32, got %v", len(id))
			}
			t.Logf("Generated Subscription ID: %v", id)
		})
	}
}
