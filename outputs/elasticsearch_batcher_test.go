// SPDX-License-Identifier: MIT OR Apache-2.0

package outputs

import (
	"fmt"
	"testing"
	"time"
)

func TestElasticsearchBatcher(t *testing.T) {
	batcher := NewElasticseachBatcher()
	batcher.callback = func(payload []byte) {
		fmt.Printf("[%v] Got Data: %v\n", time.Now(), string(payload))
	}

	for i := 0; i < 11; i++ {
		batcher.Push("test", eSPayload{})
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)
}
