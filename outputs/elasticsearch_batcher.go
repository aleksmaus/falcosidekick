// SPDX-License-Identifier: MIT OR Apache-2.0

package outputs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	defaultBatchSize     = 1000
	defaultFlushInterval = time.Second
)

var ErrIndexName = errors.New("invalid index name")

type ElasticsearchBatcherCallback func(payload []byte)

type ElasticsearchBatcher struct {
	batchSize     int
	flushInterval time.Duration

	callback ElasticsearchBatcherCallback
	mx       sync.Mutex

	pending                bytes.Buffer
	pendingCount           int
	lastScheduledFlushTime time.Time
}

func NewElasticseachBatcher() *ElasticsearchBatcher {
	b := &ElasticsearchBatcher{
		batchSize:     defaultBatchSize,
		flushInterval: defaultFlushInterval,
	}

	// TODO: make it configurable

	return b
}

func (b *ElasticsearchBatcher) Push(index string, payload eSPayload) error {
	b.mx.Lock()
	defer b.mx.Unlock()

	err := b.appendPending(index, payload)
	if err != nil {
		return err
	}
	b.pendingCount++

	if b.pendingCount == b.batchSize {
		b.lastScheduledFlushTime = time.Now()
		go b.flush()
	}

	if b.pendingCount == 1 {
		ts := time.Now()
		b.lastScheduledFlushTime = ts
		go b.scheduleFlushInterval(ts)
	}

	return nil
}

func (b *ElasticsearchBatcher) appendPending(index string, payload eSPayload) error {
	if strings.Contains(index, `"`) {
		return fmt.Errorf("%s: %w", index, ErrIndexName)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	_, _ = b.pending.WriteString(`{"create":{`)
	_, _ = b.pending.WriteString(`"_index":"`)
	_, _ = b.pending.WriteString(index)
	_, _ = b.pending.WriteString("\"}}\n")

	_, _ = b.pending.Write(body)
	_, _ = b.pending.WriteRune('\n')

	return nil
}

func (b *ElasticsearchBatcher) flush() {
	b.mx.Lock()
	defer b.mx.Unlock()
	if b.pendingCount == 0 {
		return
	}

	fmt.Println("FLUSH")

	b.pendingCount = 0
	payload := b.pending.Bytes()
	b.pending = bytes.Buffer{}

	if b.callback != nil {
		b.callback(payload)
	}
}

func (b *ElasticsearchBatcher) scheduleFlushInterval(ts time.Time) {
	<-time.After(b.flushInterval)
	if ts.Equal(b.lastScheduledFlushTime) {
		b.flush()
	}
}
