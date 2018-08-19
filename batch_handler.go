package handler

import (
	"fmt"
	"time"
)

type emptyHandler struct{}

func (h *emptyHandler) HandleMessage(v interface{}) {}

var (
	gEmptyHandler = new(emptyHandler)
)

type batchHandlerRoutine struct {
	queue   *messageQueue
	quit    chan struct{}
	handler BatchHandler
}

func NewBatchHandlerRoutine(handler BatchHandler) *batchHandlerRoutine {
	return &batchHandlerRoutine{
		quit:    make(chan struct{}),
		handler: handler,
	}
}

func (r *batchHandlerRoutine) Running() bool {
	return r.queue != nil
}

func (r *batchHandlerRoutine) Start() error {
	if r.queue != nil {
		return fmt.Errorf("batch handler routine is already started")
	}
	r.queue = newMessageQueue()
	go func() {
		for {
			m := r.queue.dequeue2()
			if m.handler == nil {
				r.quit <- struct{}{}
				r.queue = nil
				return
			}
			v := make([]interface{}, 0)
			for m != nil {
				v = append(v, m.value)
				m = m.next
			}
			r.handler.HandleMessages(v)
		}
	}()
	return nil
}

func (r *batchHandlerRoutine) Stop() error {
	if r.queue == nil {
		return fmt.Errorf("handler routine is already stopped")
	}
	m := newMessage(nil)
	m.handler = nil
	r.queue.enqueue(m)
	<-r.quit
	return nil
}

func (r *batchHandlerRoutine) SendMessage(v interface{}) error {
	return r.SendMessageAt(v, time.Now())
}

func (r *batchHandlerRoutine) SendMessageAt(v interface{}, when time.Time) error {
	if r.queue == nil {
		return fmt.Errorf("handler routine is not running")
	}
	m := newMessage(v)
	m.handler = gEmptyHandler
	m.when = when
	r.queue.enqueue(m)
	return nil
}

func (r *batchHandlerRoutine) SendMessageDelayed(v interface{}, delay time.Duration) error {
	return r.SendMessageAt(v, time.Now().Add(delay))
}
