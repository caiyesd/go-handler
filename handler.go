package handler

import (
	"fmt"
	"sync"
	"time"
)

type Handler interface {
	HandleMessage(v interface{})
}

type BatchHandler interface {
	HandleMessages(v []interface{})
}

type message struct {
	when    time.Time
	next    *message
	handler Handler
	value   interface{}
}

func newMessage(v interface{}) *message {
	return &message{
		when:    time.Now(),
		value:   v,
		handler: nil,
		next:    nil,
	}
}

type messageQueue struct {
	lock *sync.Mutex
	cond *sync.Cond
	head *message
}

func newMessageQueue() *messageQueue {
	lock := &sync.Mutex{}
	return &messageQueue{
		lock: lock,
		cond: sync.NewCond(lock),
	}
}

func (mq *messageQueue) enqueue(m *message) {
	mq.lock.Lock()
	defer mq.lock.Unlock()
	if mq.head == nil || mq.head.when.After(m.when) {
		m.next = mq.head
		mq.head = m
		mq.cond.Signal()
		return
	}
	p := mq.head
	for p.next != nil && p.next.when.Before(m.when) {
		p = p.next
	}
	m.next = p.next
	p.next = m
}

func (mq *messageQueue) dequeue() *message {
	mq.lock.Lock()
	defer mq.lock.Unlock()
	for {
		if mq.head == nil {
			mq.cond.Wait()
		} else {
			now := time.Now()
			if !now.Before(mq.head.when) {
				m := mq.head
				mq.head = m.next
				m.next = nil
				return m
			} else {
				tmp := mq.head.when.Sub(now)
				go func() {
					time.Sleep(tmp)
					mq.cond.Signal()
				}()
			}
			mq.cond.Wait()
		}
	}
}

func (mq *messageQueue) dequeue2() *message {
	mq.lock.Lock()
	defer mq.lock.Unlock()
	for {
		if mq.head == nil {
			mq.cond.Wait()
		} else {
			now := time.Now()
			if !now.Before(mq.head.when) {
				tail := mq.head
				for tail.next != nil && !now.Before(tail.next.when) {
					tail = tail.next
				}
				m := mq.head
				mq.head = tail.next
				tail.next = nil
				return m
			} else {
				tmp := mq.head.when.Sub(now)
				go func() {
					time.Sleep(tmp)
					mq.cond.Signal()
				}()
			}
			mq.cond.Wait()
		}
	}
}

type handlerRoutine struct {
	queue *messageQueue
	quit  chan struct{}
}

func NewHandlerRoutine() *handlerRoutine {
	return &handlerRoutine{
		quit: make(chan struct{}),
	}
}

func (r *handlerRoutine) Running() bool {
	return r.queue != nil
}

func (r *handlerRoutine) Start() error {
	if r.queue != nil {
		return fmt.Errorf("handler routine is already started")
	}
	r.queue = newMessageQueue()
	go func() {
		for {
			m := r.queue.dequeue()
			if m.handler == nil {
				r.quit <- struct{}{}
				r.queue = nil
				return
			}
			m.handler.HandleMessage(m.value)
		}
	}()
	return nil
}

func (r *handlerRoutine) Stop() error {
	if r.queue == nil {
		return fmt.Errorf("handler routine is already stopped")
	}
	m := newMessage(nil)
	m.handler = nil
	r.queue.enqueue(m)
	<-r.quit
	return nil
}

func (r *handlerRoutine) SendMessage(h Handler, v interface{}) error {
	return r.SendMessageAt(h, v, time.Now())
}

func (r *handlerRoutine) SendMessageAt(h Handler, v interface{}, when time.Time) error {
	if r.queue == nil {
		return fmt.Errorf("handler routine is not running")
	}
	m := newMessage(v)
	m.handler = h
	m.when = when
	r.queue.enqueue(m)
	return nil
}

func (r *handlerRoutine) SendMessageDelayed(h Handler, v interface{}, delay time.Duration) error {
	return r.SendMessageAt(h, v, time.Now().Add(delay))
}
