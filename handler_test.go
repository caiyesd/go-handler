package handler

import (
	"log"
	"testing"
	"time"
)

type H struct{}

func (h *H) HandleMessage(v interface{}) {
	log.Println(v.(string))
}

type BH struct{}

func (h *BH) HandleMessages(v []interface{}) {
	log.Println(v...)
}

func TestHandlerRoutine(t *testing.T) {
	routine := NewHandlerRoutine()
	routine.Start()

	routine.SendMessage(new(H), "Hello1")
	routine.SendMessageAt(new(H), "Hello2", time.Now().Add(time.Second*5))
	routine.SendMessageDelayed(new(H), "Hello3", time.Second*1)
	routine.SendMessageAt(new(H), "Hello4", time.Now().Add(time.Second*2))
	routine.SendMessageDelayed(new(H), "Hello5", time.Second*3)
	routine.SendMessage(new(H), "Hello6")
	time.Sleep(time.Second * 8)
	routine.Stop()
}

func TestBatchHandlerRoutine(t *testing.T) {
	routine := NewBatchHandlerRoutine(new(BH))
	routine.Start()

	routine.SendMessage("Hello1")
	routine.SendMessageAt("Hello2", time.Now().Add(time.Second*5))
	routine.SendMessageDelayed("Hello3", time.Second*1)
	routine.SendMessageAt("Hello4", time.Now().Add(time.Second*2))
	routine.SendMessageDelayed("Hello5", time.Second*3)
	routine.SendMessage("Hello6")
	routine.SendMessageAt("Hello7", time.Now().Add(time.Second*5))
	routine.SendMessageDelayed("Hello8", time.Second*1)
	routine.SendMessageAt("Hello9", time.Now().Add(time.Second*2))
	routine.SendMessageDelayed("Hello10", time.Second*3)
	routine.SendMessage("Hello11")
	time.Sleep(time.Second * 8)
	routine.Stop()
}
