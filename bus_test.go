package eventbus

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRegisterHandlers(t *testing.T) {
	bus := New(nil)
	defer bus.Close()
	assert.Equal(t, 0, bus.Len())
	bus.Add(make(chan Event))
	assert.Equal(t, 1, bus.Len())
}

func TestUnregisterHandlers(t *testing.T) {
	bus := New(nil)
	defer bus.Close()
	h := make(chan Event)
	h2 := make(chan Event)
	h3 := make(chan Event)
	assert.Equal(t, 0, bus.Len())
	bus.Add(h, h2, h3)
	assert.Equal(t, 3, bus.Len())
	bus.Remove(h2)
	assert.Equal(t, 2, bus.Len())
}

func TestPublish_ToAllListeners(t *testing.T) {
	bus := New(nil)
	defer bus.Close()

	listener1 := make(chan Event)
	listener2 := make(chan Event)
	listener3 := make(chan Event)
	bus.Add(listener1, listener2, listener3)

	evts := make([]Event, 3)
	wg := new(sync.WaitGroup)
	wg.Add(3)
	go func() {
		var seen int
		for seen < 3 {
			select {
			case e1 := <-listener1:
				evts[0] = e1
				wg.Done()
				seen++
			case e2 := <-listener2:
				evts[1] = e2
				wg.Done()
				seen++
			case e3 := <-listener3:
				evts[2] = e3
				wg.Done()
				seen++
			}
		}

	}()

	evt := Event{Name: "the event"}
	bus.Broadcaster() <- evt
	wg.Wait()
	assert.EqualValues(t, evt, evts[0])
	assert.EqualValues(t, evt, evts[1])
	assert.EqualValues(t, evt, evts[2])
}

func TestPublish_DropFailedListeners(t *testing.T) {
	bus := New(nil)
	defer bus.Close()
	listener1 := make(chan Event)
	listener2 := make(chan Event)
	listener3 := make(chan Event)
	bus.Add(listener1, listener2, listener3)
	latch := make(chan bool)
	go func() {
		time.Sleep(1 * time.Second)
		latch <- true
	}()

	evt := Event{Name: "another event"}
	bus.Broadcaster() <- evt
	<-latch

	assert.Equal(t, 0, bus.Len())
}
