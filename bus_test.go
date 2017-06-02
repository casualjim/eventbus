package eventbus

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegisterHandlers(t *testing.T) {
	bus := New(nil)
	defer bus.Close()
	assert.Equal(t, 0, bus.Len())
	bus.Subscribe(NOOPHandler)
	assert.Equal(t, 1, bus.Len())
}

func TestUnregisterHandlers(t *testing.T) {
	bus := New(nil)
	defer bus.Close()

	assert.Equal(t, 0, bus.Len())
	bus.Subscribe(NOOPHandler, NOOPHandler, NOOPHandler)
	assert.Equal(t, 3, bus.Len())
	bus.Unsubscribe(NOOPHandler)
	assert.Equal(t, 2, bus.Len())
}

func TestPublish_ToAllListeners(t *testing.T) {
	bus := New(nil)
	defer bus.Close()

	evts := make([]Event, 3)
	wg := new(sync.WaitGroup)
	wg.Add(3)
	lock := new(sync.Mutex)
	var seen int
	listener1 := Handler(func(evt Event) error {
		lock.Lock()
		evts[0] = evt
		seen++
		lock.Unlock()

		wg.Done()
		return nil
	})
	listener2 := Handler(func(evt Event) error {
		lock.Lock()
		evts[1] = evt
		seen++
		lock.Unlock()

		wg.Done()
		return nil
	})
	listener3 := Handler(func(evt Event) error {
		lock.Lock()
		evts[2] = evt
		seen++
		lock.Unlock()

		wg.Done()
		return nil
	})

	bus.Subscribe(listener1, listener2, listener3)

	evt := Event{Name: "the event"}
	bus.Publish(evt)
	wg.Wait()
	assert.EqualValues(t, evt, evts[0])
	assert.EqualValues(t, evt, evts[1])
	assert.EqualValues(t, evt, evts[2])
}
