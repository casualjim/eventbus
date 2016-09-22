package eventbus

import (
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/rcrowley/go-metrics"
)

// Event you can subscribe to
type Event struct {
	Name string
	At   time.Time
	Args interface{}
}

// EventBus does fanout to registered channels
type EventBus interface {
	Close() error
	Broadcaster() chan<- Event
	Add(handlers ...chan<- Event)
	Remove(handlers ...chan<- Event)
	Len() int
}

type defaultEventBus struct {
	sync.Mutex

	channel  chan Event
	handlers []chan<- Event
	closing  chan chan struct{}
	log      logrus.FieldLogger
}

// New event bus with specified logger
func New(log logrus.FieldLogger) EventBus { return NewWithTimeout(log, 50*time.Millisecond) }

// NewWithTimeout creates a new event bus with a custom timeout for message delivery
func NewWithTimeout(log logrus.FieldLogger, timeout time.Duration) EventBus {
	if log == nil {
		log = logrus.New().WithFields(nil)
	}
	e := &defaultEventBus{
		closing: make(chan chan struct{}),
		channel: make(chan Event),
		log:     log,
	}
	go e.dispatcherLoop(timeout)
	return e
}

func (e *defaultEventBus) dispatcherLoop(timeout time.Duration) {
	for {
		select {
		case evt := <-e.channel:
			timer := metrics.GetOrRegisterTimer("events.notify", metrics.DefaultRegistry)
			go timer.Time(func() {
				var wg sync.WaitGroup
				wg.Add(len(e.handlers))
				for _, handler := range e.handlers {
					go e.dispatchEventWithTimeout(handler, timeout, evt, &wg)
				}
				wg.Wait()
			})
		case closed := <-e.closing:
			close(e.channel)
			for _, handler := range e.handlers {
				close(handler)
			}
			e.handlers = nil
			closed <- struct{}{}
			return
		}
	}
}

func (e *defaultEventBus) dispatchEventWithTimeout(channel chan<- Event, timeout time.Duration, event Event, wg *sync.WaitGroup) {
	timer := time.NewTimer(timeout)
	select {
	case channel <- event:
		timer.Stop()
	case <-timer.C:
		e.log.Warnf("Failed to send event %+v to listener within %v", event, timeout)
		e.Remove(channel)
	}
	wg.Done()
}

func (e *defaultEventBus) Broadcaster() chan<- Event {
	return e.channel
}

func (e *defaultEventBus) Add(handler ...chan<- Event) {
	e.Lock()
	defer e.Unlock()
	e.handlers = append(e.handlers, handler...)
}

func (e *defaultEventBus) Remove(handler ...chan<- Event) {
	e.Lock()
	defer e.Unlock()
	for _, h := range handler {
		for i, handler := range e.handlers {
			if h == handler {
				e.handlers = append(e.handlers[:i], e.handlers[i+1:]...)
				break
			}
		}
	}
}

func (e *defaultEventBus) Close() error {
	ch := make(chan struct{})
	e.closing <- ch
	<-ch
	close(e.closing)

	return nil
}

func (e *defaultEventBus) Len() int {
	return len(e.handlers)
}
