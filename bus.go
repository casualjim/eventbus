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
	lock *sync.RWMutex

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
		channel: make(chan Event, 100),
		log:     log,
		lock:    new(sync.RWMutex),
	}
	go e.dispatcherLoop(timeout)
	return e
}

func (e *defaultEventBus) dispatcherLoop(timeout time.Duration) {
	totWait := new(sync.WaitGroup)
	for {
		select {
		case evt := <-e.channel:
			e.log.Debugf("Got event %+v in channel\n", evt)
			timer := metrics.GetOrRegisterTimer("events.notify", metrics.DefaultRegistry)
			go timer.Time(func() {
				totWait.Add(1)
				e.lock.RLock()

				noh := len(e.handlers)
				if noh == 0 {
					e.log.Debugf("there are no active listeners, skipping broadcast")
					e.lock.RUnlock()
					totWait.Done()
					return
				}

				var wg sync.WaitGroup
				wg.Add(noh)
				e.log.Debugf("notifying %d listeners", noh)

				for _, handler := range e.handlers {
					go e.dispatchEventWithTimeout(handler, timeout, evt, &wg)
				}

				wg.Wait()
				e.lock.RUnlock()
				totWait.Done()
			})
		case closed := <-e.closing:
			totWait.Wait()
			close(e.channel)
			e.lock.RLock()
			for _, handler := range e.handlers {
				close(handler)
			}
			e.handlers = nil
			e.lock.RUnlock()
			closed <- struct{}{}
			e.log.Debug("event bus closed")
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
	}
	wg.Done()
}

func (e *defaultEventBus) Broadcaster() chan<- Event {
	return e.channel
}

func (e *defaultEventBus) Add(handler ...chan<- Event) {
	e.lock.Lock()
	e.log.Debugf("adding %d listeners", len(handler))
	e.handlers = append(e.handlers, handler...)
	e.lock.Unlock()
}

func (e *defaultEventBus) Remove(handler ...chan<- Event) {
	e.lock.Lock()
	defer e.lock.Unlock()
	if len(e.handlers) == 0 {
		e.log.Debugf("nothing to remove from", len(handler))
		return
	}
	e.remove(handler...)
}

func (e *defaultEventBus) remove(handler ...chan<- Event) {
	e.log.Debugf("removing %d listeners", len(handler))
	for _, h := range handler {
		for i, handler := range e.handlers {
			if h == handler {
				close(handler)
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
	e.lock.RLock()
	sz := len(e.handlers)
	e.lock.RUnlock()
	return sz
}
