package eventbus

import (
	"log"
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

// NOOPHandler drops events on the floor without taking action
var NOOPHandler = HandlerWithError(func(_ Event) error { return nil }, nil)

type defaultEventHandler struct {
	on    func(Event) error
	onErr func(error)
}

func (h *defaultEventHandler) On(evt Event) error {
	return h.on(evt)
}

func (h *defaultEventHandler) OnFailed(err error) {
	if h.on != nil {
		h.OnFailed(err)
	}
}

func Handler(handler func(Event) error) EventHandler {
	return HandlerWithError(handler, func(err error) { log.Println(err) })
}

func HandlerWithError(handler func(Event) error, errorHandler func(error)) EventHandler {
	return &defaultEventHandler{
		on:    handler,
		onErr: errorHandler,
	}
}

// EventHander
type EventHandler interface {
	On(Event) error
	OnFailed(error)
}

type filteredHandler struct {
	Next    EventHandler
	Matches EventPredicate
}

func (f *filteredHandler) On(evt Event) error {
	if !f.Matches(evt) {
		return nil
	}
	return f.On(evt)
}

func (f *filteredHandler) OnFailed(err error) {
	f.OnFailed(err)
}

// EventPredicate for filtering events
type EventPredicate func(Event) bool

// Filtered composes an event handler with a filter
func Filtered(matches EventPredicate, next EventHandler) EventHandler {
	return &filteredHandler{
		Matches: matches,
		Next:    next,
	}
}

// EventBus does fanout to registered channels
type EventBus interface {
	Close() error
	Publish(Event)
	Subscribe(...EventHandler)
	Unsubscribe(...EventHandler)
	Len() int
}

type defaultEventBus struct {
	lock *sync.RWMutex

	channel  chan Event
	handlers []EventHandler
	closing  chan chan struct{}
	log      logrus.FieldLogger
}

// New event bus with specified logger
func New(log logrus.FieldLogger) EventBus {
	if log == nil {
		log = logrus.New().WithFields(nil)
	}
	e := &defaultEventBus{
		closing: make(chan chan struct{}),
		channel: make(chan Event, 100),
		log:     log,
		lock:    new(sync.RWMutex),
	}
	go e.dispatcherLoop()
	return e
}

func (e *defaultEventBus) dispatcherLoop() {
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
				e.log.Debugln(e.handlers)
				for _, handler := range e.handlers {
					e.log.Debugln("notifying", handler)
					go func(handler EventHandler) {
						if err := handler.On(evt); err != nil {
							handler.OnFailed(err)
						}
						wg.Done()
					}(handler)
				}

				wg.Wait()
				e.lock.RUnlock()
				totWait.Done()
			})
		case closed := <-e.closing:
			totWait.Wait()

			e.lock.Lock()
			e.handlers = nil
			close(e.channel)
			e.lock.Unlock()

			closed <- struct{}{}
			e.log.Debug("event bus closed")
			return
		}
	}
}

func (e *defaultEventBus) dispatchEventWithTimeout(channel chan<- Event, timeout time.Duration, event Event, wg *sync.WaitGroup) {
	timer := time.NewTimer(timeout) // uses timer so it can be stopped and cleaned up
	select {
	case channel <- event:
		timer.Stop()
	case <-timer.C:
		e.log.Warnf("Failed to send event %+v to listener within %v", event, timeout)
	}
	wg.Done()
}

// Publish an event to all interested subscribers
func (e *defaultEventBus) Publish(evt Event) {
	e.channel <- evt
}

func (e *defaultEventBus) Subscribe(handlers ...EventHandler) {
	e.lock.Lock()
	e.log.Debugf("adding %d listeners", len(handlers))
	e.handlers = append(e.handlers, handlers...)
	e.lock.Unlock()
}

func (e *defaultEventBus) Unsubscribe(handlers ...EventHandler) {
	e.lock.RLock()
	if len(e.handlers) == 0 {
		e.log.Debugf("nothing to remove from", len(handlers))
		e.lock.RUnlock()
		return
	}
	e.lock.RUnlock()
	e.lock.Lock()
	e.log.Debugf("removing %d listeners", len(handlers))
	for _, h := range handlers {
		for i, handler := range e.handlers {
			if h == handler {
				// replace handler because it will still process messages in flight
				e.handlers = append(e.handlers[:i], e.handlers[i+1:]...)
				break
			}
		}
	}
	e.lock.Unlock()
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
