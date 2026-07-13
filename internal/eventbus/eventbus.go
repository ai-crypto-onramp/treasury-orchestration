// Package eventbus provides the event subscription abstraction for the
// Treasury Orchestration service. The Transaction Orchestrator emits
// "tx.completed" events which Treasury consumes to create batch
// memberships.
//
// To keep external dependencies minimal, the EventSubscriber is an
// interface with two implementations:
//   - InMemorySubscriber: a channel-backed fan-in used by tests and the
//     in-memory run mode.
//   - HTTPPushSubscriber: accepts events over a local HTTP endpoint
//     (/v1/events/tx.completed) so an upstream producer can POST events
//     directly when a real broker (NATS/Kafka) is not deployed.
//
// A real NATS or Kafka adapter can be added later behind the same
// interface without changing callers.
package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// TxCompletedEvent is the payload emitted by transaction-orchestrator on
// tx completion.
type TxCompletedEvent struct {
	TxID         string  `json:"tx_id"`
	Amount       float64 `json:"amount"`
	Asset        string  `json:"asset"`
	FiatCurrency string  `json:"fiat_currency"`
	NotionalUSD  float64 `json:"notional_usd"`
	UserID       string  `json:"user_id"`
	CompletedAt  string  `json:"completed_at"`
}

// EventSubscriber consumes tx completion events.
type EventSubscriber interface {
	// Subscribe returns a channel of events and a stop function. The
	// caller drains the channel; calling stop releases the subscription.
	Subscribe(ctx context.Context, topic string) (<-chan TxCompletedEvent, func(), error)
	// Push enqueues an event onto the given topic (for the in-memory /
	// HTTP-push adapters; real broker adapters may no-op or publish).
	Push(ctx context.Context, topic string, ev TxCompletedEvent) error
}

// InMemorySubscriber is a channel-backed fan-in subscriber for tests.
type InMemorySubscriber struct {
	mu        sync.Mutex
	channels  map[string][]chan TxCompletedEvent
}

// NewInMemory returns an in-memory event subscriber.
func NewInMemory() *InMemorySubscriber {
	return &InMemorySubscriber{channels: map[string][]chan TxCompletedEvent{}}
}

// Subscribe returns a channel that receives events for the topic.
func (s *InMemorySubscriber) Subscribe(ctx context.Context, topic string) (<-chan TxCompletedEvent, func(), error) {
	ch := make(chan TxCompletedEvent, 1024)
	s.mu.Lock()
	s.channels[topic] = append(s.channels[topic], ch)
	s.mu.Unlock()
	stop := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		chs := s.channels[topic]
		for i, c := range chs {
			if c == ch {
				s.channels[topic] = append(chs[:i], chs[i+1:]...)
				close(c)
				return
			}
		}
	}
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ch, stop, nil
}

// Push enqueues an event to all subscribers of the topic (non-blocking).
func (s *InMemorySubscriber) Push(_ context.Context, topic string, ev TxCompletedEvent) error {
	s.mu.Lock()
	chs := append([]chan TxCompletedEvent(nil), s.channels[topic]...)
	s.mu.Unlock()
	for _, ch := range chs {
		select {
		case ch <- ev:
		default:
			// channel full; drop. Production adapters would block / retry.
		}
	}
	return nil
}

// HTTPPushSubscriber wraps an InMemorySubscriber and exposes an HTTP
// handler that accepts POSTed events. It implements EventSubscriber by
// delegating to the embedded in-memory subscriber.
type HTTPPushSubscriber struct {
	inner *InMemorySubscriber
}

// NewHTTPPush returns an HTTP-push subscriber.
func NewHTTPPush() *HTTPPushSubscriber {
	return &HTTPPushSubscriber{inner: NewInMemory()}
}

// Subscribe delegates to the in-memory subscriber.
func (s *HTTPPushSubscriber) Subscribe(ctx context.Context, topic string) (<-chan TxCompletedEvent, func(), error) {
	return s.inner.Subscribe(ctx, topic)
}

// Push delegates to the in-memory subscriber.
func (s *HTTPPushSubscriber) Push(ctx context.Context, topic string, ev TxCompletedEvent) error {
	return s.inner.Push(ctx, topic, ev)
}

// HTTPHandler returns an http.Handler that accepts POST {topic} and
// forwards the decoded event to the in-memory bus.
func (s *HTTPPushSubscriber) HTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		topic := r.URL.Path[len("/v1/events/"):]
		if topic == "" {
			http.Error(w, "missing topic", http.StatusBadRequest)
			return
		}
		var ev TxCompletedEvent
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			http.Error(w, fmt.Sprintf("malformed json: %v", err), http.StatusBadRequest)
			return
		}
		if err := s.inner.Push(r.Context(), topic, ev); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
}