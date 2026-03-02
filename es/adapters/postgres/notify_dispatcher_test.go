package postgres

import (
	"sync"
	"testing"
	"time"
)

func TestNotifyDispatcher_DefaultConfig(t *testing.T) {
	nd := NewNotifyDispatcher("postgres://localhost/test", nil)

	if nd.config.Channel != "pupsourcing_events" {
		t.Errorf("Channel = %q, want %q", nd.config.Channel, "pupsourcing_events")
	}
	if nd.config.FallbackInterval != 30*time.Second {
		t.Errorf("FallbackInterval = %v, want %v", nd.config.FallbackInterval, 30*time.Second)
	}
	if nd.config.MinReconnectInterval != 5*time.Second {
		t.Errorf("MinReconnectInterval = %v, want %v", nd.config.MinReconnectInterval, 5*time.Second)
	}
	if nd.config.MaxReconnectInterval != 60*time.Second {
		t.Errorf("MaxReconnectInterval = %v, want %v", nd.config.MaxReconnectInterval, 60*time.Second)
	}
}

func TestNotifyDispatcher_CustomConfig(t *testing.T) {
	nd := NewNotifyDispatcher("postgres://localhost/test", &NotifyDispatcherConfig{
		Channel:              "custom_channel",
		FallbackInterval:     10 * time.Second,
		MinReconnectInterval: 2 * time.Second,
		MaxReconnectInterval: 30 * time.Second,
	})

	if nd.config.Channel != "custom_channel" {
		t.Errorf("Channel = %q, want %q", nd.config.Channel, "custom_channel")
	}
	if nd.config.FallbackInterval != 10*time.Second {
		t.Errorf("FallbackInterval = %v, want %v", nd.config.FallbackInterval, 10*time.Second)
	}
	if nd.config.MinReconnectInterval != 2*time.Second {
		t.Errorf("MinReconnectInterval = %v, want %v", nd.config.MinReconnectInterval, 2*time.Second)
	}
	if nd.config.MaxReconnectInterval != 30*time.Second {
		t.Errorf("MaxReconnectInterval = %v, want %v", nd.config.MaxReconnectInterval, 30*time.Second)
	}
}

func TestNotifyDispatcher_Subscribe(t *testing.T) {
	nd := NewNotifyDispatcher("postgres://localhost/test", nil)

	ch1, unsub1 := nd.Subscribe()
	ch2, unsub2 := nd.Subscribe()
	defer unsub1()
	defer unsub2()

	if ch1 == nil || ch2 == nil {
		t.Fatal("Subscribe returned nil channel")
	}

	nd.mu.RLock()
	count := len(nd.subscribers)
	nd.mu.RUnlock()

	if count != 2 {
		t.Errorf("subscriber count = %d, want 2", count)
	}
}

func TestNotifyDispatcher_Unsubscribe(t *testing.T) {
	nd := NewNotifyDispatcher("postgres://localhost/test", nil)

	_, unsub1 := nd.Subscribe()
	_, unsub2 := nd.Subscribe()

	unsub1()

	nd.mu.RLock()
	count := len(nd.subscribers)
	nd.mu.RUnlock()

	if count != 1 {
		t.Errorf("subscriber count after unsubscribe = %d, want 1", count)
	}

	// Double unsubscribe is safe
	unsub1()

	nd.mu.RLock()
	count = len(nd.subscribers)
	nd.mu.RUnlock()

	if count != 1 {
		t.Errorf("subscriber count after double unsubscribe = %d, want 1", count)
	}

	unsub2()

	nd.mu.RLock()
	count = len(nd.subscribers)
	nd.mu.RUnlock()

	if count != 0 {
		t.Errorf("subscriber count after all unsubscribe = %d, want 0", count)
	}
}

func TestNotifyDispatcher_Broadcast(t *testing.T) {
	nd := NewNotifyDispatcher("postgres://localhost/test", nil)

	ch1, unsub1 := nd.Subscribe()
	ch2, unsub2 := nd.Subscribe()
	defer unsub1()
	defer unsub2()

	nd.broadcast()

	// Both channels should have a signal
	select {
	case <-ch1:
	default:
		t.Error("ch1 did not receive signal")
	}

	select {
	case <-ch2:
	default:
		t.Error("ch2 did not receive signal")
	}
}

func TestNotifyDispatcher_BroadcastCoalesces(t *testing.T) {
	nd := NewNotifyDispatcher("postgres://localhost/test", nil)

	ch, unsub := nd.Subscribe()
	defer unsub()

	// Multiple broadcasts before consumer reads should coalesce
	nd.broadcast()
	nd.broadcast()
	nd.broadcast()

	select {
	case <-ch:
	default:
		t.Error("ch did not receive signal")
	}

	// Channel should be empty after one read
	select {
	case <-ch:
		t.Error("ch should be empty after one read (coalesced)")
	default:
	}
}

func TestNotifyDispatcher_BroadcastConcurrent(t *testing.T) {
	nd := NewNotifyDispatcher("postgres://localhost/test", nil)

	const numSubscribers = 50
	channels := make([]<-chan struct{}, numSubscribers)
	unsubs := make([]func(), numSubscribers)

	for i := 0; i < numSubscribers; i++ {
		channels[i], unsubs[i] = nd.Subscribe()
	}
	t.Cleanup(func() {
		for i := 0; i < numSubscribers; i++ {
			unsubs[i]()
		}
	})

	// Broadcast from multiple goroutines
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nd.broadcast()
		}()
	}
	wg.Wait()

	// All subscribers should have received at least one signal
	for i, ch := range channels {
		select {
		case <-ch:
		default:
			t.Errorf("subscriber %d did not receive signal", i)
		}
	}
}

func TestNotifyDispatcher_ImplementsWakeupSource(t *testing.T) {
	nd := NewNotifyDispatcher("postgres://localhost/test", nil)

	// Verify it satisfies the interface at compile time (var _ check is in the source).
	// Also verify Subscribe returns correct types.
	signals, unsub := nd.Subscribe()
	defer unsub()

	if signals == nil {
		t.Error("Subscribe returned nil signals channel")
	}
}
