package iot

import "sync"

// Broker is an in-process fan-out of readings to live subscribers (one channel
// per active SSE client). Per-machine subscribers receive only their machine's
// readings; live subscribers receive every reading. Slow consumers drop rather
// than block ingest.
type Broker struct {
	mu         sync.RWMutex
	perMachine map[string]map[int]chan Reading
	live       map[int]chan Reading
	nextID     int
}

func NewBroker() *Broker {
	return &Broker{
		perMachine: map[string]map[int]chan Reading{},
		live:       map[int]chan Reading{},
	}
}

// Publish delivers a reading to per-machine and live subscribers, never blocking.
func (b *Broker) Publish(r Reading) {
	if b == nil {
		return
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.perMachine[r.MachineID] {
		select {
		case ch <- r:
		default:
		}
	}
	for _, ch := range b.live {
		select {
		case ch <- r:
		default:
		}
	}
}

// Subscribe returns a channel of readings for one machine and a cancel func.
func (b *Broker) Subscribe(machineID string) (<-chan Reading, func()) {
	ch := make(chan Reading, 32)
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	if b.perMachine[machineID] == nil {
		b.perMachine[machineID] = map[int]chan Reading{}
	}
	b.perMachine[machineID][id] = ch
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		if subs := b.perMachine[machineID]; subs != nil {
			if c, ok := subs[id]; ok {
				delete(subs, id)
				close(c)
			}
			if len(subs) == 0 {
				delete(b.perMachine, machineID)
			}
		}
		b.mu.Unlock()
	}
}

// SubscribeLive returns a channel of every reading and a cancel func.
func (b *Broker) SubscribeLive() (<-chan Reading, func()) {
	ch := make(chan Reading, 64)
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	b.live[id] = ch
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		if c, ok := b.live[id]; ok {
			delete(b.live, id)
			close(c)
		}
		b.mu.Unlock()
	}
}
