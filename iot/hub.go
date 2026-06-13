package iot

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

const (
	redisMachineChannelPrefix = "machine:telemetry:machine:"
	redisLiveChannel          = "machine:telemetry:live"
)

// Hub fans out readings in-process and via Redis pub/sub when REDIS_URL is set.
// HTTP ingest and the TCP gateway share it so a live feed works across replicas.
type Hub struct {
	local *Broker
	redis *redis.Client
}

// NewHub wraps a local broker and optional Redis client. Pass redis=nil for
// single-process deployments.
func NewHub(local *Broker, rdb *redis.Client) *Hub {
	if local == nil {
		local = NewBroker()
	}
	return &Hub{local: local, redis: rdb}
}

// NewHubFromEnv builds a Hub, attaching Redis when REDIS_URL parses.
func NewHubFromEnv() *Hub {
	url := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if url == "" {
		return NewHub(NewBroker(), nil)
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		slog.Warn("invalid REDIS_URL; live fan-out is in-process only", "err", err)
		return NewHub(NewBroker(), nil)
	}
	return NewHub(NewBroker(), redis.NewClient(opt))
}

// Publish delivers a reading to local subscribers and Redis channels.
func (h *Hub) Publish(r Reading) {
	if h == nil {
		return
	}
	h.local.Publish(r)
	if h.redis == nil || r.MachineID == "" {
		return
	}
	b, err := json.Marshal(r)
	if err != nil {
		return
	}
	ctx := context.Background()
	if err := h.redis.Publish(ctx, redisMachineChannelPrefix+r.MachineID, b).Err(); err != nil {
		slog.Debug("redis telemetry publish", "machineId", r.MachineID, "err", err)
	}
	if err := h.redis.Publish(ctx, redisLiveChannel, b).Err(); err != nil {
		slog.Debug("redis live publish", "err", err)
	}
}

// Subscribe returns readings for one machine. Uses Redis when configured, else
// the in-process broker.
func (h *Hub) Subscribe(machineID string) (<-chan Reading, func()) {
	if h == nil {
		ch := make(chan Reading)
		close(ch)
		return ch, func() {}
	}
	out := make(chan Reading, 32)
	forward := func(r Reading) {
		select {
		case out <- r:
		default:
		}
	}
	if h.redis != nil {
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := h.redis.Subscribe(ctx, redisMachineChannelPrefix+machineID)
			defer func() { _ = sub.Close() }()
			for msg := range sub.Channel() {
				var r Reading
				if json.Unmarshal([]byte(msg.Payload), &r) == nil {
					forward(r)
				}
			}
		}()
		return out, func() {
			cancel()
			wg.Wait()
			close(out)
		}
	}
	localCh, localCancel := h.local.Subscribe(machineID)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range localCh {
			forward(r)
		}
	}()
	return out, func() {
		localCancel()
		wg.Wait()
		close(out)
	}
}

// SubscribeLive receives every machine reading. With Redis, only the pub/sub
// channel is used (avoids duplicate delivery when ingest and API share a
// process); without Redis, the in-process broker is used.
func (h *Hub) SubscribeLive() (<-chan Reading, func()) {
	if h == nil {
		ch := make(chan Reading)
		close(ch)
		return ch, func() {}
	}
	out := make(chan Reading, 64)
	forward := func(r Reading) {
		if r.MachineID == "" {
			return
		}
		select {
		case out <- r:
		default:
		}
	}
	if h.redis != nil {
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := h.redis.Subscribe(ctx, redisLiveChannel)
			defer func() { _ = sub.Close() }()
			for msg := range sub.Channel() {
				var r Reading
				if json.Unmarshal([]byte(msg.Payload), &r) == nil {
					forward(r)
				}
			}
		}()
		return out, func() {
			cancel()
			wg.Wait()
			close(out)
		}
	}
	localCh, localCancel := h.local.SubscribeLive()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range localCh {
			forward(r)
		}
	}()
	return out, func() {
		localCancel()
		wg.Wait()
		close(out)
	}
}
