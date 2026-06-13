// Command gateway listens for MTP (Machine Telemetry Protocol) TCP connections
// from PLC/edge relays (default :5037).
//
//	DATABASE_URL=postgres://... IOT_ADDR=:5037 go run ./cmd/gateway
package main

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/iag/machine-telemetry/iot"
	"github.com/iag/machine-telemetry/pg"
)

var version = "dev"

func main() {
	configureLogger()
	addr := os.Getenv("IOT_ADDR")
	if addr == "" {
		addr = ":5037"
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	registryPool, telemetryPool, err := pg.ConnectSplit(connectCtx)
	cancel()
	if err != nil {
		slog.Error("connect Postgres", "err", err)
		os.Exit(1)
	}
	defer registryPool.Close()
	if telemetryPool != registryPool {
		defer telemetryPool.Close()
	}

	store := iot.NewSplitStore(registryPool, telemetryPool)
	if os.Getenv("REGISTRY_DATABASE_URL") != "" {
		slog.Info("machine-telemetry TCP gateway: split DB (registry + telemetry)")
	}
	hub := iot.NewHubFromEnv()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen failed", "addr", addr, "err", err)
		os.Exit(1)
	}
	slog.Info("machine-telemetry TCP gateway listening", "addr", addr, "version", version)

	srv := &tcpGateway{store: store, hub: hub}
	go srv.serve(listener)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	_ = listener.Close()
	done := make(chan struct{})
	go func() {
		srv.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
	}
}

func configureLogger() {
	var h slog.Handler
	if os.Getenv("LOG_FORMAT") == "json" {
		h = slog.NewJSONHandler(os.Stderr, nil)
	} else {
		h = slog.NewTextHandler(os.Stderr, nil)
	}
	slog.SetDefault(slog.New(h))
}

type tcpGateway struct {
	store *iot.Store
	hub   *iot.Hub
	wg    sync.WaitGroup
}

func (g *tcpGateway) serve(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("accept failed", "err", err)
			continue
		}
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			g.handle(conn)
		}()
	}
}

func (g *tcpGateway) handle(conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	r := bufio.NewReader(conn)
	serial, err := iot.ReadHandshake(r)
	if err != nil {
		return
	}
	logger := slog.With("remote", remote, "serial", serial)
	ctx := context.Background()
	device, err := g.store.FindBySerial(ctx, serial)
	if err != nil {
		_ = iot.WriteHandshakeResponse(conn, false)
		return
	}
	if err := iot.WriteHandshakeResponse(conn, true); err != nil {
		return
	}
	_ = g.store.MarkSeen(ctx, device.ID, ipOnly(remote))

	for {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		codec, records, err := iot.ReadMTPPacket(r)
		if err != nil {
			return
		}
		readings := make([]iot.Reading, 0, len(records))
		for _, rec := range records {
			readings = append(readings, iot.RecordToReading(rec, device))
		}
		if _, err := g.store.InsertReadings(ctx, readings); err != nil {
			logger.Error("insert readings failed", "err", err)
			return
		}
		if device.AssetTag != "" && len(readings) > 0 {
			newest := readings[0]
			for _, rd := range readings[1:] {
				if rd.TS.After(newest.TS) {
					newest = rd
				}
			}
			if _, err := g.store.ApplyAssetHotState(ctx, newest); err != nil {
				logger.Warn("registry sync failed after TCP ingest",
					"assetTag", device.AssetTag, "err", err)
			}
		}
		_ = g.store.MarkSeen(ctx, device.ID, ipOnly(remote))
		_ = iot.WriteACK(conn, len(records))
		if g.hub != nil {
			for _, rd := range readings {
				g.hub.Publish(rd)
			}
		}
		logger.Info("readings persisted", "count", len(records), "codec", codec)
	}
}

func ipOnly(remote string) string {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		return remote
	}
	return host
}
