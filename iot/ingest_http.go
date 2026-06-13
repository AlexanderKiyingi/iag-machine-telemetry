package iot

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"
)

const MaxIngestBatch = 1000

// IngestReadingBody is one HTTP ingest row (machine relay or manual POST).
type IngestReadingBody struct {
	AssetTag     string          `json:"assetTag"`
	TS           time.Time       `json:"ts"`
	State        string          `json:"state"`
	SpindleRPM   *float64        `json:"spindleRpm"`
	FeedRate     *float64        `json:"feedRate"`
	TemperatureC *float64        `json:"temperatureC"`
	VibrationMmS *float64        `json:"vibrationMmS"`
	PressureBar  *float64        `json:"pressureBar"`
	PowerKW      *float64        `json:"powerKw"`
	GoodCount    *int64          `json:"goodCount"`
	RejectCount  *int64          `json:"rejectCount"`
	CycleCount   *int64          `json:"cycleCount"`
	FaultCode    *string         `json:"faultCode"`
	Raw          json.RawMessage `json:"raw"`
}

// IngestBatchResult is the HTTP ingest outcome. Readings are persisted even
// when registry sync fails; callers can inspect RegistrySync* for partial
// failures.
type IngestBatchResult struct {
	Accepted           int    `json:"accepted"`
	RegistrySyncFailed bool   `json:"registrySyncFailed,omitempty"`
	RegistrySyncError  string `json:"registrySyncError,omitempty"`
}

// IngestHTTPBatch authenticates via device API key, validates, persists
// readings, syncs machine hot-state, and publishes to the live hub.
func IngestHTTPBatch(ctx context.Context, store *Store, hub *Hub, apiKey string, body []byte, clientIP string) (IngestBatchResult, error) {
	device, err := store.AuthenticateAPIKey(ctx, apiKey)
	if err != nil {
		return IngestBatchResult{}, err
	}
	body = []byte(strings.TrimSpace(string(body)))
	if len(body) > 0 && body[0] == '{' {
		body = []byte("[" + string(body) + "]")
	}
	var batch []IngestReadingBody
	if err := json.Unmarshal(body, &batch); err != nil {
		return IngestBatchResult{}, fmt.Errorf("malformed body: %w", err)
	}
	if len(batch) == 0 {
		return IngestBatchResult{}, fmt.Errorf("empty batch")
	}
	if len(batch) > MaxIngestBatch {
		return IngestBatchResult{}, fmt.Errorf("batch exceeds %d readings", MaxIngestBatch)
	}
	now := time.Now().UTC()
	readings := make([]Reading, 0, len(batch))
	for _, b := range batch {
		assetTag := b.AssetTag
		if assetTag == "" {
			assetTag = device.AssetTag
		}
		if assetTag == "" {
			return IngestBatchResult{}, fmt.Errorf("assetTag required (device has no default binding)")
		}
		ts := b.TS
		if ts.IsZero() {
			ts = now
		} else {
			if ts.Before(now.Add(-24 * time.Hour)) {
				return IngestBatchResult{}, fmt.Errorf("timestamp too old")
			}
			if ts.After(now.Add(5 * time.Minute)) {
				return IngestBatchResult{}, fmt.Errorf("timestamp in the future")
			}
		}
		raw := b.Raw
		if len(raw) == 0 {
			raw = json.RawMessage(`{}`)
		}
		devID := device.ID
		readings = append(readings, Reading{
			AssetTag: assetTag, DeviceID: &devID, TS: ts, State: NormalizeState(b.State),
			SpindleRPM: b.SpindleRPM, FeedRate: b.FeedRate, TemperatureC: b.TemperatureC,
			VibrationMmS: b.VibrationMmS, PressureBar: b.PressureBar, PowerKW: b.PowerKW,
			GoodCount: b.GoodCount, RejectCount: b.RejectCount, CycleCount: b.CycleCount,
			FaultCode: b.FaultCode, Raw: raw,
		})
	}
	n, err := store.InsertReadings(ctx, readings)
	if err != nil {
		return IngestBatchResult{}, err
	}
	result := IngestBatchResult{Accepted: n}
	if newest := newestReading(readings); newest != nil && newest.AssetTag != "" {
		if _, err := store.ApplyAssetHotState(ctx, *newest); err != nil {
			slog.Warn("registry sync failed after ingest",
				"assetTag", newest.AssetTag, "err", err)
			result.RegistrySyncFailed = true
			result.RegistrySyncError = err.Error()
		}
	}
	_ = store.MarkSeen(ctx, device.ID, clientIP)
	if hub != nil {
		for _, r := range readings {
			hub.Publish(r)
		}
	}
	return result, nil
}

// ReadIngestBody reads and normalizes a request body for IngestHTTPBatch.
func ReadIngestBody(r io.Reader) ([]byte, error) {
	return io.ReadAll(r)
}
