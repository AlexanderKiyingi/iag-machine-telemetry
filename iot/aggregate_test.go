package iot

import (
	"testing"
	"time"
)

func i64(v int64) *int64     { return &v }
func f64(v float64) *float64 { return &v }
func strp(v string) *string  { return &v }

// readingsFixture builds a simple day: 60m running, 10m down, then running.
func readingsFixture(base time.Time) []Reading {
	return []Reading{
		{MachineID: "MCH-1", TS: base, State: StateRunning, SpindleRPM: f64(10000), TemperatureC: f64(40), GoodCount: i64(0), RejectCount: i64(0), CycleCount: i64(0)},
		{MachineID: "MCH-1", TS: base.Add(30 * time.Minute), State: StateRunning, SpindleRPM: f64(12000), TemperatureC: f64(55), GoodCount: i64(280), RejectCount: i64(20), CycleCount: i64(300)},
		{MachineID: "MCH-1", TS: base.Add(60 * time.Minute), State: StateDown, FaultCode: strp("F0042"), GoodCount: i64(280), RejectCount: i64(20), CycleCount: i64(300)},
		{MachineID: "MCH-1", TS: base.Add(65 * time.Minute), State: StateDown, GoodCount: i64(280), RejectCount: i64(20), CycleCount: i64(300)},
		{MachineID: "MCH-1", TS: base.Add(70 * time.Minute), State: StateRunning, SpindleRPM: f64(11000), TemperatureC: f64(50), GoodCount: i64(380), RejectCount: i64(20), CycleCount: i64(400)},
		{MachineID: "MCH-1", TS: base.Add(100 * time.Minute), State: StateRunning, GoodCount: i64(480), RejectCount: i64(20), CycleCount: i64(500)},
	}
}

func TestAggregateDayStateMinutesAndCounters(t *testing.T) {
	base := time.Date(2026, 6, 13, 8, 0, 0, 0, time.UTC)
	// MaxGap > 30m so the full inter-reading gaps are credited (the default 15m
	// cap is exercised separately by relying on it elsewhere).
	res := AggregateDay("MCH-1", base, readingsFixture(base), AggregateParams{MaxGap: time.Hour})
	s := res.Summary

	// running: 0-30, 30-60 (60m) + 70-100 (30m) = 90m. down: 60-65,65-70 = 10m.
	if s.RunningMinutes != 90 {
		t.Errorf("RunningMinutes = %d, want 90", s.RunningMinutes)
	}
	if s.DownMinutes != 10 {
		t.Errorf("DownMinutes = %d, want 10", s.DownMinutes)
	}
	if s.ReadingCount != 6 {
		t.Errorf("ReadingCount = %d, want 6", s.ReadingCount)
	}
	if s.GoodTotal != 480 {
		t.Errorf("GoodTotal = %d, want 480", s.GoodTotal)
	}
	if s.RejectTotal != 20 {
		t.Errorf("RejectTotal = %d, want 20", s.RejectTotal)
	}
	if s.CyclesTotal != 500 {
		t.Errorf("CyclesTotal = %d, want 500", s.CyclesTotal)
	}
	if s.MaxTemperatureC == nil || *s.MaxTemperatureC != 55 {
		t.Errorf("MaxTemperatureC = %v, want 55", s.MaxTemperatureC)
	}
}

func TestAggregateDayOEE(t *testing.T) {
	base := time.Date(2026, 6, 13, 8, 0, 0, 0, time.UTC)
	// Ideal 10 cycles/min over 90 running minutes = 900 ideal; produced 500.
	res := AggregateDay("MCH-1", base, readingsFixture(base), AggregateParams{IdealCyclesPerMin: 10, MaxGap: time.Hour})
	s := res.Summary

	if s.Availability == nil {
		t.Fatal("Availability nil")
	}
	// scheduled = running(90) + down(10) + setup(0) = 100; A = 0.9.
	if got := *s.Availability; got < 0.89 || got > 0.91 {
		t.Errorf("Availability = %v, want ~0.9", got)
	}
	if s.Quality == nil || *s.Quality < 0.95 || *s.Quality > 0.97 {
		t.Errorf("Quality = %v, want ~0.96 (480/500)", s.Quality)
	}
	if s.Performance == nil {
		t.Fatal("Performance nil with ideal rate set")
	}
	if got := *s.Performance; got < 0.55 || got > 0.56 {
		t.Errorf("Performance = %v, want ~0.555 (500/900)", got)
	}
	if s.OEE == nil {
		t.Fatal("OEE nil when all three components present")
	}
}

func TestAggregateDayNoIdealRateLeavesOEEUnset(t *testing.T) {
	base := time.Date(2026, 6, 13, 8, 0, 0, 0, time.UTC)
	res := AggregateDay("MCH-1", base, readingsFixture(base), AggregateParams{})
	if res.Summary.Performance != nil || res.Summary.OEE != nil {
		t.Errorf("Performance/OEE should be nil without an ideal rate")
	}
}

func TestDetectDowntime(t *testing.T) {
	base := time.Date(2026, 6, 13, 8, 0, 0, 0, time.UTC)
	res := AggregateDay("MCH-1", base, readingsFixture(base), AggregateParams{})
	if len(res.DowntimeEvents) != 1 {
		t.Fatalf("downtime events = %d, want 1", len(res.DowntimeEvents))
	}
	ev := res.DowntimeEvents[0]
	if !ev.StartedAt.Equal(base.Add(60 * time.Minute)) {
		t.Errorf("StartedAt = %v, want +60m", ev.StartedAt)
	}
	if ev.EndedAt == nil || !ev.EndedAt.Equal(base.Add(70*time.Minute)) {
		t.Errorf("EndedAt = %v, want +70m", ev.EndedAt)
	}
	if ev.DurationMin != 10 {
		t.Errorf("DurationMin = %v, want 10", ev.DurationMin)
	}
	if ev.FaultCode != "F0042" {
		t.Errorf("FaultCode = %q, want F0042", ev.FaultCode)
	}
	if ev.Confidence != "medium" { // run has 2 readings
		t.Errorf("Confidence = %q, want medium", ev.Confidence)
	}
}

func TestPositiveDeltaHandlesReset(t *testing.T) {
	rs := []Reading{
		{GoodCount: i64(100)},
		{GoodCount: i64(150)}, // +50
		{GoodCount: i64(30)},  // reset -> +30
		{GoodCount: i64(80)},  // +50
	}
	if got := positiveDelta(rs, func(r Reading) *int64 { return r.GoodCount }); got != 130 {
		t.Errorf("positiveDelta = %d, want 130", got)
	}
}

func TestAggregateDayEmpty(t *testing.T) {
	base := time.Date(2026, 6, 13, 0, 0, 0, 0, time.UTC)
	res := AggregateDay("MCH-1", base, nil, AggregateParams{})
	if res.Summary.ReadingCount != 0 || len(res.DowntimeEvents) != 0 {
		t.Errorf("empty aggregate should be zero-valued, got %+v", res)
	}
}
