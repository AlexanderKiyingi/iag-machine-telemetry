package iot

import (
	"testing"
	"time"
)

// Availability must not flatter a machine that stood idle.
//
// The denominator was running+down+setup, which leaves idle and blocked out
// entirely: a machine that ran for one minute and stood idle for the rest of
// the shift reported ~100% availability, so a badly-utilised plant and a
// well-run one produced the same number. Idle during production time is a
// loss under any reading of OEE.
func TestIdleCountsAgainstAvailability(t *testing.T) {
	base := time.Date(2026, 6, 13, 8, 0, 0, 0, time.UTC)
	readings := []Reading{
		{AssetTag: "MCH-1", TS: base, State: StateRunning},
		// One minute of work, then it sits there for two hours.
		{AssetTag: "MCH-1", TS: base.Add(1 * time.Minute), State: StateIdle},
		{AssetTag: "MCH-1", TS: base.Add(121 * time.Minute), State: StateIdle},
	}
	res := AggregateDay("MCH-1", base, readings, AggregateParams{MaxGap: 4 * time.Hour})
	if res.Summary.Availability == nil {
		t.Fatal("availability was not computed")
	}
	got := *res.Summary.Availability
	// 1 minute of running in 121 minutes of being there to run.
	if got > 0.05 {
		t.Errorf("availability = %.3f for a machine that ran one minute in two hours; idle is being excluded from the denominator", got)
	}
}

// A machine that was switched off was not available to be run, so the time
// does not count against it. That is the one exclusion that is right.
func TestTimeSwitchedOffDoesNotCountAgainstAvailability(t *testing.T) {
	base := time.Date(2026, 6, 13, 8, 0, 0, 0, time.UTC)
	readings := []Reading{
		{AssetTag: "MCH-1", TS: base, State: StateRunning},
		{AssetTag: "MCH-1", TS: base.Add(60 * time.Minute), State: StateOff},
		{AssetTag: "MCH-1", TS: base.Add(600 * time.Minute), State: StateOff},
	}
	res := AggregateDay("MCH-1", base, readings, AggregateParams{MaxGap: 24 * time.Hour})
	if res.Summary.Availability == nil {
		t.Fatal("availability was not computed")
	}
	if got := *res.Summary.Availability; got < 0.99 {
		t.Errorf("availability = %.3f; an hour's work then nine hours powered down should not read as downtime", got)
	}
}

// `off` has to survive the wire and the normaliser, or a device cannot report
// it however well the aggregator handles it.
func TestOffSurvivesTheWireAndTheNormaliser(t *testing.T) {
	if got := StateForCode(6); got != StateOff {
		t.Errorf("MTP state byte 6 = %q, want %q", got, StateOff)
	}
	if got := NormalizeState("OFF"); got != StateOff {
		t.Errorf("NormalizeState(\"OFF\") = %q, want %q", got, StateOff)
	}
}
