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

// Running time has to be detected, not just downtime.
//
// detectDowntime walks runs of `down` and nothing else, so the only thing a
// device ever contributed to iag-production was stoppages — a sensor-fed
// machine had down intervals and no running time, and availability read a
// confident 0% for a machine that had worked all shift.
func TestEveryStateBecomesAnIntervalExceptDownAndUnknown(t *testing.T) {
	base := time.Date(2026, 6, 13, 8, 0, 0, 0, time.UTC)
	readings := []Reading{
		{AssetTag: "MCH-1", TS: base, State: StateRunning},
		{AssetTag: "MCH-1", TS: base.Add(60 * time.Minute), State: StateDown},
		{AssetTag: "MCH-1", TS: base.Add(75 * time.Minute), State: StateIdle},
		{AssetTag: "MCH-1", TS: base.Add(105 * time.Minute), State: StateOff},
		{AssetTag: "MCH-1", TS: base.Add(165 * time.Minute), State: StateUnknown},
		{AssetTag: "MCH-1", TS: base.Add(180 * time.Minute), State: StateRunning},
	}
	res := AggregateDay("MCH-1", base, readings, AggregateParams{MaxGap: time.Hour})

	byState := map[string]float64{}
	for _, iv := range res.StateIntervals {
		byState[iv.State] += iv.EndedAt.Sub(iv.StartedAt).Minutes()
	}
	if byState[StateRunning] != 120 {
		t.Errorf("running = %v, want 120 (60 up front + the trailing hour capped at MaxGap)", byState[StateRunning])
	}
	if byState[StateIdle] != 30 {
		t.Errorf("idle = %v, want 30", byState[StateIdle])
	}
	if byState[StateOff] != 60 {
		t.Errorf("off = %v, want 60", byState[StateOff])
	}
	// `down` rides the downtime event; announcing it here too would record
	// one stoppage twice in production.
	if _, ok := byState[StateDown]; ok {
		t.Error("down was emitted as a state interval as well as a downtime event")
	}
	// `unknown` is no data, not a state the machine was in.
	if _, ok := byState[StateUnknown]; ok {
		t.Error("unknown was emitted as if it were something the machine did")
	}
	// The stop and the running time either side of it must meet, not overlap.
	for _, dt := range res.DowntimeEvents {
		for _, iv := range res.StateIntervals {
			if iv.StartedAt.Before(*dt.EndedAt) && dt.StartedAt.Before(iv.EndedAt) {
				t.Errorf("state %s %v–%v overlaps the stop %v–%v",
					iv.State, iv.StartedAt, iv.EndedAt, dt.StartedAt, *dt.EndedAt)
			}
		}
	}
}
