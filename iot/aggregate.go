package iot

import "time"

// AggregateParams tunes the daily rollup. Both fields have sane zero-value
// fallbacks so callers can pass AggregateParams{} for a best-effort summary.
type AggregateParams struct {
	// IdealCyclesPerMin is the machine's nameplate throughput. When > 0,
	// Performance and therefore OEE are computed; when 0 they are left nil
	// (we refuse to invent a denominator we don't have).
	IdealCyclesPerMin float64
	// MaxGap caps how much wall-clock a single reading's state is credited
	// with, so a sensor outage doesn't inflate one bucket. Default 15m.
	MaxGap time.Duration
}

func (p AggregateParams) maxGap() time.Duration {
	if p.MaxGap <= 0 {
		return 15 * time.Minute
	}
	return p.MaxGap
}

// AggregateDay rolls a day's readings (sorted ascending by ts) into one OEE
// summary plus any detected downtime events. Pure: no DB access, fully testable.
func AggregateDay(assetTag string, day time.Time, readings []Reading, p AggregateParams) DailyResult {
	day = day.UTC().Truncate(24 * time.Hour)
	sum := DailySummary{AssetTag: assetTag, Day: day, ReadingCount: len(readings)}
	if len(readings) == 0 {
		return DailyResult{Summary: sum}
	}

	// State minute buckets, attributing each reading's state forward to the
	// next reading (capped at MaxGap).
	stateMinutes := map[string]float64{}
	var tempMax *float64
	var rpmSum float64
	var rpmN int
	for i, r := range readings {
		if r.TemperatureC != nil {
			if tempMax == nil || *r.TemperatureC > *tempMax {
				v := *r.TemperatureC
				tempMax = &v
			}
		}
		if r.SpindleRPM != nil {
			rpmSum += *r.SpindleRPM
			rpmN++
		}
		if i+1 < len(readings) {
			gap := readings[i+1].TS.Sub(r.TS)
			if gap < 0 {
				gap = 0
			}
			if gap > p.maxGap() {
				gap = p.maxGap()
			}
			stateMinutes[NormalizeState(r.State)] += gap.Minutes()
		}
	}

	sum.RunningMinutes = int(stateMinutes[StateRunning] + 0.5)
	sum.IdleMinutes = int(stateMinutes[StateIdle] + 0.5)
	sum.DownMinutes = int(stateMinutes[StateDown] + 0.5)
	sum.SetupMinutes = int(stateMinutes[StateSetup] + 0.5)

	sum.GoodTotal = positiveDelta(readings, func(r Reading) *int64 { return r.GoodCount })
	sum.RejectTotal = positiveDelta(readings, func(r Reading) *int64 { return r.RejectCount })
	sum.CyclesTotal = positiveDelta(readings, func(r Reading) *int64 { return r.CycleCount })

	first := readings[0].TS
	last := readings[len(readings)-1].TS
	sum.FirstReading = &first
	sum.LastReading = &last
	if tempMax != nil {
		sum.MaxTemperatureC = tempMax
	}
	if rpmN > 0 {
		avg := rpmSum / float64(rpmN)
		sum.AvgSpindleRPM = &avg
	}

	// OEE components.
	scheduled := stateMinutes[StateRunning] + stateMinutes[StateDown] + stateMinutes[StateSetup]
	if scheduled > 0 {
		a := stateMinutes[StateRunning] / scheduled
		sum.Availability = &a
	}
	producedUnits := sum.GoodTotal + sum.RejectTotal
	if producedUnits > 0 {
		q := float64(sum.GoodTotal) / float64(producedUnits)
		sum.Quality = &q
	}
	if p.IdealCyclesPerMin > 0 && stateMinutes[StateRunning] > 0 {
		idealCycles := p.IdealCyclesPerMin * stateMinutes[StateRunning]
		perf := float64(sum.CyclesTotal) / idealCycles
		if perf > 1 {
			perf = 1 // clamp: counters can outrun a conservative nameplate
		}
		sum.Performance = &perf
	}
	if sum.Availability != nil && sum.Performance != nil && sum.Quality != nil {
		oee := *sum.Availability * *sum.Performance * *sum.Quality
		sum.OEE = &oee
	}

	return DailyResult{Summary: sum, DowntimeEvents: detectDowntime(assetTag, readings, p.maxGap())}
}

// detectDowntime emits one event per contiguous run of "down" readings.
func detectDowntime(assetTag string, readings []Reading, maxGap time.Duration) []DowntimeEvent {
	var out []DowntimeEvent
	i := 0
	for i < len(readings) {
		if NormalizeState(readings[i].State) != StateDown {
			i++
			continue
		}
		start := readings[i].TS
		fault := ""
		n := 0
		j := i
		for j < len(readings) && NormalizeState(readings[j].State) == StateDown {
			if fault == "" && readings[j].FaultCode != nil && *readings[j].FaultCode != "" {
				fault = *readings[j].FaultCode
			}
			n++
			j++
		}
		// End at the first non-down reading, else cap the last down reading by maxGap.
		var end time.Time
		if j < len(readings) {
			end = readings[j].TS
		} else {
			end = readings[j-1].TS.Add(maxGap)
		}
		dur := end.Sub(start).Minutes()
		if dur >= 1 {
			endCopy := end
			out = append(out, DowntimeEvent{
				AssetTag:    assetTag,
				StartedAt:   start,
				EndedAt:     &endCopy,
				DurationMin: dur,
				FaultCode:   fault,
				Reason:      "unplanned stop",
				Confidence:  downtimeConfidence(n),
			})
		}
		i = j
	}
	return out
}

func downtimeConfidence(readingsInRun int) string {
	switch {
	case readingsInRun >= 3:
		return "high"
	case readingsInRun == 2:
		return "medium"
	default:
		return "low"
	}
}

// positiveDelta sums the increases of a cumulative counter across the series,
// treating any decrease as a device-side counter reset (count from 0 again).
func positiveDelta(readings []Reading, pick func(Reading) *int64) int64 {
	var total int64
	var prev *int64
	for _, r := range readings {
		cur := pick(r)
		if cur == nil {
			continue
		}
		if prev == nil {
			prev = cur
			continue
		}
		if *cur >= *prev {
			total += *cur - *prev
		} else {
			total += *cur // reset: the post-reset value is itself new production
		}
		prev = cur
	}
	return total
}
