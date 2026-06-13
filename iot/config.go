package iot

import (
	"os"
	"strconv"
)

// MaxTrackRowLimit caps how many readings a single Track query may return,
// overridable via MAX_TRACK_ROWS. Guards the timeseries DB against an
// unbounded range scan from a misbehaving client.
func MaxTrackRowLimit() int {
	if v := os.Getenv("MAX_TRACK_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 50000
}

// newestReading returns the reading with the latest TS, or nil for an empty slice.
func newestReading(readings []Reading) *Reading {
	if len(readings) == 0 {
		return nil
	}
	best := readings[0]
	for _, r := range readings[1:] {
		if r.TS.After(best.TS) {
			best = r
		}
	}
	return &best
}
