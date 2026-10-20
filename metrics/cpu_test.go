package metrics

import (
	"math"
	"testing"
)

func TestCPUUsageDoesNotDoubleCountGuests(t *testing.T) {
	for _, tc := range []struct {
		name, before, after string
		want                float64
	}{
		{"guest", "cpu 100 0 0 100 0 0 0 0 0 0", "cpu 150 0 0 150 0 0 0 0 50 0", 50},
		{"nice guest", "cpu 0 100 0 100 0 0 0 0 0 0", "cpu 0 150 0 150 0 0 0 0 0 50", 50},
		{"normal", "cpu 100 0 50 100 0 0 0 0", "cpu 110 0 60 180 0 0 0 0", 20},
		{"older kernel", "cpu 100 0 50 100", "cpu 110 0 60 180", 20},
		{"busy", "cpu 0 0 0 100", "cpu 100 0 0 100", 100},
		{"idle", "cpu 100 0 0 100", "cpu 100 0 0 200", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idle1, total1, ok1 := parseCPUStat([]byte(tc.before + "\ncpu0 999 999 999 999\n"))
			idle2, total2, ok2 := parseCPUStat([]byte(tc.after))
			if !ok1 || !ok2 {
				t.Fatal("valid CPU counters rejected")
			}
			if got := cpuUsageBetween(idle1, total1, idle2, total2); math.Abs(got-tc.want) > 0.000001 {
				t.Fatalf("CPU usage = %g, want %g", got, tc.want)
			}
		})
	}
}

func TestCPUStatRejectsInvalidCounters(t *testing.T) {
	for _, input := range []string{"", "cpu0 1 2 3 4", "cpu 1 2 3", "cpu 1 invalid 3 4", "cpu -1 2 3 4", "cpu 18446744073709551615 1 0 0"} {
		if _, _, ok := parseCPUStat([]byte(input)); ok {
			t.Fatalf("accepted invalid counters %q", input)
		}
	}
	for _, counters := range [][4]uint64{{10, 20, 10, 20}, {10, 20, 5, 30}, {10, 20, 11, 19}, {10, 20, 30, 25}} {
		if got := cpuUsageBetween(counters[0], counters[1], counters[2], counters[3]); got != 0 {
			t.Fatalf("invalid counter progress %v produced CPU usage %g", counters, got)
		}
	}
}
