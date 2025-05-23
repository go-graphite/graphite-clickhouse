package load_avg

import (
	"fmt"
	"testing"
)

func TestWeight(t *testing.T) {
	tests := []struct {
		weight          int
		degraged        float64
		degragedLoadAvg float64
		loadAvg         float64
		want            int64
	}{
		// weight : 100
		{weight: 100, loadAvg: 0, want: 200},
		{weight: 100, loadAvg: 0.1, want: 199},
		{weight: 100, loadAvg: 0.11, want: 199},
		{weight: 100, loadAvg: 0.2, want: 169},
		{weight: 100, loadAvg: 0.5, want: 130},
		{weight: 100, loadAvg: 0.9, want: 104},
		{weight: 100, loadAvg: 1, want: 100},
		{weight: 100, loadAvg: 1.1, want: 36},
		{weight: 100, loadAvg: 1.9, want: 12},
		{weight: 100, loadAvg: 2, want: 1},
		{weight: 100, loadAvg: 9, want: 1},
		{weight: 100, loadAvg: 10, want: 1},
		{weight: 100, loadAvg: 20, want: 1},
		// weight : 1000
		{weight: 1000, loadAvg: 0, want: 2000},
		{weight: 1000, loadAvg: 0.1, want: 1999},
		{weight: 1000, loadAvg: 0.11, want: 1999},
		{weight: 1000, loadAvg: 0.2, want: 1698},
		{weight: 1000, loadAvg: 0.5, want: 1301},
		{weight: 1000, loadAvg: 0.9, want: 1045},
		{weight: 1000, loadAvg: 1, want: 1000},
		{weight: 1000, loadAvg: 1.1, want: 357},
		{weight: 1000, loadAvg: 1.9, want: 120},
		{weight: 1000, loadAvg: 2, want: 1},
		{weight: 1000, loadAvg: 3, want: 1},
		{weight: 1000, loadAvg: 4, want: 1},
		{weight: 1000, loadAvg: 9, want: 1},
		{weight: 1000, loadAvg: 10, want: 1},
		{weight: 1000, loadAvg: 20, want: 1},
		// weight : 100, aggressive: 4, degragedLoadAvg: 0.8
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 0, want: 200},
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 0.8, want: 109},
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 0.81, want: 50},
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 0.9, want: 45},
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 1, want: 40},
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 1.1, want: 36},
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 1.9, want: 12},
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 2, want: 1},
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 3, want: 1},
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 4, want: 1},
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 9, want: 1},
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 10, want: 1},
		{weight: 100, degragedLoadAvg: 0.8, loadAvg: 20, want: 1},
		// weight : 1000, degraged: 8, degragedLoadAvg: 0.8
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 0, want: 2000},
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 0.8, want: 1096},
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 0.81, want: 188},
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 0.9, want: 143},
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 1, want: 97},
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 1.2, want: 18},
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 1.3, want: 1},
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 2, want: 1},
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 3, want: 1},
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 4, want: 1},
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 9, want: 1},
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 10, want: 1},
		{weight: 1000, degraged: 8, degragedLoadAvg: 0.8, loadAvg: 20, want: 1},
	}
	for _, tt := range tests {
		if tt.degraged == 0 {
			tt.degraged = 4 // default
		}

		if tt.degragedLoadAvg == 0 {
			tt.degragedLoadAvg = 1.0 // default
		}

		t.Run(fmt.Sprintf("%d#%f#%f#%f", tt.weight, tt.degraged, tt.degragedLoadAvg, tt.loadAvg), func(t *testing.T) {
			if got := Weight(tt.weight, tt.degraged, tt.degragedLoadAvg, tt.loadAvg); got != tt.want {
				t.Errorf("Weight(%d, %f, %f, %f) = %v, want %v", tt.weight, tt.degraged, tt.degragedLoadAvg, tt.loadAvg, got, tt.want)
			}
		})
	}
}
