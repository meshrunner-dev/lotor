package meshcore

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// hostTempPath is where a Linux kernel puts the first thermal zone.
const hostTempPath = "/sys/class/thermal/thermal_zone0/temp"

// hostTempTTL bounds how often the sensor is actually read. The read
// itself happens on the goroutine that also drives the radio, so a
// companion asking for telemetry in a loop must not turn into a loop
// of filesystem syscalls between two receptions.
const hostTempTTL = 30 * time.Second

var hostTemp struct {
	mu    sync.Mutex
	at    time.Time
	value float64
	ok    bool
}

// hostTemperature reports the host's own thermal sensor — the one
// figure a mains-powered relay can honestly report about itself.
// Absent on hosts without one, which is not a fault.
func hostTemperature() (float64, bool) {
	hostTemp.mu.Lock()
	defer hostTemp.mu.Unlock()
	if time.Since(hostTemp.at) < hostTempTTL {
		return hostTemp.value, hostTemp.ok
	}
	hostTemp.at = time.Now()
	hostTemp.value, hostTemp.ok = readHostTemperature()
	return hostTemp.value, hostTemp.ok
}

func readHostTemperature() (float64, bool) {
	raw, err := os.ReadFile(hostTempPath)
	if err != nil {
		return 0, false
	}
	milli, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0, false
	}
	return float64(milli) / 1000, true
}
