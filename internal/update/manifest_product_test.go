package update

import (
	"strings"
	"testing"
	"time"
)

func TestAManifestForAnotherProductIsRefused(t *testing.T) {
	// A valid signature proves who published, not that the bytes are
	// ours to run: a manifest naming another product — a shared key,
	// a misrouted URL — fails by name before anything downloads.
	raw := []byte(`{"product":"otterd","channel":"dev","version":"1.0.0",` +
		`"published":"` + time.Now().UTC().Format(time.RFC3339) + `",` +
		`"artifacts":{"linux/arm64":{"url":"https://x/y.gz","sha256":"` +
		strings.Repeat("a", 64) + `","size":1}}}`)
	if _, err := ParseManifest(raw); err == nil ||
		!strings.Contains(err.Error(), `for product "otterd"`) {
		t.Errorf("foreign manifest = %v", err)
	}
}
