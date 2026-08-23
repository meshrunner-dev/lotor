//go:build !lean

package relay

// NoiseHistoryDefault is the build's answer when the configuration
// does not say whether to archive a relay's noise floor. The normal
// build archives: an unattended site's history is its value.
const NoiseHistoryDefault = true
