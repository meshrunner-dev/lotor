package meshcore

import "meshrunner.dev/lotor/internal/meshcorecfg"

// presets are the band profiles this protocol ships with.
// The values are MeshCore network agreements: a relay must match its
// mesh exactly or hear nothing.
var presets = meshcorecfg.Presets()
