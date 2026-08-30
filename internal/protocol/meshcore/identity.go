package meshcore

import (
	"meshrunner.dev/lotor/internal/meshcorecfg"

	"meshrunner.dev/pkg/meshcore"
)

// identityFromConfig parses the relay's inline identity value: hex,
// format told by length. It IS the private key — the config file
// carrying it deserves the same care as the node.
//
//   - 32 bytes: a seed, expanded exactly as the reference expands it.
//   - 64 bytes: an expanded private key — what the reference CLI's
//     prv.key command speaks, the migration path from an existing
//     MeshCore node (a seed cannot be recovered from it). The public
//     key is derived the way the firmware derives it.
//   - 96 bytes: an expanded private key with its public key appended;
//     the pair is verified to agree.
func identityFromConfig(value string) (*meshcore.LocalIdentity, error) {
	return meshcorecfg.Identity(value)
}
