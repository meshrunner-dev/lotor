package meshcorecfg

import (
	"encoding/hex"
	"fmt"
	"strings"

	"meshrunner.dev/pkg/meshcore"
)

// Identity parses the common inline identity value: a seed, an expanded
// private key, or an expanded private/public pair, all in hex.
func Identity(value string) (*meshcore.LocalIdentity, error) {
	raw, err := hex.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("identity: want hex: %w", err)
	}
	switch len(raw) {
	case meshcore.SeedSize:
		return meshcore.LocalIdentityFromSeed(raw)
	case meshcore.PrvKeySize:
		return meshcore.LocalIdentityFromKeys(raw, nil)
	case meshcore.PrvKeySize + meshcore.PubKeySize:
		return meshcore.LocalIdentityFromKeys(raw[:meshcore.PrvKeySize], raw[meshcore.PrvKeySize:])
	default:
		return nil, fmt.Errorf(
			"identity: %d bytes — want a %d-byte seed, a %d-byte private key, or a %d-byte key pair",
			len(raw), meshcore.SeedSize, meshcore.PrvKeySize,
			meshcore.PrvKeySize+meshcore.PubKeySize)
	}
}
