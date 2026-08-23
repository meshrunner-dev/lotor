package meshcore

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"
)

// loadOrCreateIdentity returns the relay's node identity, generating
// and persisting a fresh seed on first run. The file holds the
// 32-byte seed as one hex line, mode 0600: it IS the private key —
// whoever holds it is this node.
func loadOrCreateIdentity(path string, log *zap.Logger) (*meshcore.LocalIdentity, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // the operator's own identity_file setting
	switch {
	case err == nil:
		seed, err := hex.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil || len(seed) != meshcore.SeedSize {
			return nil, fmt.Errorf("identity file %s: want %d hex-encoded seed bytes",
				path, meshcore.SeedSize)
		}
		return meshcore.LocalIdentityFromSeed(seed)
	case os.IsNotExist(err):
		seed := make([]byte, meshcore.SeedSize)
		if _, err := rand.Read(seed); err != nil {
			return nil, err
		}
		id, err := meshcore.LocalIdentityFromSeed(seed)
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, []byte(hex.EncodeToString(seed)+"\n"), 0o600); err != nil {
			return nil, err
		}
		log.Info("node identity created",
			zap.String("file", path),
			zap.String("pubkey", hex.EncodeToString(id.PubKey[:])[:keyPrefixLen]))
		return id, nil
	default:
		return nil, err
	}
}
