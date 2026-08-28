package update

// Where the trusted keys come from: the binary itself, and a
// root-owned directory. The embedded list is how an official rotation
// reaches the fleet — a release that trusts old and new at once, then
// a signing switch, then a later release that drops the old — with no
// operator doing anything. The directory is the operator's own say:
// a fork's key beside the official ones, deposited by root, which is
// what makes the signature a privilege boundary and not merely a
// transport check.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// officialKeys is the project's own signing keys, newest first,
// rendered exactly as their .pub files read — the channel pin rides
// each comment line, and the two trains never share a key: the
// stable one signs release, rc and beta behind a protected
// environment, the fast one signs dev and try-* on every push. Empty
// until the first keys are minted; the workflows refuse to publish
// while it is.
var officialKeys = []string{
	// The stable train: what a production relay follows. Signed from
	// the protected environment, tags only, a human approving.
	"untrusted comment: minisign public key EB9E324264F759A8; channels: release rc beta\n" +
		"RWTrnjJCZPdZqDU4KGmFPcezPrYj+7+pTb1F+CJZQY21lJEaxLdk0iFT\n",
	// The fast train: every push to main, every manual build. Hot by
	// design, and pinned so it can never speak for the stable train.
	"untrusted comment: minisign public key 1B2B9D744447E47F; channels: dev try-*\n" +
		"RWQbK510REfkf5h/5S6NkBNJWSIEQ43DvFnI6SWlw9IObb4kT3Cx08mW\n",
}

// TrustedKeysDir is where an operator deposits additional public
// keys — a fork's, typically. Root-owned on purpose.
const TrustedKeysDir = "/etc/lotor/trusted-keys"

// Trusted assembles the verification set: embedded official keys plus
// every .pub in dir. A dir that does not exist contributes nothing; a
// key that does not parse is an error, not a skip — a trust store
// that silently ignores a malformed key is one an operator cannot
// reason about.
func Trusted(dir string) ([]PublicKey, error) {
	keys := make([]PublicKey, 0, len(officialKeys))
	for i, text := range officialKeys {
		k, err := ParsePublicKey([]byte(text))
		if err != nil {
			return nil, fmt.Errorf("embedded key %d: %w", i, err)
		}
		keys = append(keys, k)
	}
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return keys, nil
	}
	if err != nil {
		return nil, fmt.Errorf("trusted keys: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".pub") {
			continue
		}
		//nolint:gosec // the operator's own trust store, walked by listing
		text, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("trusted key %s: %w", e.Name(), err)
		}
		k, err := ParsePublicKey(text)
		if err != nil {
			return nil, fmt.Errorf("trusted key %s: %w", e.Name(), err)
		}
		keys = append(keys, k)
	}
	return keys, nil
}
