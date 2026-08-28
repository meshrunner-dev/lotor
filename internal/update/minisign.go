package update

// Minisign-compatible signatures, both halves. lotor only ever
// verifies; the signing half lives here too because cmd/relsign is a
// thin shell around it and the two must agree byte for byte.
//
// The format, as stock minisign writes it. A public key file is two
// lines: an untrusted comment, then base64 of "Ed" ‖ keyID[8] ‖
// pubkey[32]. A signature file is four: an untrusted comment, base64
// of alg[2] ‖ keyID[8] ‖ sig[64], a trusted comment, and base64 of a
// global signature — ed25519 over sig ‖ trusted-comment text — which
// binds the comment to the signature it rides with. alg "ED" signs
// BLAKE2b-512 of the content (the modern default, and what relsign
// writes); "Eb" era pure-ed25519 "Ed" is verified too, for manifests
// signed by older tooling.

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/blake2b"
)

const (
	keyIDSize = 8

	algPrehashed = "ED"
	algPure      = "Ed"

	untrustedPrefix = "untrusted comment: "
	trustedPrefix   = "trusted comment: "
)

// PublicKey is one key manifests may be signed under.
type PublicKey struct {
	ID  [keyIDSize]byte
	Key ed25519.PublicKey
}

// Hex names the key the way operators and file names refer to it.
func (p PublicKey) Hex() string { return fmt.Sprintf("%x", p.ID) }

// ParsePublicKey reads a minisign public key file.
func ParsePublicKey(text []byte) (PublicKey, error) {
	var p PublicKey
	raw, err := decodeAfterComment(text, untrustedPrefix)
	if err != nil {
		return p, fmt.Errorf("public key: %w", err)
	}
	if len(raw) != 2+keyIDSize+ed25519.PublicKeySize || string(raw[:2]) != algPure {
		return p, errors.New("public key: not an ed25519 minisign key")
	}
	copy(p.ID[:], raw[2:2+keyIDSize])
	p.Key = ed25519.PublicKey(raw[2+keyIDSize:])
	return p, nil
}

// signature is one parsed .minisig file.
type signature struct {
	alg       string
	keyID     [keyIDSize]byte
	sig       []byte
	trusted   string
	globalSig []byte
}

func parseSignature(text []byte) (*signature, error) {
	lines := strings.Split(strings.TrimSpace(string(text)), "\n")
	if len(lines) != 4 {
		return nil, errors.New("signature: want 4 lines")
	}
	raw, err := decodeAfterComment([]byte(lines[0]+"\n"+lines[1]), untrustedPrefix)
	if err != nil {
		return nil, fmt.Errorf("signature: %w", err)
	}
	if len(raw) != 2+keyIDSize+ed25519.SignatureSize {
		return nil, errors.New("signature: wrong length")
	}
	s := &signature{alg: string(raw[:2]), sig: raw[2+keyIDSize:]}
	copy(s.keyID[:], raw[2:2+keyIDSize])
	if s.alg != algPrehashed && s.alg != algPure {
		return nil, fmt.Errorf("signature: unknown algorithm %q", s.alg)
	}
	if !strings.HasPrefix(lines[2], trustedPrefix) {
		return nil, errors.New("signature: missing trusted comment")
	}
	s.trusted = strings.TrimPrefix(lines[2], trustedPrefix)
	if s.globalSig, err = base64.StdEncoding.DecodeString(lines[3]); err != nil ||
		len(s.globalSig) != ed25519.SignatureSize {
		return nil, errors.New("signature: bad global signature")
	}
	return s, nil
}

// Verify proves content was signed under one of the trusted keys, and
// returns which. The set is the point: rollover trusts old and new at
// once, and a fork's key sits beside the official ones.
func Verify(content, sigText []byte, trusted []PublicKey) (PublicKey, error) {
	s, err := parseSignature(sigText)
	if err != nil {
		return PublicKey{}, err
	}
	for _, k := range trusted {
		if k.ID != s.keyID {
			continue
		}
		msg := content
		if s.alg == algPrehashed {
			h := blake2b.Sum512(content)
			msg = h[:]
		}
		if !ed25519.Verify(k.Key, msg, s.sig) {
			return PublicKey{}, fmt.Errorf("signature under key %s does not verify", k.Hex())
		}
		// The global signature binds the trusted comment to the
		// signature it rides with: a comment nobody signed is a
		// comment anybody could have edited.
		if !ed25519.Verify(k.Key, append(append([]byte(nil), s.sig...), []byte(s.trusted)...), s.globalSig) {
			return PublicKey{}, fmt.Errorf("trusted comment under key %s does not verify", k.Hex())
		}
		return k, nil
	}
	return PublicKey{}, fmt.Errorf("signed under key %x, which nothing here trusts", s.keyID)
}

// SecretKey is the signing half, relsign's alone. Its file form is
// two lines like the public one — not stock minisign's encrypted
// container, because the key lives in a CI secret where the store's
// own encryption is the envelope.
type SecretKey struct {
	ID  [keyIDSize]byte
	Key ed25519.PrivateKey
}

// GenerateKey mints a fresh signing pair with a random key id.
func GenerateKey() (SecretKey, PublicKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return SecretKey{}, PublicKey{}, err
	}
	var id [keyIDSize]byte
	if _, err := rand.Read(id[:]); err != nil {
		return SecretKey{}, PublicKey{}, err
	}
	return SecretKey{ID: id, Key: priv}, PublicKey{ID: id, Key: pub}, nil
}

// MarshalPublic renders the key as a minisign public key file.
func MarshalPublic(p PublicKey) []byte {
	raw := append(append([]byte(algPure), p.ID[:]...), p.Key...)
	return []byte(untrustedPrefix + "minisign public key " + strings.ToUpper(p.Hex()) + "\n" +
		base64.StdEncoding.EncodeToString(raw) + "\n")
}

// MarshalSecret renders the signing key for the CI secret.
func MarshalSecret(s SecretKey) []byte {
	raw := append(append([]byte(algPure), s.ID[:]...), s.Key...)
	return []byte(untrustedPrefix + "relsign secret key " + fmt.Sprintf("%x", s.ID) + "\n" +
		base64.StdEncoding.EncodeToString(raw) + "\n")
}

// ParseSecret reads what MarshalSecret wrote.
func ParseSecret(text []byte) (SecretKey, error) {
	var s SecretKey
	raw, err := decodeAfterComment(text, untrustedPrefix)
	if err != nil {
		return s, fmt.Errorf("secret key: %w", err)
	}
	if len(raw) != 2+keyIDSize+ed25519.PrivateKeySize || string(raw[:2]) != algPure {
		return s, errors.New("secret key: not a relsign key")
	}
	copy(s.ID[:], raw[2:2+keyIDSize])
	s.Key = ed25519.PrivateKey(raw[2+keyIDSize:])
	return s, nil
}

// Sign produces a .minisig for the content, prehashed the way stock
// minisign does it, so anyone can check the file by hand.
func Sign(content []byte, key SecretKey, trustedComment string) []byte {
	h := blake2b.Sum512(content)
	sig := ed25519.Sign(key.Key, h[:])
	global := ed25519.Sign(key.Key, append(append([]byte(nil), sig...), []byte(trustedComment)...))
	raw := append(append([]byte(algPrehashed), key.ID[:]...), sig...)
	var b bytes.Buffer
	b.WriteString(untrustedPrefix + "signature from relsign secret key\n")
	b.WriteString(base64.StdEncoding.EncodeToString(raw) + "\n")
	b.WriteString(trustedPrefix + trustedComment + "\n")
	b.WriteString(base64.StdEncoding.EncodeToString(global) + "\n")
	return b.Bytes()
}

// decodeAfterComment reads the base64 payload of a two-line keyfile,
// tolerating trailing lines.
func decodeAfterComment(text []byte, prefix string) ([]byte, error) {
	lines := strings.SplitN(strings.TrimSpace(string(text)), "\n", 3)
	if len(lines) < 2 || !strings.HasPrefix(lines[0], prefix) {
		return nil, errors.New("want a comment line then base64")
	}
	return base64.StdEncoding.DecodeString(strings.TrimSpace(lines[1]))
}
