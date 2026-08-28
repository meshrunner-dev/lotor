package mqtt

// The ecosystem's device authentication: a JWT-shaped token signed by
// the node's own ed25519 identity. Shaped, not standard — the third
// segment is the signature in uppercase hex, not base64url, and the
// broker side expects exactly that. The claims carry the public key
// in uppercase hex under "publicKey", the audience, and the validity
// window; the username beside it is "v1_" and the same uppercase key.

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// jwtDefaultTTL is the token lifetime when the broker names none.
const jwtDefaultTTL = 24 * time.Hour

// Signer produces the node's ed25519 signature over a message — the
// engine's identity, reached through a closure so the key itself
// never travels.
type Signer func(message []byte) ([]byte, error)

// AuthToken builds the token a JWT broker takes as the password.
// owner, when set, rides as the optional claim the platforms use to
// tie the feed to an operator.
func AuthToken(pubKeyHex, audience, owner string, ttl time.Duration, now time.Time, sign Signer) (string, error) {
	if audience == "" {
		return "", errors.New("a token needs an audience")
	}
	if ttl <= 0 {
		ttl = jwtDefaultTTL
	}
	header, err := json.Marshal(struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}{"Ed25519", "JWT"})
	if err != nil {
		return "", err
	}
	iat := now.Unix()
	payload, err := json.Marshal(struct {
		PublicKey string `json:"publicKey"`
		Aud       string `json:"aud"`
		Iat       int64  `json:"iat"`
		Exp       int64  `json:"exp"`
		Owner     string `json:"owner,omitempty"`
	}{strings.ToUpper(pubKeyHex), audience, iat, iat + int64(ttl.Seconds()), owner})
	if err != nil {
		return "", err
	}
	b64 := base64.RawURLEncoding
	signing := b64.EncodeToString(header) + "." + b64.EncodeToString(payload)
	sig, err := sign([]byte(signing))
	if err != nil {
		return "", err
	}
	return signing + "." + strings.ToUpper(hex.EncodeToString(sig)), nil
}

// JWTUsername is the fixed shape a JWT broker matches the token's key
// against.
func JWTUsername(pubKeyHex string) string {
	return "v1_" + strings.ToUpper(pubKeyHex)
}
