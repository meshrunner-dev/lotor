package update

// Fetching a channel: the manifest, its signature, and the proof,
// in one call. This is the only file in the package that touches the
// network, and it never touches the filesystem — the trust store is
// handed in, and what comes back is already verified or is an error.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// fetchLimit bounds what a manifest may weigh. A channel statement is
// a page of JSON; anything heavier is not one.
const fetchLimit = 1 << 20

// Client reads channels from one update host.
type Client struct {
	// Base is the manifest tree, https://updates.meshrunner.dev/lotor
	// by default; Token rides as a bearer when the host wants one — a
	// private fork's assets, typically.
	Base  string
	Token string
	// Trusted is the verification set; empty refuses everything,
	// because a channel nobody vouches for is not a channel.
	Trusted []PublicKey
	// HTTP serves the requests; nil takes a client with sane bounds.
	HTTP *http.Client
	// Progress, when set, is told how much of an artifact has arrived
	// as it arrives. It runs on the fetching goroutine, once per
	// chunk, so it must not block — the pacing of what an operator
	// sees belongs to whoever renders it, not to the download.
	Progress func(done, total int64)
}

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 30 * time.Second}
}

// Checked is one verified channel statement: the manifest, the key
// that vouched for it, and the validator for the next cheap check.
type Checked struct {
	Manifest *Manifest
	Key      PublicKey
	ETag     string
	// Raw and Sig are the exact bytes that verified: what a stage
	// carries along so the privileged installer can re-verify them
	// against its own trust store before touching anything.
	Raw []byte
	Sig []byte
}

// ErrUnchanged says the channel has not moved since the last check —
// the 304 an If-None-Match earns, which is what makes a periodic
// check cost a handshake and nothing more.
var ErrUnchanged = errors.New("channel unchanged")

// Check fetches and verifies one channel. etag carries the previous
// check's validator; empty asks unconditionally.
func (c *Client) Check(ctx context.Context, channel, etag string) (*Checked, error) {
	if len(c.Trusted) == 0 {
		return nil, errors.New("no trusted keys — nothing this client could believe")
	}
	base := strings.TrimSuffix(c.Base, "/")
	raw, gotETag, err := c.get(ctx, base+"/"+channel+"/manifest.json", etag)
	if err != nil {
		return nil, err
	}
	sig, err := c.signatureFor(ctx, base, channel)
	if err != nil {
		return nil, err
	}
	key, err := Verify(raw, sig, c.Trusted)
	if err != nil {
		return nil, err
	}
	m, err := ParseManifest(raw)
	if err != nil {
		return nil, err
	}
	if m.Channel != channel {
		return nil, fmt.Errorf("asked for channel %s, the manifest says %s", channel, m.Channel)
	}
	// The pin, enforced where it counts: a key vouches for the trains
	// it was pinned to and no others, so the fast channels' hot key
	// can never speak for a stable one.
	if !key.Vouches(channel) {
		return nil, fmt.Errorf("key %s does not vouch for channel %s", key.Hex(), channel)
	}
	return &Checked{Manifest: m, Key: key, ETag: gotETag, Raw: raw, Sig: sig}, nil
}

// signatureFor finds a signature one of our keys can check: the
// primary file first, then the per-key files a rollover overlap lays
// beside it, so a relay that only trusts the outgoing key still
// verifies while the fleet catches up.
func (c *Client) signatureFor(ctx context.Context, base, channel string) ([]byte, error) {
	sig, _, err := c.get(ctx, base+"/"+channel+"/manifest.json.minisig", "")
	if err == nil {
		if s, perr := parseSignature(sig); perr == nil {
			for _, k := range c.Trusted {
				if k.ID == s.keyID {
					return sig, nil
				}
			}
		}
	}
	for _, k := range c.Trusted {
		alt, _, aerr := c.get(ctx, base+"/"+channel+"/manifest.json."+k.Hex()+".minisig", "")
		if aerr == nil {
			return alt, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("channel %s carries no signature: %w", channel, err)
	}
	return sig, nil // signed under a key we do not trust: let Verify say so
}

// get is one bounded request. A 304 against the caller's validator
// surfaces as ErrUnchanged.
func (c *Client) get(ctx context.Context, url, etag string) (body []byte, gotETag string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.http().Do(req)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusNotModified:
		return nil, "", ErrUnchanged
	case resp.StatusCode == http.StatusNotFound:
		return nil, "", fmt.Errorf("%s: not published", url)
	case resp.StatusCode != http.StatusOK:
		return nil, "", fmt.Errorf("%s: %s", url, resp.Status)
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, fetchLimit+1))
	if err != nil {
		return nil, "", err
	}
	if len(body) > fetchLimit {
		return nil, "", fmt.Errorf("%s: heavier than any channel statement", url)
	}
	return body, resp.Header.Get("ETag"), nil
}
