package update

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func pair(t *testing.T) (SecretKey, PublicKey) {
	t.Helper()
	sec, pub, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return sec, pub
}

func TestSignedBytesVerifyAndTamperedOnesDoNot(t *testing.T) {
	sec, pub := pair(t)
	content := []byte(`{"a manifest":"in spirit"}`)
	sig := Sign(content, sec, "channel:release")

	key, err := Verify(content, sig, []PublicKey{pub})
	if err != nil || key.ID != pub.ID {
		t.Fatalf("Verify = %v, key %x", err, key.ID)
	}
	// One flipped byte in the content, refused.
	bent := append([]byte(nil), content...)
	bent[3] ^= 1
	if _, err := Verify(bent, sig, []PublicKey{pub}); err == nil {
		t.Error("tampered content verified")
	}
	// An edited trusted comment, refused: the global signature binds
	// it to the signature it rides with.
	edited := strings.Replace(string(sig), "channel:release", "channel:evil!!", 1)
	if _, err := Verify(content, []byte(edited), []PublicKey{pub}); err == nil {
		t.Error("an edited trusted comment verified")
	}
	// A key nobody trusts, named as such.
	_, other := pair(t)
	if _, err := Verify(content, sig, []PublicKey{other}); err == nil ||
		!strings.Contains(err.Error(), "nothing here trusts") {
		t.Errorf("untrusted key: %v", err)
	}
}

func TestRolloverIsAUnionOfKeys(t *testing.T) {
	// The whole mechanism: a relay trusting old and new verifies
	// manifests signed under either, so the signing side can switch
	// whenever it likes and nobody in the fleet acts at all.
	oldSec, oldPub := pair(t)
	newSec, newPub := pair(t)
	both := []PublicKey{oldPub, newPub}
	content := []byte("the same manifest, two eras")
	if k, err := Verify(content, Sign(content, oldSec, "era:1"), both); err != nil || k.ID != oldPub.ID {
		t.Fatalf("old era: %v", err)
	}
	if k, err := Verify(content, Sign(content, newSec, "era:2"), both); err != nil || k.ID != newPub.ID {
		t.Fatalf("new era: %v", err)
	}
}

func TestKeyFilesRoundTrip(t *testing.T) {
	sec, pub := pair(t)
	pub2, err := ParsePublicKey(MarshalPublic(pub))
	if err != nil || pub2.ID != pub.ID || !pub2.Key.Equal(pub.Key) {
		t.Fatalf("public round trip: %v", err)
	}
	sec2, err := ParseSecret(MarshalSecret(sec))
	if err != nil || sec2.ID != sec.ID || !sec2.Key.Equal(sec.Key) {
		t.Fatalf("secret round trip: %v", err)
	}
}

func TestTrustedReadsTheOperatorsDirectory(t *testing.T) {
	dir := t.TempDir()
	_, pub := pair(t)
	if err := os.WriteFile(filepath.Join(dir, "fork.pub"), MarshalPublic(pub), 0o644); err != nil {
		t.Fatal(err)
	}
	// Only .pub files are keys; a README beside them is not.
	if err := os.WriteFile(filepath.Join(dir, "README"), []byte("not a key"), 0o644); err != nil {
		t.Fatal(err)
	}
	keys, err := Trusted(dir)
	if err != nil || len(keys) != len(officialKeys)+1 ||
		keys[len(keys)-1].ID != pub.ID {
		t.Fatalf("Trusted = %d keys, %v — want the official set plus the fork's", len(keys), err)
	}
	// A directory that does not exist contributes nothing and fails
	// nothing: most relays never deposit a key.
	if keys, err := Trusted(filepath.Join(dir, "absent")); err != nil || len(keys) != len(officialKeys) {
		t.Fatalf("absent dir: %d keys, %v", len(keys), err)
	}
	// A malformed key is an error, never a silent skip.
	if err := os.WriteFile(filepath.Join(dir, "bent.pub"), []byte("untrusted comment: x\nzzz\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Trusted(dir); err == nil {
		t.Error("a malformed key was skipped in silence")
	}
}

const goodManifest = `{
  "product": "lotor", "channel": "release", "version": "1.4.2",
  "published": "2026-08-28T10:00:00Z",
  "artifacts": {
    "linux/arm64": {"url": "https://example.org/lotor", "sha256": "` + zeroes64 + `", "size": 1}
  }
}`

const zeroes64 = "0000000000000000000000000000000000000000000000000000000000000000"

func TestManifestRefusesWhatItCannotStand(t *testing.T) {
	if _, err := ParseManifest([]byte(goodManifest)); err != nil {
		t.Fatalf("the good manifest was refused: %v", err)
	}
	for _, c := range []struct{ name, from, to string }{
		{"no version", `"version": "1.4.2",`, ""},
		{"http url", "https://example.org", "http://example.org"},
		{"short sha", zeroes64, "00ff"},
		{"zero size", `"size": 1`, `"size": 0`},
		{"unknown field", `"product"`, `"produce"`},
	} {
		bent := strings.Replace(goodManifest, c.from, c.to, 1)
		if _, err := ParseManifest([]byte(bent)); err == nil {
			t.Errorf("%s was accepted", c.name)
		}
	}
	m, _ := ParseManifest([]byte(goodManifest))
	if _, err := m.ArtifactFor("linux/arm64"); err != nil {
		t.Errorf("the built platform was refused: %v", err)
	}
	if _, err := m.ArtifactFor("plan9/386"); err == nil ||
		!strings.Contains(err.Error(), "linux/arm64") {
		t.Errorf("the refusal does not name what is built: %v", err)
	}
}

func TestNewerSpeaksSemverOnStableChannelsAndTimeOnFastOnes(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want int
	}{
		{"1.4.2", "1.4.2", 0},
		{"v1.4.2", "1.4.2", 0},
		{"1.4.10", "1.4.9", 1},
		{"1.4.2", "2.0.0", -1},
		{"1.4.2-rc.1", "1.4.2", -1},
		{"1.4.2-rc.2", "1.4.2-rc.1", 1},
		{"1.4.2-rc.10", "1.4.2-rc.9", 1},
		{"1.4.2-beta.1", "1.4.2-rc.1", -1},
		{"1.4.2-rc.1.extra", "1.4.2-rc.1", 1},
		{"1.4.2+arm", "1.4.2+riscv", 0},
	} {
		if got := CompareVersions(c.a, c.b); got != c.want {
			t.Errorf("CompareVersions(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
	release := &Manifest{Channel: "release", Version: "1.4.2"}
	if !Newer(release, "1.4.1", 0) || Newer(release, "1.4.2", 0) || Newer(release, "1.5.0", 0) {
		t.Error("the stable rule bent")
	}
	// A replayed old manifest, however honestly signed, is not an
	// update: within a channel, time only moves forward.
	dev := &Manifest{Channel: "dev", Version: "0.1.0-dev.abcdef",
		Published: time.Unix(1000, 0)}
	if !Newer(dev, "whatever", 999) || Newer(dev, "whatever", 1000) || Newer(dev, "whatever", 2000) {
		t.Error("the fast rule bent")
	}
	// What runs is what is offered: never an update, however fresh
	// the manifest's stamp.
	if Newer(dev, "0.1.0-dev.abcdef", 0) {
		t.Error("the running version read as an update")
	}
}

func TestLoopbackMaySpeakHTTP(t *testing.T) {
	ok := Artifact{URL: "http://127.0.0.1:8080/x", SHA256: zeroes64, Size: 1}
	if err := ok.check(); err != nil {
		t.Errorf("loopback http refused: %v", err)
	}
	for _, url := range []string{
		"http://example.org/x", "http://127.0.0.2/x", "http://localhost.evil.org/x",
	} {
		bad := Artifact{URL: url, SHA256: zeroes64, Size: 1}
		if err := bad.check(); err == nil {
			t.Errorf("%s passed as loopback", url)
		}
	}
}

func TestAKeyVouchesForItsTrainAndNoOther(t *testing.T) {
	stable := PublicKey{Channels: []string{"release", "rc", "beta"}}
	fast := PublicKey{Channels: []string{"dev", "try-*"}}
	open := PublicKey{}
	for _, c := range []struct {
		key     PublicKey
		channel string
		want    bool
	}{
		{stable, "release", true}, {stable, "beta", true}, {stable, "dev", false},
		{fast, "dev", true}, {fast, "try-pr-123", true}, {fast, "release", false},
		{fast, "try", false}, // the family needs its dash
		{open, "anything", true},
	} {
		if got := c.key.Vouches(c.channel); got != c.want {
			t.Errorf("Vouches(%v, %q) = %v", c.key.Channels, c.channel, got)
		}
	}
}

func TestTheChannelPinRidesTheKeyFile(t *testing.T) {
	_, pub := pair(t)
	pub.Channels = []string{"release", "rc", "beta"}
	back, err := ParsePublicKey(MarshalPublic(pub))
	if err != nil || len(back.Channels) != 3 || back.Channels[0] != "release" {
		t.Fatalf("the pin did not round trip: %+v, %v", back.Channels, err)
	}
	// The comment is where root speaks: a hand-written store entry
	// pins the same way.
	text := strings.Replace(string(MarshalPublic(pub)),
		"channels: release rc beta", "channels: dev,try-*", 1)
	edited, err := ParsePublicKey([]byte(text))
	if err != nil || !edited.Vouches("try-x") || edited.Vouches("release") {
		t.Fatalf("the hand pin did not read: %+v, %v", edited.Channels, err)
	}
}

func TestTheHotKeyCannotSpeakForTheStableChannel(t *testing.T) {
	// The whole point of the trains: a manifest honestly signed by
	// the fast key, served on the release channel, is refused by the
	// pin — at the fetch, and again at the installer's re-check.
	fastSec, fastPub := pair(t)
	fastPub.Channels = []string{"dev", "try-*"}
	m := channelManifest() // channel: release
	srv := host(t, fastSec, m)
	c := &Client{Base: srv.URL, Trusted: []PublicKey{fastPub}}
	if _, err := c.Check(context.Background(), "release", ""); err == nil ||
		!strings.Contains(err.Error(), "does not vouch for channel release") {
		t.Fatalf("the pin did not hold at the fetch: %v", err)
	}
}

func TestTheEmbeddedKeysParseAndKeepTheirTrains(t *testing.T) {
	keys, err := Trusted(t.TempDir() + "/absent")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("%d embedded keys, want the two trains", len(keys))
	}
	stable, fast := keys[0], keys[1]
	if !stable.Vouches("release") || !stable.Vouches("rc") || !stable.Vouches("beta") ||
		stable.Vouches("dev") {
		t.Errorf("the stable key's pin bent: %v", stable.Channels)
	}
	if !fast.Vouches("dev") || !fast.Vouches("try-pr-9") || fast.Vouches("release") {
		t.Errorf("the fast key's pin bent: %v", fast.Channels)
	}
	if stable.Hex() != "eb9e324264f759a8" || fast.Hex() != "1b2b9d744447e47f" {
		t.Errorf("key ids drifted: %s, %s", stable.Hex(), fast.Hex())
	}
}
