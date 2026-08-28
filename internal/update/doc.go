// Package update reads the release channels: fetch a manifest, prove
// who published it, decide whether what it names is newer than what
// runs. Nothing here touches the filesystem or the network — this is
// the part that must be right, kept small enough to audit.
//
// The trust model. A channel is a signed manifest naming artifacts by
// sha256; the signature is the root, the hashes carry the trust down
// to the bytes. Verification accepts a set of public keys, never one:
// key rollover is a release that trusts the old and the new key at
// once, then a switch of the signing key, then — releases later — a
// release that drops the old. A relay always verifies against the
// union of what its binary embeds and what the operator dropped in
// the trusted-keys directory, so the fleet never needs to act for an
// official rotation, and a fork only needs its own key beside ours.
// During an overlap the publisher may lay several signature files
// beside one manifest, one per active key, so even a relay that only
// trusts the outgoing key still verifies.
//
// Signatures are minisign-compatible (ed25519, prehashed with
// BLAKE2b-512): the CI signs with cmd/relsign, and anyone can check a
// manifest by hand with stock minisign.
package update
