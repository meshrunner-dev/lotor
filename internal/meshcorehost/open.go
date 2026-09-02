package meshcorehost

// Opening what was sealed to us: a stranger's anonymous request under
// our key, or a client's datagram under the secret its session holds.

import "meshrunner.dev/pkg/meshcore"

// Anon is an anonymous request this node could read: who asked, the
// secret the answer must be sealed under, and what they said.
type Anon struct {
	Sender []byte
	Secret []byte
	Plain  []byte
}

// OpenAnon reads an ANON_REQ payload addressed to id. short is true
// for a payload too small to be one at all — the reference releases
// such a packet unrouted; ok is false when the request is not ours to
// read — wrong destination hash, a key we cannot agree a secret with,
// a MAC that does not verify — and the packet must flow through plain
// routing like any other traffic, exactly as the reference forwards
// what it could not decrypt.
func OpenAnon(id *meshcore.LocalIdentity, payload []byte) (a Anon, short, ok bool) {
	d, err := meshcore.ParseAnonDatagram(payload)
	if err != nil {
		return Anon{}, true, false
	}
	if id == nil || d.DestHash[0] != id.PubKey[0] {
		return Anon{}, false, false
	}
	secret, err := id.SharedSecret(d.SenderPub)
	if err != nil {
		return Anon{}, false, false
	}
	plain, err := d.Open(secret)
	if err != nil {
		return Anon{}, false, false // a failed MAC routes on, unread
	}
	return Anon{Sender: d.SenderPub, Secret: secret, Plain: plain}, false, true
}

// OpenSession finds the live session that sent a datagram addressed to
// id and returns its decrypted content. The source hash narrows the
// candidates; the MAC decides. Nil when the datagram is not ours, or
// no session holds its key: route it on.
func (t *Table) OpenSession(id *meshcore.LocalIdentity, payload []byte) (*Client, []byte) {
	d, err := meshcore.ParseDatagram(payload)
	if err != nil || id == nil || d.DestHash[0] != id.PubKey[0] {
		return nil, nil
	}
	for _, c := range t.Matching(d.SrcHash[0]) {
		if plain, err := d.Open(c.Secret); err == nil && len(plain) >= meshcore.AdminTagSize+1 {
			return c, plain
		}
	}
	return nil, nil
}
