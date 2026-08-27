package meshcore

import (
	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

// reception is one frame decoded exactly once.
//
// Parsing a packet is cheap; verifying an advert's signature and
// opening a request addressed to us are not — a scalar multiplication
// and a MAC sweep apiece. Both used to happen twice for every frame:
// once so the judgement could name what it saw, once again so the
// action could act on it. That is work a stranger can ask of us
// before any limiter is consulted, so whatever the judgement paid
// for, the action reads back from here.
type reception struct {
	pkt   *meshcore.Packet
	frame radio.Frame
	id    txn.ID

	// advert is the announcement this packet carries, parsed and
	// signature-checked; nil unless ok is true.
	advert     *meshcore.Advert
	advertOK   bool
	selfAdvert bool

	// opened is a request addressed to this node that we could read.
	opened *opened

	// scope is the transport scope this frame arrived in and whether
	// we carry it, resolved once: matching costs an HMAC over the
	// whole payload per scope carried.
	scope        string
	scopeKey     meshcore.TransportKey
	scopeCarried bool
	scopeKnown   bool
}

// opened is a request this node decrypted: who asked, under what
// secret, and what they said. Exactly one of sender and session is
// set — sender for a stranger's anonymous question, session for a
// logged-in client's.
type opened struct {
	sender  []byte
	session *client
	secret  []byte
	plain   []byte
	// req is the decoded anonymous question, nil when the plaintext
	// held a password rather than one of the typed questions.
	req *meshcore.AnonRequest
}
