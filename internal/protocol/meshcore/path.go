package meshcore

// Learning the way home. A question that arrives direct arrives with
// its path already spent — every hop consumed its own entry — so the
// packet says nothing about where the asker is, and the only answer
// that reaches both the adjacent and the distant is a flood. That is
// the whole mesh paying for one client's question.
//
// The protocol's way out is for the client to say it: a PATH packet
// whose content is the route the server should use to reach it. The
// server stores it against the session and answers direct from then
// on. Nothing is inferred and nothing is gossiped — the client is the
// only authority on how to reach the client.

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"
)

// pathVerdict judges a PATH addressed to us. It is ours to read only
// when a live session's shared secret opens it, which is the same test
// an authenticated request passes: the sender hash narrows the search,
// the MAC decides. A PATH we cannot open belongs to some other pair
// and routes on untouched.
//
// The reference sends no reciprocal path here, unlike the chat mesh
// that shares the code path: a repeater teaches its own route through
// its adverts, and answering a route with a route would double the
// exchange for nothing.
func (e *engine) pathVerdict(rx *reception) (verdict, why string, handled bool) {
	d, err := meshcore.ParseDatagram(rx.pkt.Payload)
	if err != nil || e.id == nil || d.DestHash[0] != e.id.PubKey[0] {
		return "", "", false
	}
	for _, c := range e.acl.matching(d.SrcHash[0]) {
		plain, err := d.Open(c.secret)
		if err != nil {
			continue // some other session's packet, or not one at all
		}
		pr, err := meshcore.DecodePathReturn(plain)
		if err != nil {
			// Opened, so it was addressed to us and is ours to
			// consume; unreadable, so there is nothing to learn.
			return verdictIgnored, "route home badly encoded", true
		}
		e.learnOutPath(c, pr)
		return verdictClientPath, fmt.Sprintf("route home, %d hops",
			pr.PathLen&pathHopCountMask), true
	}
	return "", "", false
}

// pathHopCountMask isolates the hop count from a path descriptor's
// low six bits, for the journal line that reports what was learned.
const pathHopCountMask = 63

// learnOutPath records the route and refreshes the session on it. The
// newest one wins outright: a client that moved is the reason it sent
// a second, and preferring the older would pin the answer to the route
// it just left.
func (e *engine) learnOutPath(c *client, pr *meshcore.PathReturn) {
	c.out = &outPath{
		pathLen: pr.PathLen,
		path:    append([]byte(nil), pr.Path...),
		learned: time.Now(),
	}
	c.lastActive = time.Now()
	e.log.Info("a client taught us its route home",
		zap.String("pubkey", shortKey(c.pubKey[:])),
		zap.Int("hops", int(pr.PathLen&pathHopCountMask)))
}
