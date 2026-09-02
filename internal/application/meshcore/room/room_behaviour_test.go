package room

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/application"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/origin"
	"meshrunner.dev/lotor/internal/radio"

	mesh "meshrunner.dev/pkg/meshcore"
)

// A room on the bench: shadow gate so answers reach the queue, an
// in-memory store so history persists across a rebuild, no radio.
func benchRoom(t *testing.T, store *confdb.Store) *service {
	t.Helper()
	cfg := baseConfig()
	cfg["admin_password"] = "sesame"
	cfg["guest_password"] = "welcome"
	cfg["allow_read_only"] = true
	cfg["advert_local_interval"], cfg["advert_flood_interval"] = "0s", "0s"
	// Without a store the room must run RAM-only: a persisted post
	// that has nowhere to go is refused, which is the contract.
	cfg["persist_history"] = store != nil
	svc, err := build(application.Spec{Name: "lobby", Protocol: "meshcore", Type: "meshcore-room",
		Config: cfg, TX: application.TXPolicy{Mode: config.TXShadow, QueueDepth: 8}, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	room, ok := svc.(*service)
	if !ok {
		t.Fatalf("build returned a %T", svc)
	}
	return room
}

type client struct {
	id     *mesh.LocalIdentity
	secret []byte
}

func newClient(t *testing.T, room *service) client {
	t.Helper()
	id, err := mesh.NewLocalIdentity(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := id.SharedSecret(room.id.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	return client{id: id, secret: secret}
}

// hear hands the room one packet as if the radio had.
func hear(t *testing.T, svc *service, pkt *mesh.Packet) {
	t.Helper()
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	svc.processRF(context.Background(), radio.Frame{Payload: raw, Correlation: correlation.New()})
}

// queued takes the next emission the room composed, or fails.
func queued(t *testing.T, svc *service) origin.Emission {
	t.Helper()
	item, ok := svc.pipeline.Queue.TakeUntil(context.Background(), time.Now().Add(2*serverResponseDelay))
	if !ok {
		t.Fatal("the room composed nothing")
	}
	return item
}

func nothingQueued(t *testing.T, svc *service) {
	t.Helper()
	if item, ok := svc.pipeline.Queue.TakeUntil(context.Background(), time.Now().Add(2*serverResponseDelay)); ok {
		t.Fatalf("the room composed %s when silence was owed", item.Kind)
	}
}

// login sends a room login and returns the reply the room queued.
func login(t *testing.T, svc *service, c client, password string, since uint32) *mesh.Packet {
	t.Helper()
	ts := uint32(time.Now().Add(-10 * time.Second).Unix())
	pkt, _, err := mesh.BuildRoomLoginReq(c.id, svc.id.PubKey[:], ts, since, password)
	if err != nil {
		t.Fatal(err)
	}
	hear(t, svc, pkt)
	return emissionPacket(queued(t, svc))
}

func emissionPacket(e origin.Emission) *mesh.Packet {
	pkt, _ := e.Subject.(*mesh.Packet)
	return pkt
}

func TestTheRoomAdmitsByItsDoorsAndAnswersTheReferenceReply(t *testing.T) {
	svc := benchRoom(t, nil)
	admin, guest, member, stranger := newClient(t, svc), newClient(t, svc), newClient(t, svc), newClient(t, svc)

	reply := login(t, svc, admin, "sesame", 0)
	// A flooded login is answered inside a PATH return, so the client
	// learns the way here; the reply inside is the reference's 13 bytes.
	if reply.PayloadType() != mesh.PayloadTypePath {
		t.Fatalf("flooded login answered with %v, want a path return", reply.PayloadType())
	}
	d, _ := mesh.ParseDatagram(reply.Payload)
	plain, err := d.Open(admin.secret)
	if err != nil {
		t.Fatal(err)
	}
	pr, err := mesh.DecodePathReturn(plain)
	if err != nil || pr.ExtraType != uint8(mesh.PayloadTypeResponse) {
		t.Fatalf("path return = %+v, %v", pr, err)
	}
	lr, err := mesh.ParseLoginReply(pr.Extra)
	if err != nil || lr.Result != mesh.LoginOK || !lr.IsAdmin || mesh.Role(lr.Permissions) != mesh.PermAdmin ||
		lr.FirmwareLevel != firmwareVerLevel {
		t.Fatalf("login reply = %+v, %v", lr, err)
	}
	// The room word earns a member who may post; any other word, a
	// guest who may only read; the same word twice is a replay.
	if lr, _ := mesh.ParseLoginReply(openReply(t, member, login(t, svc, member, "welcome", 0))); mesh.Role(lr.Permissions) != mesh.PermReadWrite {
		t.Fatalf("member role = %+v", lr)
	}
	if lr, _ := mesh.ParseLoginReply(openReply(t, guest, login(t, svc, guest, "whatever", 0))); mesh.Role(lr.Permissions) != mesh.PermGuest {
		t.Fatalf("guest role = %+v", lr)
	}
	svc.mu.Lock()
	if len(svc.table.Entries()) != 2 || len(svc.table.Sessions()) != 3 {
		t.Errorf("table = %d durable, %d live", len(svc.table.Entries()), len(svc.table.Sessions()))
	}
	svc.mu.Unlock()
	// Read-only closed: an unknown word earns silence.
	svc.mu.Lock()
	svc.p.AllowReadOnly = false
	svc.mu.Unlock()
	pkt, _, _ := mesh.BuildRoomLoginReq(stranger.id, svc.id.PubKey[:], uint32(time.Now().Unix()), 0, "nope")
	hear(t, svc, pkt)
	nothingQueued(t, svc)
}

// openReply opens the login reply a client received and returns the
// reply body, whatever envelope it came in.
func openReply(t *testing.T, c client, reply *mesh.Packet) []byte {
	t.Helper()
	d, err := mesh.ParseDatagram(reply.Payload)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := d.Open(c.secret)
	if err != nil {
		t.Fatal(err)
	}
	if reply.PayloadType() == mesh.PayloadTypePath {
		pr, err := mesh.DecodePathReturn(plain)
		if err != nil {
			t.Fatal(err)
		}
		plain = pr.Extra
	}
	// The reply's first field is the room's clock — the login reply is
	// the whole frame, as a companion reads it.
	return plain
}

// post sends a plain text from a logged-in client and returns the
// room's next emission, nil when it stayed silent.
func sendPost(t *testing.T, svc *service, c client, text string, at time.Time) (*mesh.Packet, []byte) {
	t.Helper()
	plain := mesh.BuildTextPlaintext(at, mesh.TxtTypePlain, text)
	pkt, err := mesh.BuildDatagram(mesh.PayloadTypeTxtMsg, svc.id.PubKey[:mesh.PathHashSize],
		c.id.PubKey[:mesh.PathHashSize], c.secret, plain)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = mesh.MakeHeader(mesh.RouteDirect, mesh.PayloadTypeTxtMsg, mesh.PayloadVer1)
	hear(t, svc, pkt)
	item, ok := svc.pipeline.Queue.TakeUntil(context.Background(), time.Now().Add(2*serverResponseDelay))
	if !ok {
		return nil, plain
	}
	return emissionPacket(item), plain
}

func TestAPostIsKeptAcknowledgedAndPushedToTheOthers(t *testing.T) {
	ctx := context.Background()
	store, err := confdb.Open(ctx, confdb.Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	svc := benchRoom(t, store)
	alice, bob, carol := newClient(t, svc), newClient(t, svc), newClient(t, svc)
	login(t, svc, alice, "welcome", 0)
	login(t, svc, bob, "welcome", 0)
	// A guest: reads, never posts — and logs in with a cursor already
	// past everything, so nothing is ever pushed to her.
	login(t, svc, carol, "whatever", uint32(time.Now().Add(time.Hour).Unix()))

	at := time.Now()
	ack, plain := sendPost(t, svc, alice, "hello room", at)
	if ack == nil || ack.PayloadType() != mesh.PayloadTypeAck {
		t.Fatalf("a post earned %v, want an ACK", ack)
	}
	preimage, _ := mesh.TextAckPreimage(plain)
	if crc, _ := mesh.ParseAck(ack.Payload); crc != mesh.AckCRC(preimage, alice.id.PubKey[:]) {
		t.Error("the ACK does not hash to the post under the author's key")
	}
	// Kept in the ring, and on disk: a post acknowledged is a post kept.
	svc.mu.Lock()
	if len(svc.posts) != 1 || svc.posts[0].text != "hello room" || svc.posts[0].author != alice.id.PubKey {
		t.Fatalf("ring = %+v", svc.posts)
	}
	svc.mu.Unlock()
	if kept, _ := store.LoadRoomPosts(ctx, "lobby"); len(kept) != 1 || kept[0].Text != "hello room" {
		t.Fatalf("store = %+v", kept)
	}
	// The same post again is a retry: acknowledged, not stored twice.
	if ack, _ := sendPost(t, svc, alice, "hello room", at); ack == nil {
		t.Fatal("a retry earned no ACK")
	}
	svc.mu.Lock()
	n := len(svc.posts)
	svc.mu.Unlock()
	if n != 1 {
		t.Fatalf("a retry was stored again: %d posts", n)
	}
	// A guest's post earns silence; an over-long post is refused.
	if ack, _ := sendPost(t, svc, carol, "let me in", time.Now()); ack != nil {
		t.Error("a guest's post was acknowledged")
	}
	long := make([]byte, maxPostText+1)
	for i := range long {
		long[i] = 'x'
	}
	if ack, _ := sendPost(t, svc, bob, string(long), time.Now().Add(time.Second)); ack != nil {
		t.Error("an over-long post was acknowledged")
	}

	// The push clock: alice never receives her own post; bob does,
	// once the post has settled, as signed-plain text carrying her
	// prefix, and his cursor moves when he acknowledges it.
	later := at.Add(postSyncDelay + time.Second)
	svc.pushDue(later)
	svc.pushDue(later)
	svc.pushDue(later)
	var push *mesh.Packet
	for {
		item, ok := svc.pipeline.Queue.TakeUntil(ctx, time.Now().Add(2*serverResponseDelay))
		if !ok {
			break
		}
		if item.Kind == "post-push" {
			push = emissionPacket(item)
		}
	}
	if push == nil {
		t.Fatal("no post was pushed")
	}
	d, _ := mesh.ParseDatagram(push.Payload)
	if _, err := d.Open(carol.secret); err == nil {
		t.Fatal("a post was pushed to a member whose cursor is already past it")
	}
	reader := bob
	pushed, err := d.Open(bob.secret)
	if err != nil {
		t.Fatal("the push does not open under bob's secret")
	}
	text, err := mesh.ParseTextPlaintext(pushed)
	if err != nil || text.Type != mesh.TxtTypeSignedPlain || text.Text != "hello room" ||
		string(text.SignedPrefix) != string(alice.id.PubKey[:4]) {
		t.Fatalf("push = %+v, %v", text, err)
	}
	ackBody, _ := mesh.BuildTextAckBody(pushed, reader.id.PubKey[:])
	ackPkt, _ := mesh.BuildAck(ackBody)
	hear(t, svc, ackPkt)
	svc.mu.Lock()
	m := svc.members[reader.id.PubKey]
	svc.mu.Unlock()
	if m.pendingAck != 0 || m.syncSince != svc.posts[0].at {
		t.Fatalf("after the ACK, member = %+v (post at %d)", m, svc.posts[0].at)
	}
	// The cursor reaches the store on the lazy flush, and a rebuilt
	// room restores both the history and the cursor.
	svc.flushCursors(ctx)
	rebuilt := benchRoom(t, store)
	rebuilt.mu.Lock()
	defer rebuilt.mu.Unlock()
	if len(rebuilt.posts) != 1 || rebuilt.members[reader.id.PubKey] == nil ||
		rebuilt.members[reader.id.PubKey].syncSince != m.syncSince {
		t.Fatalf("rebuilt room: %d posts, cursor %+v", len(rebuilt.posts), rebuilt.members[reader.id.PubKey])
	}
}

func TestAKeepAliveMovesTheCursorAndIsAnsweredDirectWithTheCount(t *testing.T) {
	svc := benchRoom(t, nil)
	alice, bob := newClient(t, svc), newClient(t, svc)
	login(t, svc, alice, "welcome", 0)
	login(t, svc, bob, "welcome", 0)
	sendPost(t, svc, alice, "one", time.Now())
	sendPost(t, svc, alice, "two", time.Now().Add(time.Second))

	// Without a taught route the room answers nothing — the rule.
	ts := uint32(time.Now().Unix()) + 10
	req, err := mesh.BuildRequest(bob.id, svc.id.PubKey[:], bob.secret, ts, mesh.FrameKeepAliveRequest(0))
	if err != nil {
		t.Fatal(err)
	}
	req.Header = mesh.MakeHeader(mesh.RouteDirect, mesh.PayloadTypeReq, mesh.PayloadVer1)
	hear(t, svc, req)
	nothingQueued(t, svc)

	// Bob teaches his route: a PATH return, which the room learns and
	// does not answer.
	path, err := mesh.BuildPathReturn(svc.id.PubKey[:mesh.PathHashSize], bob.id.PubKey[:mesh.PathHashSize],
		bob.secret, 1, []byte{0x42}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	hear(t, svc, path)
	nothingQueued(t, svc)
	svc.mu.Lock()
	if c := svc.table.Get(bob.id.PubKey[:]); c == nil || c.Out == nil || c.Out.Path[0] != 0x42 {
		t.Fatalf("route not learned: %+v", c)
	}
	svc.mu.Unlock()

	// Now the keep-alive is answered direct: the CRC both ends compute,
	// and the two posts bob has not read as the fifth byte.
	ts++
	req, _ = mesh.BuildRequest(bob.id, svc.id.PubKey[:], bob.secret, ts, mesh.FrameKeepAliveRequest(0))
	req.Header = mesh.MakeHeader(mesh.RouteDirect, mesh.PayloadTypeReq, mesh.PayloadVer1)
	hear(t, svc, req)
	ack := emissionPacket(queued(t, svc))
	if ack.PayloadType() != mesh.PayloadTypeAck || !ack.IsRouteDirect() {
		t.Fatalf("keep-alive answered with %v %v", ack.PayloadType(), ack.Route())
	}
	crc, unsynced, err := mesh.ParseKeepAliveAck(ack.Payload)
	want, _ := mesh.KeepAliveAckCRC(mesh.FrameAdmin(ts, mesh.FrameKeepAliveRequest(0)), bob.id.PubKey[:])
	if err != nil || crc != want || unsynced != 2 {
		t.Fatalf("keep-alive ack = %08x %d %v, want %08x 2", crc, unsynced, err, want)
	}
	// A cursor the client forces is taken as its word.
	svc.mu.Lock()
	second := svc.posts[0].at
	svc.mu.Unlock()
	ts++
	req, _ = mesh.BuildRequest(bob.id, svc.id.PubKey[:], bob.secret, ts, mesh.FrameKeepAliveRequest(second))
	req.Header = mesh.MakeHeader(mesh.RouteDirect, mesh.PayloadTypeReq, mesh.PayloadVer1)
	hear(t, svc, req)
	ack = emissionPacket(queued(t, svc))
	if _, unsynced, _ := mesh.ParseKeepAliveAck(ack.Payload); unsynced != 1 {
		t.Fatalf("after forcing the cursor, %d unsynced, want 1", unsynced)
	}
}
