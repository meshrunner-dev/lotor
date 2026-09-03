package room

import (
	"context"
	"crypto/rand"
	"strings"
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
	return sendPostAttempt(t, svc, c, text, at, 0)
}

// sendPostAttempt is sendPost with the retransmission counter a retrying
// client bumps — the reference's way of keeping a retry's hash distinct
// from the frame it repeats, so the seen ring lets it through.
func sendPostAttempt(t *testing.T, svc *service, c client, text string, at time.Time, attempt int) (*mesh.Packet, []byte) {
	t.Helper()
	plain := mesh.BuildTextPlaintextAttempt(at, mesh.TxtTypePlain, text, attempt)
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
	// The same post again, with the fresh attempt bits a retrying client
	// sends, is a retry: acknowledged, not stored twice.
	if ack, _ := sendPostAttempt(t, svc, alice, "hello room", at, 1); ack == nil {
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
	// Three turns of the clock, each on its schedule: the round-robin
	// reaches every member, and a turn before its time would do nothing.
	turn := at.Add(postSyncDelay + time.Second)
	for range 3 {
		svc.pushDue(turn)
		svc.mu.Lock()
		turn = svc.nextPush.Add(time.Millisecond)
		svc.mu.Unlock()
	}
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

func TestAFullRoomUnseatsTheIdlestMemberNeverAnAdmin(t *testing.T) {
	svc := benchRoom(t, nil)
	admin := newClient(t, svc)
	login(t, svc, admin, "sesame", 0)
	members := make([]client, 0, maxClients)
	for i := 1; i < maxClients; i++ {
		c := newClient(t, svc)
		members = append(members, c)
		login(t, svc, c, "welcome", 0)
		svc.mu.Lock()
		// Older members spoke earlier: the first one is the idlest.
		svc.table.Get(c.id.PubKey[:]).LastActive = time.Now().Add(-time.Duration(maxClients-i) * time.Minute)
		svc.mu.Unlock()
	}
	svc.mu.Lock()
	svc.table.Get(admin.id.PubKey[:]).LastActive = time.Now().Add(-24 * time.Hour)
	svc.mu.Unlock()
	newcomer := newClient(t, svc)
	if lr, _ := mesh.ParseLoginReply(openReply(t, newcomer, login(t, svc, newcomer, "welcome", 0))); lr.Result != mesh.LoginOK {
		t.Fatalf("the twenty-first login was refused: %+v", lr)
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.table.Get(admin.id.PubKey[:]) == nil {
		t.Fatal("the idle admin was unseated")
	}
	if svc.table.Get(members[0].id.PubKey[:]) != nil {
		t.Fatal("the idlest member kept its place")
	}
	if svc.table.Get(newcomer.id.PubKey[:]) == nil || len(svc.table.By) != maxClients {
		t.Fatalf("table = %d entries, newcomer present %v", len(svc.table.By), svc.table.Get(newcomer.id.PubKey[:]) != nil)
	}
	if _, remembered := svc.members[members[0].id.PubKey]; remembered {
		t.Error("the evicted member's cursor was kept")
	}
}

// A retransmission of a post the room refused is judged afresh, not
// acknowledged as the retry of something kept: the reference client
// sends up to 160 characters, the room keeps 151, and the second try
// used to earn an ACK for a post nobody had.
func TestARetryOfARefusedPostIsNotAcknowledged(t *testing.T) {
	svc := benchRoom(t, nil)
	alice := newClient(t, svc)
	login(t, svc, alice, "welcome", 0)
	long := strings.Repeat("x", maxPostText+1)
	at := time.Now()
	if reply, _ := sendPost(t, svc, alice, long, at); reply != nil {
		t.Fatalf("an over-long post was answered with %v", reply.PayloadType())
	}
	// The same text at the same timestamp, fresh attempt bits: a retry
	// in the reference's eyes, and still nothing the room kept.
	if reply, _ := sendPostAttempt(t, svc, alice, long, at, 1); reply != nil {
		t.Fatalf("the retry of a refused post was acknowledged with %v", reply.PayloadType())
	}
	svc.mu.Lock()
	kept, refused := len(svc.posts), svc.refused
	svc.mu.Unlock()
	if kept != 0 || refused != 2 {
		t.Fatalf("posts kept %d, refused %d — want 0 and 2", kept, refused)
	}
	// A kept post's retry still earns its ACK again and nothing else.
	if reply, _ := sendPost(t, svc, alice, "short", at.Add(time.Second)); reply == nil || reply.PayloadType() != mesh.PayloadTypeAck {
		t.Fatal("a kept post was not acknowledged")
	}
	if reply, _ := sendPostAttempt(t, svc, alice, "short", at.Add(time.Second), 1); reply == nil || reply.PayloadType() != mesh.PayloadTypeAck {
		t.Fatal("the retry of a kept post was not acknowledged again")
	}
	svc.mu.Lock()
	kept = len(svc.posts)
	svc.mu.Unlock()
	if kept != 1 {
		t.Fatalf("a retry stored the post twice: %d", kept)
	}
}

// The copies a flood sends back, or a recording replayed, act once:
// the reference keeps its handlers behind a seen ring, and so does the
// room — one ACK for five identical post frames, one cursor move for
// six identical keep-alives.
func TestADuplicateFrameActsOnce(t *testing.T) {
	svc := benchRoom(t, nil)
	alice := newClient(t, svc)
	login(t, svc, alice, "welcome", 0)
	plain := mesh.BuildTextPlaintext(time.Now(), mesh.TxtTypePlain, "hello")
	pkt, err := mesh.BuildDatagram(mesh.PayloadTypeTxtMsg, svc.id.PubKey[:mesh.PathHashSize],
		alice.id.PubKey[:mesh.PathHashSize], alice.secret, plain)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		hear(t, svc, pkt)
	}
	if reply := emissionPacket(queued(t, svc)); reply.PayloadType() != mesh.PayloadTypeAck {
		t.Fatalf("the first copy earned a %v", reply.PayloadType())
	}
	nothingQueued(t, svc)
	svc.mu.Lock()
	duplicates, kept := svc.duplicates, len(svc.posts)
	svc.mu.Unlock()
	if duplicates != 4 || kept != 1 {
		t.Fatalf("duplicates %d, posts %d — want 4 and 1", duplicates, kept)
	}
}

// The push clock keeps to its schedule: a turn that fires before the
// two seconds a login bought does nothing and leaves the schedule
// where the login put it, so the reply reaches the member before any
// post does.
func TestAPushTurnBeforeItsTimeDoesNothing(t *testing.T) {
	svc := benchRoom(t, nil)
	alice, bob := newClient(t, svc), newClient(t, svc)
	login(t, svc, alice, "welcome", 0)
	sendPost(t, svc, alice, "for bob", time.Now().Add(-postSyncDelay-time.Second))
	// Bob logs in behind on his cursor: a post is waiting for him, and
	// the login bought him two seconds of quiet.
	login(t, svc, bob, "welcome", 0)
	svc.mu.Lock()
	// Age the post past its settling delay so only the schedule holds it back.
	for i := range svc.posts {
		svc.posts[i].at -= uint32(postSyncDelay/time.Second) + 1
	}
	scheduled := svc.nextPush
	svc.mu.Unlock()
	svc.pushDue(time.Now())
	nothingQueued(t, svc)
	svc.mu.Lock()
	if svc.nextPush != scheduled {
		t.Errorf("an early turn moved the schedule from %v to %v", scheduled, svc.nextPush)
	}
	svc.mu.Unlock()
	// On time, the round-robin reaches bob within two turns (alice, the
	// author, is skipped for her own post).
	turn := scheduled.Add(time.Millisecond)
	for range 2 {
		svc.pushDue(turn)
		svc.mu.Lock()
		turn = svc.nextPush.Add(time.Millisecond)
		svc.mu.Unlock()
	}
	if pushed := emissionPacket(queued(t, svc)); pushed.PayloadType() != mesh.PayloadTypeTxtMsg {
		t.Fatalf("the turns on time pushed a %v", pushed.PayloadType())
	}
}
