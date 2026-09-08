package room

// The room's answers to authenticated questions, its refusals, the
// bounds of its memory, the failures of its push clock, and its radio
// lifecycle — each with the bytes or the state a client would see.

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/application"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/origin"
	"meshrunner.dev/lotor/internal/radio"

	mesh "meshrunner.dev/pkg/meshcore"
)

func memoryStore(t *testing.T) *confdb.Store {
	t.Helper()
	store, err := confdb.Open(context.Background(), confdb.Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// ask sends an authenticated REQ from a client and returns the room's
// reply body — the admin frame unwrapped — or nil when it stayed silent.
func ask(t *testing.T, svc *service, c client, body []byte) []byte {
	t.Helper()
	req, err := mesh.BuildRequest(c.id, svc.id.PubKey[:], c.secret, uint32(time.Now().Unix())+uint32(time.Now().Nanosecond()%1000), body)
	if err != nil {
		t.Fatal(err)
	}
	hear(t, svc, req)
	item, ok := svc.pipeline.Queue.TakeUntil(context.Background(), time.Now().Add(2*serverResponseDelay))
	if !ok {
		return nil
	}
	_, answer, err := mesh.UnframeAdmin(openReply(t, c, emissionPacket(item)))
	if err != nil {
		t.Fatal(err)
	}
	return answer
}

// The status a companion's page shows and the access list an admin may
// ask for are answered in the reference's bytes; a member asking for
// the access list is answered with silence.
func TestAuthenticatedQuestionsAreAnsweredInTheReferenceBytes(t *testing.T) {
	svc := benchRoom(t, memoryStore(t))
	admin, member := newClient(t, svc), newClient(t, svc)
	login(t, svc, admin, "sesame", 0)
	login(t, svc, member, "welcome", 0)
	sendPost(t, svc, member, "one", time.Now())

	status, err := mesh.FrameStatusRequest()
	if err != nil {
		t.Fatal(err)
	}
	stats, err := mesh.ParseRoomStats(ask(t, svc, member, status))
	if err != nil {
		t.Fatal(err)
	}
	// Two flooded logins, one direct post and this flooded question: the
	// bench hands frames straight to the dispatcher, so the receive-side
	// tally is what the dispatcher saw.
	if stats.Posted != 1 || stats.RecvFlood != 3 || stats.RecvDirect != 1 || stats.UptimeSecs > 5 {
		t.Fatalf("status = %+v", stats)
	}

	if answer := ask(t, svc, member, mesh.FrameAccessListRequest()); answer != nil {
		t.Fatalf("a member was handed the access list: % x", answer)
	}
	entries := mesh.ParseAccessList(ask(t, svc, admin, mesh.FrameAccessListRequest()))
	if len(entries) != 1 || mesh.Role(entries[0].Permissions) != mesh.PermAdmin ||
		string(entries[0].PubKeyPrefix[:]) != string(admin.id.PubKey[:len(entries[0].PubKeyPrefix)]) {
		t.Fatalf("access list = %+v, want the one admin", entries)
	}
}

// "A post acknowledged is a post kept": a post the room cannot keep is
// refused — no ACK, nothing in the ring, one refusal counted — whether
// the store was never there or has gone away.
func TestAPostThatCannotBeKeptIsNotAcknowledged(t *testing.T) {
	orphan := benchRoomTuned(t, nil, func(cfg map[string]any) { cfg["persist_history"] = true })
	alice := newClient(t, orphan)
	login(t, orphan, alice, "welcome", 0)
	if ack, _ := sendPost(t, orphan, alice, "lost", time.Now()); ack != nil {
		t.Fatalf("a post nobody could keep was acknowledged with %v", ack.PayloadType())
	}
	if info := orphan.Info(); info.Summary["refused"] != "1" || info.Summary["posts"] != "0 / 32" {
		t.Fatalf("summary = %v", info.Summary)
	}

	store := memoryStore(t)
	svc := benchRoom(t, store)
	bob := newClient(t, svc)
	login(t, svc, bob, "welcome", 0)
	if ack, _ := sendPost(t, svc, bob, "kept", time.Now()); ack == nil {
		t.Fatal("a post the store could keep was not acknowledged")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if ack, _ := sendPost(t, svc, bob, "lost", time.Now().Add(time.Second)); ack != nil {
		t.Fatalf("a post the closed store could not keep was acknowledged with %v", ack.PayloadType())
	}
	if info := svc.Info(); info.Summary["refused"] != "1" || info.Summary["posts"] != "1 / 32" {
		t.Fatalf("summary = %v", info.Summary)
	}
}

// The ring holds history posts, newest kept: the store agrees, and a
// room rebuilt with a shorter memory reads back only what fits.
func TestTheRingForgetsTheOldestAndAShorterMemoryReadsLess(t *testing.T) {
	store := memoryStore(t)
	svc := benchRoomTuned(t, store, func(cfg map[string]any) { cfg["history"] = 3 })
	alice := newClient(t, svc)
	login(t, svc, alice, "welcome", 0)
	at := time.Now()
	for i, text := range []string{"one", "two", "three", "four", "five"} {
		if ack, _ := sendPost(t, svc, alice, text, at.Add(time.Duration(i)*time.Second)); ack == nil {
			t.Fatalf("post %q refused", text)
		}
	}
	texts := func(posts []post) []string {
		out := make([]string, 0, len(posts))
		for _, p := range posts {
			out = append(out, p.text)
		}
		return out
	}
	svc.mu.Lock()
	kept := texts(svc.posts)
	svc.mu.Unlock()
	if len(kept) != 3 || kept[0] != "three" || kept[2] != "five" {
		t.Fatalf("ring = %v, want the newest three in order", kept)
	}
	stored, err := store.LoadRoomPosts(context.Background(), "lobby")
	if err != nil || len(stored) != 3 || stored[0].Text != "three" || stored[2].Text != "five" {
		t.Fatalf("store = %+v, %v", stored, err)
	}

	shorter := benchRoomTuned(t, store, func(cfg map[string]any) { cfg["history"] = 2 })
	if err := shorter.loadHistory(context.Background()); err != nil {
		t.Fatal(err)
	}
	shorter.mu.Lock()
	loaded := texts(shorter.posts)
	shorter.mu.Unlock()
	if len(loaded) != 2 || loaded[0] != "four" || loaded[1] != "five" {
		t.Fatalf("a shorter memory loaded %v, want the newest two", loaded)
	}
}

// teachRoute hands the room a PATH return from a client, so the room
// knows the way back to it.
func teachRoute(t *testing.T, svc *service, c client, path []byte) {
	t.Helper()
	pkt, err := mesh.BuildPathReturn(svc.id.PubKey[:mesh.PathHashSize], c.id.PubKey[:mesh.PathHashSize],
		c.secret, uint8(len(path)), path, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	hear(t, svc, pkt)
}

// A member with a taught route is pushed down it, with the reference's
// per-hop ACK patience; three pushes that time out stall the member,
// and a keep-alive clears the strikes.
func TestPushesGoDownATaughtRouteAndThreeTimeoutsStallAMember(t *testing.T) {
	svc := benchRoom(t, nil)
	alice, bob := newClient(t, svc), newClient(t, svc)
	login(t, svc, alice, "welcome", 0)
	login(t, svc, bob, "welcome", 0)
	teachRoute(t, svc, bob, []byte{0x42, 0x43})
	sendPost(t, svc, alice, "hello", time.Now())

	now := time.Now().Add(2 * postSyncDelay)
	turn := func() {
		svc.mu.Lock()
		svc.nextPush = time.Time{}
		svc.mu.Unlock()
		svc.pushDue(now)
	}
	member := func() (uint32, time.Time, int) {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		m := svc.member(bob.id.PubKey)
		return m.pendingAck, m.ackDeadline, int(m.failures)
	}
	// Alice authored the post, so bob is the one member served; the
	// walk may take a turn to reach him.
	var push *mesh.Packet
	for range 3 {
		turn()
		if svc.pipeline.Queue.Len() > 0 {
			item := queued(t, svc)
			finishPush(svc, item, origin.Outcome{Sent: true, At: now})
			push = emissionPacket(item)
			break
		}
	}
	if push == nil || !push.IsRouteDirect() || push.PathLen != 2 || push.Path[0] != 0x42 {
		t.Fatalf("push = %+v", push)
	}
	pending, deadline, failures := member()
	if pending == 0 || failures != 0 || deadline != now.Add(pushAckBase+3*pushAckPerHop) {
		t.Fatalf("after the push: pending %08x, deadline %v, failures %d — want a deadline of 4s + 2s per hop + 2s",
			pending, deadline.Sub(now), failures)
	}

	// Each timed-out push counts a failure and is re-pushed when the
	// walk reaches bob again, until the third strike stalls him: no
	// turn after that pushes anything.
	walkToBob := func() bool {
		for range 3 {
			turn()
			if svc.pipeline.Queue.Len() > 0 {
				return true
			}
		}
		return false
	}
	for strike := 1; strike <= maxPushFailures; strike++ {
		now = deadline.Add(time.Second)
		turn()
		pending, deadline, failures = member()
		if failures != strike {
			t.Fatalf("strike %d: failures %d", strike, failures)
		}
		if strike < maxPushFailures {
			if !walkToBob() {
				t.Fatalf("strike %d: not re-pushed", strike)
			}
			finishPush(svc, queued(t, svc), origin.Outcome{Sent: true, At: now})
			if pending, deadline, _ = member(); pending == 0 {
				t.Fatalf("strike %d: re-pushed without an ACK armed", strike)
			}
		}
	}
	if walkToBob() || pending != 0 {
		t.Fatalf("a stalled member was pushed again (queue %d, pending %08x)", svc.pipeline.Queue.Len(), pending)
	}

	// Bob speaks: a keep-alive clears the strikes and the push resumes.
	req, _ := mesh.BuildRequest(bob.id, svc.id.PubKey[:], bob.secret, uint32(time.Now().Unix())+10, mesh.FrameKeepAliveRequest(0))
	req.Header = mesh.MakeHeader(mesh.RouteDirect, mesh.PayloadTypeReq, mesh.PayloadVer1)
	hear(t, svc, req)
	if ack := emissionPacket(queued(t, svc)); ack.PayloadType() != mesh.PayloadTypeAck {
		t.Fatalf("keep-alive answered with %v", ack.PayloadType())
	}
	if _, _, failures := member(); failures != 0 {
		t.Fatalf("a keep-alive left %d strikes", failures)
	}
	now = now.Add(time.Minute)
	if !walkToBob() {
		t.Fatal("the push did not resume after the keep-alive")
	}
	if push := emissionPacket(queued(t, svc)); push.PayloadType() != mesh.PayloadTypeTxtMsg {
		t.Fatalf("the push did not resume: %v", push.PayloadType())
	}
}

// A stranger's blank word reaches the doors and, with allow_read_only,
// earns a guest — the reference's behaviour; an empty admin_password
// is a closed door, not the open one the reference's strcmp("", "")
// leaves.
func TestABlankWordOpensOnlyTheReadOnlyDoor(t *testing.T) {
	open := benchRoomTuned(t, nil, func(cfg map[string]any) { cfg["admin_password"] = "" })
	stranger := newClient(t, open)
	if lr, _ := mesh.ParseLoginReply(openReply(t, stranger, login(t, open, stranger, "", 0))); lr.Result != mesh.LoginOK ||
		!lr.Guest || lr.IsAdmin {
		t.Fatalf("a blank word under allow_read_only = %+v, want a guest", lr)
	}
	closed := benchRoomTuned(t, nil, func(cfg map[string]any) {
		cfg["admin_password"], cfg["allow_read_only"] = "", false
	})
	other := newClient(t, closed)
	pkt, _, err := mesh.BuildRoomLoginReq(other.id, closed.id.PubKey[:], uint32(time.Now().Unix())-10, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	hear(t, closed, pkt)
	nothingQueued(t, closed)
	closed.mu.Lock()
	defer closed.mu.Unlock()
	if closed.table.Get(other.id.PubKey[:]) != nil {
		t.Fatal("a blank word opened a closed admin door")
	}
}

// The push clock runs on its own goroutine: a post old enough is pushed
// without anyone calling the clock, and the flush tick writes the
// cursors the logins dirtied.
func TestThePushClockRunsAndTheFlushTickWritesCursors(t *testing.T) {
	ctx := context.Background()
	store := memoryStore(t)
	svc := benchRoom(t, store)
	svc.delays.flush = 20 * time.Millisecond
	alice, bob := newClient(t, svc), newClient(t, svc)
	login(t, svc, alice, "welcome", 0)
	login(t, svc, bob, "welcome", 0)
	sendPost(t, svc, alice, "hello", time.Now())
	svc.mu.Lock()
	// Aged past the settling delay, and the clock told not to wait
	// out the notify pause a login buys.
	svc.posts[0].at -= uint32(2 * postSyncDelay / time.Second)
	svc.nextPush = time.Time{}
	svc.mu.Unlock()

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = svc.Run(runCtx)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		s := svc.Info().Summary
		cursors, err := store.LoadRoomCursors(ctx, "lobby")
		if err != nil {
			t.Fatal(err)
		}
		if s["pushes"] != "0" && s["dropped"] != "0" && s["pushes pending"] == "0" && len(cursors) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("summary = %v, cursors %d — want a detached push refused and two cursors flushed", s, len(cursors))
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
}

// roomRadio is a device the room can be attached to on the bench: it
// hands out the frames a test feeds it, and reports what it was asked.
type roomRadio struct {
	frames       chan radio.Frame
	configureErr error
	configured   int
}

func (*roomRadio) Envelope() radio.Envelope {
	return radio.Envelope{MaxTxPowerSet: true, MaxTxPowerDBm: 22, ChipMinDBm: -9, ChipMaxDBm: 22}
}

func (r *roomRadio) Configure(radio.Waveform) error {
	r.configured++
	return r.configureErr
}
func (*roomRadio) StartReceive() error { return nil }
func (r *roomRadio) Receive(ctx context.Context) (radio.Frame, error) {
	select {
	case <-ctx.Done():
		return radio.Frame{}, ctx.Err()
	case f := <-r.frames:
		if f.Payload == nil {
			return radio.Frame{}, radio.ErrCorrupt
		}
		return f, nil
	}
}
func (*roomRadio) NoiseFloor() (radio.NoiseFloor, bool) { return radio.NoiseFloor{DBm: -110}, true }
func (*roomRadio) NoiseStarved() uint64                 { return 0 }
func (*roomRadio) ChipStats() (radio.ChipStats, bool)   { return radio.ChipStats{}, false }
func (*roomRadio) Airtime(int) time.Duration            { return 10 * time.Millisecond }
func (*roomRadio) Close() error                         { return nil }
func (*roomRadio) AssessChannel(context.Context, float64) (bool, error) {
	return false, nil
}
func (*roomRadio) Transmit(_ context.Context, _ []byte, power int8) (radio.TxReport, error) {
	return radio.TxReport{At: time.Now(), Airtime: 10 * time.Millisecond, PowerDBm: power}, nil
}

func waitRF(t *testing.T, svc *service, want application.RFState) application.Info {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		info := svc.Info()
		if info.RF == want {
			return info
		}
		if time.Now().After(deadline) {
			t.Fatalf("RF state = %s (%s), want %s", info.RF, info.RFCause, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// The room's radio is a capability the manager supplies and withdraws
// while the room keeps running: attached, it hears frames and counts
// the corrupt ones; a radio that will not take the waveform is down
// with its cause; withdrawn, the room is detached, not broken.
func TestTheRoomFollowsItsRadioWithoutStopping(t *testing.T) {
	svc := benchRoom(t, nil)
	dev := &roomRadio{frames: make(chan radio.Frame, 4)}
	driver := radio.Driver{
		Inspect: func(map[string]any) (radio.Envelope, error) { return dev.Envelope(), nil },
		Open:    func(map[string]any, *zap.Logger) (radio.Device, error) { return dev, nil },
	}
	controller, err := radio.NewController("slot1", driver, nil, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go controller.Run(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = svc.Run(ctx)
	}()
	defer func() { cancel(); <-done }()

	if info := svc.Info(); info.RF != application.RFDetached {
		t.Fatalf("a room without a radio is %s, want detached", info.RF)
	}
	demand, err := asks(baseConfig())
	if err != nil || demand.Waveform != svc.p.Waveform || demand.PowerDBm != svc.p.TXPowerDBm {
		t.Fatalf("radio demand = %+v, %v", demand, err)
	}
	if got := svc.RadioDemand(); got.Waveform != svc.p.Waveform {
		t.Fatalf("live radio demand = %+v", got)
	}

	binding, err := controller.Bind("lobby", radio.RoleApplication, svc.p.Waveform)
	if err != nil {
		t.Fatal(err)
	}
	svc.AttachRadio("slot1", binding, radio.NewAirtimeLedger(0, nil), "")
	waitRF(t, svc, application.RFActive)

	alice := newClient(t, svc)
	pkt, _, err := mesh.BuildRoomLoginReq(alice.id, svc.id.PubKey[:], uint32(time.Now().Unix())-10, 0, "welcome")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := pkt.MarshalBinary()
	dev.frames <- radio.Frame{Payload: raw, Correlation: correlation.New(), RSSI: -80, SNR: 6}
	dev.frames <- radio.Frame{} // the fake's word for a corrupt reception
	// The room runs whole here — its TX loop drains the queue through
	// the shadow gate — so the reply shows as one frame composed and
	// one sent, not as an emission the test could take.
	deadline := time.Now().Add(2 * time.Second)
	for {
		s := svc.Info().Summary
		if s["heard"] == "1" && s["corrupt"] == "1" && s["composed"] == "1" && s["sent"] == "1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("summary = %v, want one heard, one corrupt, one composed and sent", s)
		}
		time.Sleep(time.Millisecond)
	}
	svc.mu.Lock()
	lastRSSI, lastSNR := svc.lastRSSI, svc.lastSNR
	svc.mu.Unlock()
	if lastRSSI != -80 || lastSNR != 6 {
		t.Fatalf("last RSSI/SNR = %v/%v", lastRSSI, lastSNR)
	}

	// Withdrawn: detached, still running, its members kept.
	svc.AttachRadio("", nil, nil, "")
	binding.Unbind()
	info := waitRF(t, svc, application.RFDetached)
	if info.State != application.StateRunning || info.Summary["sessions"] != "1" {
		t.Fatalf("detached room = %s, %v", info.State, info.Summary)
	}

	// A radio that refuses the waveform: down, with the cause.
	dev.configureErr = errors.New("frequency outside the envelope")
	again, err := controller.Bind("lobby", radio.RoleApplication, svc.p.Waveform)
	if err != nil {
		t.Fatal(err)
	}
	svc.AttachRadio("slot1", again, nil, "")
	// The attachment itself reads down until the session is tried; the
	// cause arrives when the radio refuses the waveform.
	deadline = time.Now().Add(2 * time.Second)
	for {
		info := svc.Info()
		if info.RF == application.RFDown && info.RFCause != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a refused waveform left no cause: %+v", info)
		}
		time.Sleep(time.Millisecond)
	}
}
