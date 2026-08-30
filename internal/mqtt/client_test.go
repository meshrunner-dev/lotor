package mqtt

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

// miniBroker speaks just enough MQTT 3.1.1 to prove the client does:
// CONNECT earns its CONNACK, PUBLISH is recorded (and acked at QoS 1),
// PINGREQ its PINGRESP. Anything more is not this test's business.
type miniBroker struct {
	ln net.Listener

	mu   sync.Mutex
	msgs []miniMsg
}

type miniMsg struct {
	topic   string
	qos     byte
	payload []byte
}

func startMiniBroker(t *testing.T) *miniBroker {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &miniBroker{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go b.serve(conn)
		}
	}()
	return b
}

func (b *miniBroker) url() string { return "tcp://" + b.ln.Addr().String() }

func (b *miniBroker) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.msgs)
}

func (b *miniBroker) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	for {
		head := make([]byte, 1)
		if _, err := io.ReadFull(conn, head); err != nil {
			return
		}
		length, ok := readRemaining(conn)
		if !ok {
			return
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		switch head[0] >> 4 {
		case 1: // CONNECT
			if _, err := conn.Write([]byte{0x20, 2, 0, 0}); err != nil {
				return
			}
		case 3: // PUBLISH
			qos := (head[0] >> 1) & 3
			topicLen := int(binary.BigEndian.Uint16(body))
			topic := string(body[2 : 2+topicLen])
			rest := body[2+topicLen:]
			var payload []byte
			if qos > 0 {
				id := rest[:2]
				payload = rest[2:]
				if _, err := conn.Write([]byte{0x40, 2, id[0], id[1]}); err != nil {
					return
				}
			} else {
				payload = rest
			}
			b.mu.Lock()
			b.msgs = append(b.msgs, miniMsg{topic, qos, append([]byte(nil), payload...)})
			b.mu.Unlock()
		case 12: // PINGREQ
			if _, err := conn.Write([]byte{0xD0, 0}); err != nil {
				return
			}
		case 14: // DISCONNECT
			return
		}
	}
}

func readRemaining(conn net.Conn) (int, bool) {
	value, shift := 0, 0
	for {
		b := make([]byte, 1)
		if _, err := io.ReadFull(conn, b); err != nil {
			return 0, false
		}
		value |= int(b[0]&0x7f) << shift
		if b[0]&0x80 == 0 {
			return value, true
		}
		shift += 7
		if shift > 21 {
			return 0, false
		}
	}
}

func TestBrokerSpeaksRealMQTT(t *testing.T) {
	srv := startMiniBroker(t)
	connected := make(chan struct{}, 1)
	broker, err := Dial(Options{
		URL: srv.url(), Instance: "test", Keepalive: 55 * time.Second,
		OnConnect: func() {
			select {
			case connected <- struct{}{}:
			default:
			}
		},
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	deadline := time.After(5 * time.Second)
	select {
	case <-connected:
	case <-deadline:
		t.Fatal("the client never signalled its connection")
	}
	if !broker.Connected() {
		t.Fatal("signalled connected, reports not")
	}
	if err := broker.Publish("meshcore/PAR/feed/packets", 0, false, []byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish("meshcore/PAR/feed/status", 1, false, []byte(`{"status":"online"}`)); err != nil {
		t.Fatal(err)
	}
	for srv.count() < 2 {
		select {
		case <-deadline:
			t.Fatalf("broker recorded %d messages", srv.count())
		case <-time.After(10 * time.Millisecond):
		}
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.msgs[0].topic != "meshcore/PAR/feed/packets" || srv.msgs[0].qos != 0 {
		t.Errorf("first message: %+v", srv.msgs[0])
	}
	if srv.msgs[1].topic != "meshcore/PAR/feed/status" || srv.msgs[1].qos != 1 ||
		!strings.Contains(string(srv.msgs[1].payload), "online") {
		t.Errorf("second message: %+v", srv.msgs[1])
	}
}

// TestConnectionLadderWalksTheWholeTimeline feeds the ladder the
// notifications Paho would emit across an observer's whole life —
// broker absent at start, then an immediate connect, then flapping —
// and reads the transitions back in order. Paho numbers a round's
// attempts from zero, so only attempt 0 opens a round; the retries
// inside it stay silent.
func TestConnectionLadderWalksTheWholeTimeline(t *testing.T) {
	var got []string
	notify := func(state, cause string) {
		if cause != "" {
			state += ":" + cause
		}
		got = append(got, state)
	}
	ladder := connectionLadder("tcp://b", notify, zap.NewNop())
	for _, n := range []paho.ConnectionNotification{
		// Broker absent: one round of two attempts, then the round fails.
		paho.ConnectionNotificationConnecting{Attempt: 0},
		paho.ConnectionNotificationBrokerFailed{Reason: errors.New("refused")},
		paho.ConnectionNotificationConnecting{Attempt: 1},
		paho.ConnectionNotificationFailed{Reason: errors.New("refused")},
		// The next round connects at once.
		paho.ConnectionNotificationConnecting{Attempt: 0},
		paho.ConnectionNotificationConnected{},
		// Flapping: lost, a reconnect round with its own retry, back.
		paho.ConnectionNotificationLost{Reason: errors.New("EOF")},
		paho.ConnectionNotificationConnecting{IsReconnect: true, Attempt: 0},
		paho.ConnectionNotificationConnecting{IsReconnect: true, Attempt: 1},
		paho.ConnectionNotificationConnected{},
	} {
		ladder(nil, n)
	}
	want := []string{
		"connecting", "failed:refused", "connecting", "connected",
		"lost:EOF", "reconnecting", "reconnected",
	}
	if len(got) != len(want) {
		t.Fatalf("timeline = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("timeline = %v, want %v", got, want)
		}
	}
}

func TestConnectionLadderRestoresPahoCallbackOrder(t *testing.T) {
	// Paho starts its reconnect goroutine before launching Lost, and
	// Connected/Lost themselves run in independent goroutines. Feed the
	// legal but causally reversed callback order deterministically: the
	// archive must still tell session, loss, reconnect attempt, session.
	var got []string
	ladder := connectionLadder("tcp://b", func(state, cause string) {
		got = append(got, state)
	}, zap.NewNop())
	for _, n := range []paho.ConnectionNotification{
		paho.ConnectionNotificationConnecting{Attempt: 0},
		paho.ConnectionNotificationConnecting{IsReconnect: true, Attempt: 0},
		paho.ConnectionNotificationFailed{Reason: errors.New("refused")},
		paho.ConnectionNotificationLost{Reason: errors.New("EOF")},
		paho.ConnectionNotificationConnected{}, // delayed first session
		paho.ConnectionNotificationConnected{}, // re-established session
	} {
		ladder(nil, n)
	}
	wantDelayed := []string{"connecting", "connected", "lost", "reconnecting", "failed", "reconnected"}
	if len(got) != len(wantDelayed) {
		t.Fatalf("timeline = %v, want %v", got, wantDelayed)
	}
	for i := range wantDelayed {
		if got[i] != wantDelayed[i] {
			t.Fatalf("timeline = %v, want %v", got, wantDelayed)
		}
	}

	// The reconnected callback may also win the race and arrive before
	// Lost. It waits behind the loss instead of being written above it.
	got = nil
	ladder = connectionLadder("tcp://b", func(state, cause string) {
		got = append(got, state)
	}, zap.NewNop())
	for _, n := range []paho.ConnectionNotification{
		paho.ConnectionNotificationConnecting{Attempt: 0},
		paho.ConnectionNotificationConnected{},
		paho.ConnectionNotificationConnecting{IsReconnect: true, Attempt: 0},
		paho.ConnectionNotificationConnected{},
		paho.ConnectionNotificationLost{Reason: errors.New("EOF")},
	} {
		ladder(nil, n)
	}
	want := []string{"connecting", "connected", "lost", "reconnecting", "reconnected"}
	if len(got) != len(want) {
		t.Fatalf("early reconnect timeline = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("early reconnect timeline = %v, want %v", got, want)
		}
	}
}
