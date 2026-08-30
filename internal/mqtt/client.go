package mqtt

// The broker connection itself: a thin coat over the Paho client.
// Reconnection, keepalive and TLS are exactly the wheels not worth
// reinventing; what stays ours is the policy — bounded waits, offline
// refusals counted rather than queued without end, credentials minted
// fresh at each connect when the broker authenticates the device.

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/product"
)

// publishWait bounds one delivery: a broker that cannot take a QoS 1
// message in this long has lost it to the counters, not to a growing
// queue.
const publishWait = 5 * time.Second

// connectWait bounds the first dial only; afterwards Paho reconnects
// in the background and Publish answers "not connected" honestly.
const connectWait = 10 * time.Second

// defaultKeepalive suits a plain broker; the ones behind balancers
// override it downward through their preset.
const defaultKeepalive = 2 * time.Minute

// Options is one connection\'s whole decision set.
type Options struct {
	URL      string
	Instance string // names the client id, unique per session

	Username string
	Password string
	// Credentials, when set, is asked at every connect — how a minted
	// device token stays fresh across reconnects. It replaces the
	// static pair above.
	Credentials func() (username, password string)

	Keepalive time.Duration
	// CAFile pins the broker\'s chain to one PEM file; empty trusts
	// the system roots.
	CAFile string

	// OnConnect is told about every established session, first and
	// re-established alike — the observer\'s cue to speak.
	OnConnect func()
	// OnTransition is told the connection's real life — connected,
	// reconnected, lost — with its cause, so the archive can date an
	// outage instead of trusting "the goroutine started" for "up".
	OnTransition func(state, cause string)
}

// connectionStory restores causality across Paho's callback goroutines.
// Paho starts reconnecting before it launches Lost, and Connected/Lost
// themselves run independently: a mutex alone serialises scheduler
// order, not connection order.
type connectionStory struct {
	mu sync.Mutex

	brokerURL string
	notify    func(state, cause string)
	log       *zap.Logger

	sessions         uint32
	connected        bool
	reconnectPending bool
	connectedPending int
	staleConnected   int
	failuresPending  []string
}

// connectionLadder tells the connection's life as it happens:
// attempts and their failures in debug — the socket's abnormal life —
// and the state changes an operator acts on in info: connected,
// lost, reconnected.
func connectionLadder(brokerURL string, notify func(state, cause string),
	log *zap.Logger,
) paho.ConnectionNotificationHandler {
	story := &connectionStory{brokerURL: brokerURL, notify: notify, log: log}
	return story.handle
}

func (s *connectionStory) tell(state, cause string) {
	if s.notify != nil {
		s.notify(state, cause)
	}
}

func (s *connectionStory) handle(_ paho.Client, n paho.ConnectionNotification) {
	s.mu.Lock()
	defer s.mu.Unlock()
	url := zap.String("url", s.brokerURL)
	switch e := n.(type) {
	case paho.ConnectionNotificationConnecting:
		s.connecting(e, url)
	case paho.ConnectionNotificationBrokerFailed:
		logging.Trace(s.log, "observer broker attempt failed", url, zap.Error(e.Reason))
	case paho.ConnectionNotificationFailed:
		s.failed(e, url)
	case paho.ConnectionNotificationConnected:
		s.established()
	case paho.ConnectionNotificationLost:
		s.lost(e, url)
	}
}

func (s *connectionStory) connecting(e paho.ConnectionNotificationConnecting, url zap.Field) {
	logging.Trace(s.log, "observer broker dialing", url,
		zap.Int("attempt", e.Attempt), zap.Bool("reconnect", e.IsReconnect))
	// Paho numbers one round from zero. Retries within the round are
	// socket noise rather than new lifecycle transitions.
	if e.Attempt != 0 {
		return
	}
	if !e.IsReconnect {
		s.tell("connecting", "")
		return
	}
	if s.connected || s.sessions == 0 {
		// Lost has not run yet, or even the first Connected callback
		// is still waiting for a scheduler. Hold the successor behind it.
		s.reconnectPending = true
		return
	}
	s.tell("reconnecting", "")
}

func (s *connectionStory) failed(e paho.ConnectionNotificationFailed, url zap.Field) {
	logging.Trace(s.log, "observer broker round failed — backing off", url, zap.Error(e.Reason))
	cause := errorText(e.Reason)
	if s.reconnectPending {
		s.failuresPending = append(s.failuresPending, cause)
		return
	}
	s.tell("failed", cause)
}

func (s *connectionStory) established() {
	if s.staleConnected > 0 {
		// Lost reconstructed a session whose Connected callback had not
		// run. This is that late callback, not another session.
		s.staleConnected--
		return
	}
	if s.reconnectPending && s.connected {
		s.connectedPending++
		return
	}
	if !s.connected {
		s.emitConnected()
	}
}

func (s *connectionStory) lost(e paho.ConnectionNotificationLost, url zap.Field) {
	s.log.Info("observer broker lost", url, zap.Error(e.Reason))
	if !s.connected && s.sessions == 0 {
		// Lost proves the first session existed. Tell its beginning
		// before its end and ignore its late callback when it arrives.
		s.emitConnected()
		s.staleConnected++
	}
	if s.connected {
		s.tell("lost", errorText(e.Reason))
		s.connected = false
	}
	if s.reconnectPending {
		s.tell("reconnecting", "")
		s.reconnectPending = false
	}
	for _, cause := range s.failuresPending {
		s.tell("failed", cause)
	}
	s.failuresPending = nil
	if s.connectedPending > 0 {
		s.connectedPending--
		s.emitConnected()
	}
}

func (s *connectionStory) emitConnected() {
	word, state := "observer broker connected", "connected"
	if s.sessions > 0 {
		word, state = "observer broker reconnected", "reconnected"
	}
	s.sessions++
	s.connected = true
	s.log.Info(word, zap.String("url", s.brokerURL))
	s.tell(state, "")
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Broker is the production Sink.
type Broker struct {
	client paho.Client
	log    *zap.Logger
}

// Dial connects to one broker. The URL scheme picks the transport —
// tcp, ssl, ws, wss — and the client identity is unique per session,
// because two sessions under one id evict each other forever.
func Dial(o Options, log *zap.Logger) (*Broker, error) {
	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, err
	}
	keepalive := o.Keepalive
	if keepalive <= 0 {
		keepalive = defaultKeepalive
	}
	clientID := fmt.Sprintf("%s-%s-%s", product.Slug, o.Instance, hex.EncodeToString(salt[:]))
	auth := "anonymous"
	if o.Credentials != nil {
		auth = "dynamic"
	} else if o.Username != "" {
		auth = "static"
	}
	logging.Trace(log, "observer broker client configured",
		zap.String("url", o.URL), zap.String("client_id", clientID),
		zap.Duration("keepalive", keepalive), zap.String("auth", auth),
		zap.Bool("custom_ca", o.CAFile != ""))
	opts := paho.NewClientOptions().
		AddBroker(o.URL).
		SetClientID(clientID).
		SetUsername(o.Username).
		SetPassword(o.Password).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(10 * time.Second).
		SetMaxReconnectInterval(2 * time.Minute).
		SetKeepAlive(keepalive).
		SetConnectTimeout(connectWait).
		SetWriteTimeout(publishWait).
		SetOrderMatters(false)
	if o.Credentials != nil {
		opts.SetCredentialsProvider(o.Credentials)
	}
	if o.CAFile != "" {
		pem, err := os.ReadFile(o.CAFile) // #nosec G304 -- the operator names the pin
		if err != nil {
			return nil, fmt.Errorf("broker ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("broker ca: no certificate in %s", o.CAFile)
		}
		opts.SetTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
	}
	opts.OnConnect = func(paho.Client) {
		if o.OnConnect != nil {
			o.OnConnect()
		}
	}
	opts.OnConnectionNotification = connectionLadder(o.URL, o.OnTransition, log)
	b := &Broker{client: paho.NewClient(opts), log: log}
	// Connect in the background: a broker that is down at start must
	// not hold the daemon\'s assembly, and Paho\'s retry keeps dialing.
	b.client.Connect()
	return b, nil
}

// Publish delivers one message, bounded.
func (b *Broker) Publish(topic string, qos byte, retain bool, payload []byte) error {
	if !b.client.IsConnectionOpen() {
		return errors.New("broker not connected")
	}
	token := b.client.Publish(topic, qos, retain, payload)
	if !token.WaitTimeout(publishWait) {
		return errors.New("broker took too long")
	}
	return token.Error()
}

// Connected reports whether the session is up right now.
func (b *Broker) Connected() bool { return b.client.IsConnectionOpen() }

// Close ends the session, letting in-flight messages a moment to
// leave.
func (b *Broker) Close() {
	logging.Trace(b.log, "observer broker client closing")
	b.client.Disconnect(250)
}
