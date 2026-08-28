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
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
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
	opts := paho.NewClientOptions().
		AddBroker(o.URL).
		SetClientID(fmt.Sprintf("lotor-%s-%s", o.Instance, hex.EncodeToString(salt[:]))).
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
		log.Info("observer broker connected", zap.String("url", o.URL))
		if o.OnConnect != nil {
			o.OnConnect()
		}
	}
	opts.OnConnectionLost = func(_ paho.Client, err error) {
		log.Warn("observer broker lost", zap.String("url", o.URL), zap.Error(err))
	}
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
func (b *Broker) Close() { b.client.Disconnect(250) }
