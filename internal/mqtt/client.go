package mqtt

// The broker connection itself: a thin coat over the Paho client.
// Reconnection, keepalive and TLS are exactly the wheels not worth
// reinventing; what stays ours is the policy — bounded waits, offline
// refusals counted rather than queued without end.

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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

// Broker is the production Sink.
type Broker struct {
	client paho.Client
	log    *zap.Logger
}

// Dial connects to one broker. The URL scheme picks the transport —
// tcp, ssl, ws, wss — and the client identity is unique per session,
// because two sessions under one id evict each other forever.
func Dial(url, username, password, instance string, log *zap.Logger) (*Broker, error) {
	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, err
	}
	opts := paho.NewClientOptions().
		AddBroker(url).
		SetClientID(fmt.Sprintf("lotor-%s-%s", instance, hex.EncodeToString(salt[:]))).
		SetUsername(username).
		SetPassword(password).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(10 * time.Second).
		SetMaxReconnectInterval(2 * time.Minute).
		SetKeepAlive(2 * time.Minute).
		SetConnectTimeout(connectWait).
		SetOrderMatters(false)
	opts.OnConnect = func(paho.Client) {
		log.Info("observer broker connected", zap.String("url", url))
	}
	opts.OnConnectionLost = func(_ paho.Client, err error) {
		log.Warn("observer broker lost", zap.String("url", url), zap.Error(err))
	}
	b := &Broker{client: paho.NewClient(opts), log: log}
	// Connect in the background: a broker that is down at start must
	// not hold the daemon's assembly, and Paho's retry keeps dialing.
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
