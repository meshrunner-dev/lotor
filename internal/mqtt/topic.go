package mqtt

// The topic template: one mechanism for every layout the ecosystem
// uses. The default spells the meshcore layout; a meshrank uplink or
// anything custom is the same placeholders in another order.

import (
	"fmt"
	"strings"
)

// DefaultTopic is the ecosystem's standard layout.
const DefaultTopic = "meshcore/{iata}/{device}/{type}"

// The message classes a topic is built for.
const (
	TopicStatus    = "status"
	TopicPackets   = "packets"
	TopicRaw       = "raw"
	TopicNeighbors = "neighbors"
)

// DeviceID is a node identity as the shared brokers spell it: hex in
// UPPERCASE. The topic's device level, the payloads' origin ids, the
// neighbour pubkeys and the pubkey-derived usernames all speak this
// one case — while frame bytes and path hops stay lowercase. One
// function, so the two hex vocabularies cannot blur into each other.
func DeviceID(pubKeyHex string) string {
	return strings.ToUpper(pubKeyHex)
}

// BuildTopic expands the template for one message class. Unknown
// placeholders pass through verbatim — the reference does the same —
// but a topic that still carries braces, or came out empty, or names
// a level with nothing in it, is refused: a half-expanded topic
// published anyway would scatter messages under literal "{iata}".
func BuildTopic(template, iata, device, token, class string) (string, error) {
	r := strings.NewReplacer(
		"{iata}", iata,
		"{device}", device,
		"{token}", token,
		"{type}", class,
	)
	topic := r.Replace(template)
	switch {
	case topic == "":
		return "", fmt.Errorf("topic template %q expands to nothing", template)
	case strings.Contains(topic, "{") || strings.Contains(topic, "}"):
		return "", fmt.Errorf("topic %q still carries a placeholder", topic)
	case strings.Contains(topic, "//") || strings.HasPrefix(topic, "/") ||
		strings.HasSuffix(topic, "/"):
		return "", fmt.Errorf("topic %q has an empty level — a placeholder expanded to nothing", topic)
	case strings.ContainsAny(topic, "+#"):
		return "", fmt.Errorf("topic %q carries an MQTT wildcard", topic)
	}
	return topic, nil
}
