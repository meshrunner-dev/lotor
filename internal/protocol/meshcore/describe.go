package meshcore

import (
	"encoding/hex"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
)

// keyPrefixLen is how much of a public key the logs and journal show:
// enough to identify a node in a neighbourhood, greppable as a prefix
// of the full key.
const keyPrefixLen = 12

// describe decodes what a frame says when its payload speaks an open
// format — adverts and discovery — and annotates the judgement with
// it. It returns advertOK: false only when the payload is an ADVERT
// whose signature fails to verify, so the verdict can drop it the way
// the reference does. Encrypted payloads stay opaque by design.
func describe(pkt *meshcore.Packet, judged *bus.FrameJudged) (fields []zap.Field, advertOK bool) {
	switch pkt.PayloadType() {
	case meshcore.PayloadTypeAdvert:
		return describeAdvert(pkt, judged)
	case meshcore.PayloadTypeControl:
		return describeControl(pkt, judged), true
	default:
		return nil, true
	}
}

// describeAdvert names the advertising node — after verifying its
// signature, as the reference does before trusting a word of it. Only
// a verified advert populates the identity fields (node, pubkey) that
// feed the sentinel's directory.
func describeAdvert(pkt *meshcore.Packet, judged *bus.FrameJudged) (fields []zap.Field, advertOK bool) {
	adv, err := meshcore.ParseAdvert(pkt.Payload)
	switch {
	case errors.Is(err, meshcore.ErrBadSignature):
		judged.Detail = "advert signature invalid"
		return []zap.Field{zap.String("advert", "signature-invalid")}, false
	case err != nil:
		judged.Detail = "advert malformed"
		return []zap.Field{zap.String("advert", "malformed")}, false
	}

	judged.PubKey = hex.EncodeToString(adv.Identity.PubKey[:])[:keyPrefixLen]
	fields = []zap.Field{zap.String("pubkey", judged.PubKey)}
	if adv.Data != nil {
		judged.Node = adv.Data.Name
		judged.Detail = advTypeName(adv.Data.Type)
		fields = append(fields,
			zap.String("node", adv.Data.Name),
			zap.String("node_type", judged.Detail),
		)
	}
	return fields, true
}

func describeControl(pkt *meshcore.Packet, judged *bus.FrameJudged) []zap.Field {
	if req, err := meshcore.ParseDiscoverReq(pkt); err == nil {
		judged.Detail = "node discovery request"
		if req.Filter.Includes(meshcore.AdvTypeRepeater) {
			judged.Detail += " (repeaters)"
		}
		return []zap.Field{
			zap.String("control", "discover-request"),
			zap.Uint32("tag", req.Tag),
		}
	}
	if resp, err := meshcore.ParseDiscoverResp(pkt); err == nil {
		// A discovery response is unauthenticated — its pubkey is a
		// claim, not a verified identity — so it is described but does
		// NOT populate judged.PubKey, keeping the directory advert-only.
		respKey := hex.EncodeToString(resp.PubKey)[:min(keyPrefixLen, hex.EncodedLen(len(resp.PubKey)))]
		judged.Detail = fmt.Sprintf("discovery response (%s, snr %+.2f dB, unverified key %s)",
			advTypeName(resp.NodeType), resp.SNR, respKey)
		return []zap.Field{
			zap.String("control", "discover-response"),
			zap.String("claimed_pubkey", respKey),
			zap.Float64("resp_snr_db", resp.SNR),
		}
	}
	return nil
}

func advTypeName(t uint8) string {
	switch t {
	case meshcore.AdvTypeChat:
		return "chat"
	case meshcore.AdvTypeRepeater:
		return "repeater"
	case meshcore.AdvTypeRoom:
		return "room"
	default:
		return fmt.Sprintf("type-%d", t)
	}
}
