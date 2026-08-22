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
// it. Encrypted payloads stay opaque by design; describing never
// changes a verdict.
func describe(pkt *meshcore.Packet, judged *bus.FrameJudged) []zap.Field {
	switch pkt.PayloadType() {
	case meshcore.PayloadTypeAdvert:
		return describeAdvert(pkt, judged)
	case meshcore.PayloadTypeControl:
		return describeControl(pkt, judged)
	default:
		return nil
	}
}

// describeAdvert names the advertising node — after verifying its
// signature, as the reference does before trusting a word of it.
func describeAdvert(pkt *meshcore.Packet, judged *bus.FrameJudged) []zap.Field {
	adv, err := meshcore.ParseAdvert(pkt.Payload)
	switch {
	case errors.Is(err, meshcore.ErrBadSignature):
		judged.Detail = "advert signature invalid"
		return []zap.Field{zap.String("advert", "signature-invalid")}
	case err != nil:
		judged.Detail = "advert malformed"
		return []zap.Field{zap.String("advert", "malformed")}
	}

	judged.PubKey = hex.EncodeToString(adv.Identity.PubKey[:])[:keyPrefixLen]
	fields := []zap.Field{zap.String("pubkey", judged.PubKey)}
	if adv.Data != nil {
		judged.Node = adv.Data.Name
		judged.Detail = advTypeName(adv.Data.Type)
		fields = append(fields,
			zap.String("node", adv.Data.Name),
			zap.String("node_type", judged.Detail),
		)
	}
	return fields
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
		judged.PubKey = hex.EncodeToString(resp.PubKey)[:min(keyPrefixLen, hex.EncodedLen(len(resp.PubKey)))]
		judged.Detail = fmt.Sprintf("discovery response (%s, snr %+.2f dB)",
			advTypeName(resp.NodeType), resp.SNR)
		return []zap.Field{
			zap.String("control", "discover-response"),
			zap.String("pubkey", judged.PubKey),
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
