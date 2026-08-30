package meshcore

import (
	"context"

	"go.uber.org/zap"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

func mailboxChannel(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	code := companion.ResponseCode(payload[0])
	return code == companion.ResponseChannelMessage || code == companion.ResponseChannelMessageV3 ||
		code == companion.ResponseChannelData
}

// enqueueMailboxLocked reproduces the reference's overflow priority: an old
// channel broadcast may be displaced, but a direct contact message is never
// silently evicted to admit another item.
func (s *service) enqueueMailboxLocked(response companion.Response) bool {
	payload, err := companion.MarshalResponse(response)
	if err != nil {
		return false
	}
	if len(s.mailbox) >= s.p.MailboxCap {
		replace := -1
		for i, existing := range s.mailbox {
			if mailboxChannel(existing) {
				replace = i
				break
			}
		}
		if replace < 0 {
			return false
		}
		s.mailbox = append(s.mailbox[:replace], s.mailbox[replace+1:]...)
	}
	s.mailbox = append(s.mailbox, payload)
	return true
}

func (s *service) enqueueContactMailbox(ctx context.Context, publicKey [mesh.PubKeySize]byte,
	syncSince uint32, response companion.Response,
) {
	s.mu.Lock()
	before := s.snapshotLocked()
	changed := false
	if entry, exists := s.contacts[publicKey]; exists && syncSince > entry.syncSince {
		entry.syncSince = syncSince
		s.contacts[publicKey] = entry
		changed = true
	}
	accepted := s.enqueueMailboxLocked(response)
	var err error
	if accepted || changed {
		err = s.persistLocked(ctx, before)
	}
	s.mu.Unlock()
	if err != nil {
		s.log.Error("station contact mailbox persistence failed", zap.Error(err))
		return
	}
	if accepted {
		s.push(companion.MessagesWaiting{})
	}
}
