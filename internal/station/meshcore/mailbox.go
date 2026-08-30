package meshcore

import (
	"context"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/logging"

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
type mailboxEnqueueResult struct {
	accepted bool
	evicted  bool
	payload  []byte
	corr     correlation.ID
	depth    int
}

func (s *service) enqueueMailboxLocked(response companion.Response,
	corr correlation.ID,
) (mailboxEnqueueResult, error) {
	payload, err := companion.MarshalResponse(response)
	if err != nil {
		return mailboxEnqueueResult{}, err
	}
	result := mailboxEnqueueResult{payload: payload, corr: corr, depth: len(s.mailbox)}
	if len(s.mailbox) >= s.p.MailboxCap {
		replace := -1
		for i, existing := range s.mailbox {
			if mailboxChannel(existing) {
				replace = i
				break
			}
		}
		if replace < 0 {
			return result, nil
		}
		s.mailbox = append(s.mailbox[:replace], s.mailbox[replace+1:]...)
		s.mailboxCorr = append(s.mailboxCorr[:replace], s.mailboxCorr[replace+1:]...)
		result.evicted = true
	}
	s.mailbox = append(s.mailbox, payload)
	s.mailboxCorr = append(s.mailboxCorr, corr)
	result.accepted, result.depth = true, len(s.mailbox)
	return result, nil
}

func (s *service) enqueueContactMailbox(ctx context.Context, publicKey [mesh.PubKeySize]byte,
	syncSince uint32, response companion.Response,
) {
	s.mu.Lock()
	before := s.snapshotLocked()
	corr := correlationFromContext(ctx)
	changed := false
	if entry, exists := s.contacts[publicKey]; exists && syncSince > entry.syncSince {
		entry.syncSince = syncSince
		s.contacts[publicKey] = entry
		changed = true
	}
	result, encodeErr := s.enqueueMailboxLocked(response, corr)
	var err error
	if result.accepted || changed {
		err = s.persistLocked(ctx, before)
	}
	s.mu.Unlock()
	if err != nil {
		s.log.Error("station contact mailbox persistence failed", zap.Error(err))
		return
	}
	if encodeErr != nil {
		s.log.Error("station contact mailbox encode failed", zap.Error(encodeErr))
		return
	}
	s.logMailboxEnqueue(result)
	if changed {
		fields := []zap.Field{zap.String("contact", contactPrefix(publicKey)), zap.Uint32("sync_since", syncSince)}
		if !corr.IsZero() {
			fields = append(fields, zap.String("corr", corr.Short()))
		}
		logging.Trace(s.log, "station mailbox contact cursor advanced", fields...)
	}
	if result.accepted {
		s.push(companion.MessagesWaiting{}, result.corr)
	}
}

func (s *service) logMailboxEnqueue(result mailboxEnqueueResult) {
	fields := []zap.Field{
		zap.String("message", mailboxKind(result.payload)), zap.Int("depth", result.depth),
		zap.Int("capacity", s.p.MailboxCap),
	}
	if !result.corr.IsZero() {
		fields = append(fields, zap.String("corr", result.corr.Short()))
	}
	if !result.accepted {
		logging.Trace(s.log, "station mailbox item refused", append(fields, zap.String("reason", "full"))...)
		return
	}
	fields = append(fields, zap.Bool("evicted_channel", result.evicted))
	logging.Trace(s.log, "station mailbox item enqueued", fields...)
}
