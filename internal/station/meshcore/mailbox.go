package meshcore

import "meshrunner.dev/pkg/meshcore/companion"

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
