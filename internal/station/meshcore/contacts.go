package meshcore

import (
	"errors"
	"sort"
	"time"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

func (s *service) orderedContacts() []contactEntry {
	out := make([]contactEntry, 0, len(s.contacts))
	for _, entry := range s.contacts {
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].order < out[j].order })
	return out
}

func (s *service) getContacts(command companion.GetContacts) []companion.Response {
	responses := make([]companion.Response, 0, len(s.contacts)+2)
	responses = append(responses, companion.ContactsStart{Count: uint32(len(s.contacts))})
	var mostRecent uint32
	for _, entry := range s.orderedContacts() {
		if command.HasSince && entry.info.LastModifiedUnix <= command.Since {
			continue
		}
		responses = append(responses, companion.ContactResponse{Contact: entry.info})
		mostRecent = max(mostRecent, entry.info.LastModifiedUnix)
	}
	responses = append(responses, companion.EndOfContacts{MostRecent: mostRecent})
	return responses
}

func (s *service) getContact(publicKey [mesh.PubKeySize]byte) []companion.Response {
	entry, exists := s.contacts[publicKey]
	if !exists {
		return errorResponses(companion.ErrorNotFound)
	}
	return []companion.Response{companion.ContactResponse{Contact: entry.info}}
}

func (s *service) addUpdateContact(command companion.AddUpdateContact) []companion.Response {
	entry, exists := s.contacts[command.PublicKey]
	if !exists && len(s.contacts) >= s.p.MaxContacts {
		return errorResponses(companion.ErrorTableFull)
	}
	lastModified := uint32(time.Now().Add(s.clockDelta).Unix())
	if command.HasLastModified {
		lastModified = command.LastModifiedUnix
	}
	info := command.Contact
	info.LastModifiedUnix = lastModified
	if !command.HasLocation {
		info.LatitudeE6, info.LongitudeE6 = 0, 0
	}
	if exists {
		entry.info = info
		entry.ephemeral = false
	} else {
		s.nextContact++
		entry = contactEntry{info: info, order: s.nextContact}
	}
	s.contacts[command.PublicKey] = entry
	return okResponses()
}

func (s *service) resetContactPath(publicKey [mesh.PubKeySize]byte) []companion.Response {
	entry, exists := s.contacts[publicKey]
	if !exists {
		return errorResponses(companion.ErrorNotFound)
	}
	entry.info.PathLen = 0xff
	entry.info.Path = [mesh.MaxPathSize]byte{}
	s.contacts[publicKey] = entry
	return okResponses()
}

func (s *service) removeContact(publicKey [mesh.PubKeySize]byte) []companion.Response {
	if _, exists := s.contacts[publicKey]; !exists {
		return errorResponses(companion.ErrorNotFound)
	}
	delete(s.contacts, publicKey)
	return okResponses()
}

func (s *service) exportContact(command companion.ExportContact) []companion.Response {
	if command.Self {
		packet, err := s.selfAdvert(time.Now().Add(s.clockDelta))
		if err != nil {
			return errorResponses(companion.ErrorTableFull)
		}
		raw, err := packet.MarshalBinary()
		if err != nil {
			return errorResponses(companion.ErrorFileIO)
		}
		return []companion.Response{companion.ExportedContact{Packet: raw}}
	}
	entry, exists := s.contacts[command.PublicKey]
	if !exists || len(entry.advert) == 0 {
		return errorResponses(companion.ErrorNotFound)
	}
	return []companion.Response{companion.ExportedContact{Packet: append([]byte(nil), entry.advert...)}}
}

func (s *service) importContact(raw []byte) []companion.Response {
	packet, err := mesh.ParsePacket(raw)
	if err != nil || packet.PayloadType() != mesh.PayloadTypeAdvert {
		return errorResponses(companion.ErrorIllegalArgument)
	}
	if _, _, err := s.storeAdvert(packet, false); err != nil {
		return errorResponses(companion.ErrorIllegalArgument)
	}
	return okResponses()
}

func (s *service) shouldAutoAdd(advertType uint8) bool {
	if !s.p.ManualContacts {
		return true
	}
	if advertType < mesh.AdvTypeChat || advertType > mesh.AdvTypeSensor {
		return false
	}
	return s.autoFlags&(1<<advertType) != 0
}

func (s *service) evictOldestContact() bool {
	var oldestKey [mesh.PubKeySize]byte
	var oldest contactEntry
	found := false
	for key, entry := range s.contacts {
		if entry.info.Flags&1 != 0 {
			continue
		}
		if !found || entry.info.LastModifiedUnix < oldest.info.LastModifiedUnix ||
			(entry.info.LastModifiedUnix == oldest.info.LastModifiedUnix && entry.order < oldest.order) {
			oldestKey, oldest, found = key, entry, true
		}
	}
	if found {
		delete(s.contacts, oldestKey)
	}
	return found
}

// storeAdvert verifies and updates a contact from an advert. enforceReplay is
// true for actual RF reception and false for the reference's explicit import
// loopback, which removes the packet from its duplicate table first.
func (s *service) storeAdvert(packet *mesh.Packet, enforceReplay bool) (stored, created bool, err error) {
	advert, err := mesh.ParseAdvert(packet.Payload)
	if err != nil {
		return false, false, err
	}
	if advert.Data.Name == "" {
		return false, false, errors.New("advert has no name")
	}
	key := advert.Identity.PubKey
	entry, exists := s.contacts[key]
	if exists && enforceReplay && uint32(advert.Timestamp.Unix()) <= entry.info.LastAdvertUnix {
		return false, false, nil
	}
	if !exists {
		if !s.shouldAutoAdd(advert.Data.Type) ||
			(s.autoHops > 0 && packet.PathHashCount() >= int(s.autoHops)) {
			return false, false, nil
		}
		if len(s.contacts) >= s.p.MaxContacts &&
			(s.autoFlags&1 == 0 || !s.evictOldestContact()) {
			return false, false, nil
		}
		s.nextContact++
		entry.order = s.nextContact
	}
	entry.info.PublicKey = key
	entry.info.Type = advert.Data.Type
	entry.info.Name = advert.Data.Name
	entry.info.LastAdvertUnix = uint32(advert.Timestamp.Unix())
	entry.info.LastModifiedUnix = uint32(time.Now().Add(s.clockDelta).Unix())
	if advert.Data.HasLoc {
		entry.info.LatitudeE6, entry.info.LongitudeE6 = advert.Data.LatE6, advert.Data.LonE6
	}
	entry.info.PathLen = packet.PathLen
	entry.info.Path = [mesh.MaxPathSize]byte{}
	copy(entry.info.Path[:], packet.Path)
	entry.ephemeral = false

	// Export/share stores a plain, zero-path FLOOD advert exactly like the
	// reference blob store, independent of the scope/path by which it arrived.
	copyPacket := *packet
	copyPacket.Payload = append([]byte(nil), packet.Payload...)
	copyPacket.Path = nil
	copyPacket.Header = mesh.MakeHeader(mesh.RouteFlood, mesh.PayloadTypeAdvert, packet.PayloadVer())
	copyPacket.TransportCodes = [2]uint16{}
	copyPacket.SetPathHashSizeAndCount(packet.PathHashSize(), 0)
	entry.advert, err = copyPacket.MarshalBinary()
	if err != nil {
		return false, false, err
	}
	s.contacts[key] = entry
	return true, !exists, nil
}

func (s *service) selfAdvert(at time.Time) (*mesh.Packet, error) {
	data := &mesh.AdvertData{Type: mesh.AdvTypeChat, Name: s.p.NodeName}
	if s.p.AdvertLoc != 0 && (s.p.NodeLat != 0 || s.p.NodeLon != 0) {
		data.HasLoc = true
		data.LatE6 = int32(s.p.NodeLat * 1e6)
		data.LonE6 = int32(s.p.NodeLon * 1e6)
	}
	return mesh.BuildAdvert(s.id, at, data)
}
