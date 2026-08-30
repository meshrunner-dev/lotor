package meshcore

import (
	"bytes"
	"time"

	"meshrunner.dev/lotor/internal/radio"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

type advertPath struct {
	prefix       [7]byte
	receivedUnix uint32
	pathLen      uint8
	path         [mesh.MaxPathSize]byte
}

func (s *service) sendTrace(command companion.SendTracePath) []companion.Response {
	width := 1 << (command.Flags & 0x03)
	if len(command.Path) == 0 || len(command.Path)%width != 0 || len(command.Path)/width > mesh.MaxPathSize {
		return errorResponses(companion.ErrorIllegalArgument)
	}
	packet, err := mesh.BuildTrace(command.Tag, command.Auth, command.Flags)
	if err != nil || len(packet.Payload)+len(command.Path) > mesh.MaxPacketPayload {
		return errorResponses(companion.ErrorTableFull)
	}
	packet.Payload = append(packet.Payload, command.Path...)
	packet.Header = mesh.MakeHeader(mesh.RouteDirect, mesh.PayloadTypeTrace, packet.PayloadVer())
	packet.SetPathHashSizeAndCount(1, 0)
	packet.Path = nil
	if responses := s.submitLocked(packet, "station-trace"); responses != nil {
		return responses
	}
	return []companion.Response{companion.Sent{
		ExpectedACK: command.Tag, TimeoutMillis: s.estimateTraceTimeout(packet, len(command.Path)/width),
	}}
}

func (s *service) estimateTraceTimeout(packet *mesh.Packet, hashes int) uint32 {
	airtime := time.Duration(0)
	if s.rfDevice != nil {
		airtime = s.rfDevice.Airtime(packet.RawLength())
	}
	perHop := 6*airtime + 250*time.Millisecond
	return uint32((stationTimeoutBase + time.Duration(hashes+1)*perHop).Milliseconds())
}

func (s *service) receiveTrace(packet *mesh.Packet, frame radio.Frame) {
	if !packet.IsRouteDirect() {
		return
	}
	trace, err := mesh.ParseTrace(packet)
	if err != nil || len(trace.SNRx4)*trace.HashWidth < len(trace.Route) || len(trace.Route) > 255 {
		return
	}
	body := make([]byte, 0, 12+len(trace.Route)+len(trace.SNRx4)+1)
	body = append(body, 0, byte(len(trace.Route)), trace.Flags)
	body = appendUint32(body, trace.Tag)
	body = appendUint32(body, trace.AuthCode)
	body = append(body, trace.Route...)
	for _, snr := range trace.SNRx4 {
		body = append(body, byte(snr))
	}
	body = append(body, byte(snrQuarter(frame.SNR)))
	s.push(companion.Push{Code: companion.PushTraceData, Body: body})
}

func appendUint32(dst []byte, value uint32) []byte {
	return append(dst, byte(value), byte(value>>8), byte(value>>16), byte(value>>24))
}

func (s *service) cacheAdvertPathLocked(publicKey [mesh.PubKeySize]byte, packet *mesh.Packet) {
	if !mesh.ValidPathLen(packet.PathLen) || pathByteLen(packet.PathLen) != len(packet.Path) {
		return
	}
	prefix := [7]byte(publicKey[:7])
	selected := 0
	oldest := ^uint32(0)
	for i := range s.advertPaths {
		if s.advertPaths[i].prefix == prefix {
			selected = i
			break
		}
		if s.advertPaths[i].receivedUnix < oldest {
			selected, oldest = i, s.advertPaths[i].receivedUnix
		}
	}
	item := advertPath{prefix: prefix, receivedUnix: uint32(time.Now().Add(s.clockDelta).Unix()), pathLen: packet.PathLen}
	copy(item.path[:], packet.Path)
	s.advertPaths[selected] = item
}

func (s *service) getAdvertPath(publicKey [mesh.PubKeySize]byte) []companion.Response {
	prefix := publicKey[:7]
	for _, item := range s.advertPaths {
		if item.receivedUnix == 0 || !bytes.Equal(item.prefix[:], prefix) {
			continue
		}
		pathBytes := pathByteLen(item.pathLen)
		return []companion.Response{companion.AdvertPath{
			ReceivedUnix: item.receivedUnix, PathLen: item.pathLen,
			Path: append([]byte(nil), item.path[:pathBytes]...),
		}}
	}
	return errorResponses(companion.ErrorNotFound)
}
