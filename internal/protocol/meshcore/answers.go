package meshcore

// The bodies of the answers an authenticated client asks for. Each is
// a wire format the reference defines and a companion parses, so they
// live together, apart from the session machinery that decides who is
// allowed to ask.

import (
	"encoding/binary"
	"sort"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// little-endian order.
func (e *engine) statusBody() []byte {
	s := e.stats.snapshot()
	nf := int16(0)
	if e.floor != nil {
		if f, ok := e.floor(); ok {
			nf = int16(f.DBm)
		}
	}
	out := make([]byte, 0, 56)
	u16 := func(v uint16) { out = binary.LittleEndian.AppendUint16(out, v) }
	u32 := func(v uint32) { out = binary.LittleEndian.AppendUint32(out, v) }

	u16(0) // battery millivolts: mains powered, and a lie would be worse
	u16(uint16(e.queueLen()))
	u16(uint16(nf))
	u16(uint16(int16(s.LastRSSI)))
	u32(s.RecvTotal)
	u32(s.SentFlood + s.SentDirect)
	u32(uint32(s.TxAirtime / time.Second))
	u32(uint32(time.Since(e.started) / time.Second))
	u32(s.SentFlood)
	u32(s.SentDirect)
	u32(s.RecvFlood)
	u32(s.RecvDirect)
	u16(0) // error events: none tracked as a bitfield here
	u16(uint16(int16(s.LastSNR * 4)))
	u16(uint16(s.DirectDups))
	u16(uint16(s.FloodDups))
	u32(uint32(s.RxAirtime / time.Second))
	u32(s.RecvErrors)
	return out
}

// telemetryBody reports what this node can honestly measure about
// itself, in the Cayenne encoding companions expect.
func (e *engine) telemetryBody() []byte {
	enc := meshcore.NewLPPEncoder()
	if c, ok := hostTemperature(); ok {
		_ = enc.Add(meshcore.LPPReading{
			Channel: telemChannelSelf, Type: meshcore.LPPTemperature, Value: c,
		})
	}
	return enc.Bytes()
}

// neighboursBody answers the neighbourhood query: the total known, how
// many are returned, then each one's key prefix, how long ago it was
// heard, and the SNR it was heard at.
func (e *engine) neighboursBody(args []byte) []byte {
	if len(args) < 7 || args[1] != 0 {
		return nil // only version 0 exists
	}
	count := int(args[2])
	offset := int(binary.LittleEndian.Uint16(args[3:5]))
	orderBy := args[5]
	prefixLen := min(int(args[6]), meshcore.PubKeySize)

	all := e.neighbours.snapshot() // newest heard first
	switch orderBy {
	case 1: // oldest to newest
		sort.SliceStable(all, func(i, j int) bool { return all[i].Heard.Before(all[j].Heard) })
	case 2: // strongest to weakest
		sort.SliceStable(all, func(i, j int) bool { return all[i].SNR > all[j].SNR })
	case 3: // weakest to strongest
		sort.SliceStable(all, func(i, j int) bool { return all[i].SNR < all[j].SNR })
	}

	// The reference bounds its results buffer; so does this, and the
	// count it reports is what actually fits.
	const maxResults = 130
	entry := prefixLen + 5
	var rows []byte
	returned := 0
	now := time.Now()
	for i := offset; i < len(all) && returned < count; i++ {
		if len(rows)+entry > maxResults {
			break
		}
		n := all[i]
		rows = append(rows, n.PubKey[:prefixLen]...)
		rows = binary.LittleEndian.AppendUint32(rows, uint32(now.Sub(n.Heard)/time.Second))
		rows = append(rows, byte(int8(n.SNR*4)))
		returned++
	}
	out := binary.LittleEndian.AppendUint16(nil, uint16(len(all)))
	out = binary.LittleEndian.AppendUint16(out, uint16(returned))
	return append(out, rows...)
}
