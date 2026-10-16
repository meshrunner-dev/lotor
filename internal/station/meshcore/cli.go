package meshcore

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

const cliUnknown = "Unknown command"

func (s *service) runCLI(line string) companion.CLIReply {
	line = strings.TrimLeft(line, " ")
	// An "XX|" prefix comes back on the reply, as in the reference.
	var tag string
	if len(line) > 4 && line[2] == '|' {
		tag, line = line[:3], line[3:]
	}
	return companion.CLIReply{Text: tag + s.cliAnswer(line)}
}

func (s *service) cliAnswer(line string) string {
	switch {
	case line == "get name":
		return "> " + s.p.NodeName
	case strings.HasPrefix(line, "set name "):
		return s.cliSetName(strings.TrimPrefix(line, "set name "))
	case line == "board":
		return stationModel
	case line == "ver":
		return fmt.Sprintf("%s (Build: %s)", s.buildVersion, s.buildDate)
	case line == "get radio":
		return s.cliRadio()
	case strings.HasPrefix(line, "set radio "):
		return s.cliSetRadio(strings.TrimPrefix(line, "set radio "))
	case line == "get tx" || strings.HasPrefix(line, "get tx "):
		return fmt.Sprintf("> %d", s.p.TXPowerDBm)
	case strings.HasPrefix(line, "set tx "):
		return s.cliSetTx(strings.TrimPrefix(line, "set tx "))
	case line == "get af":
		return "> " + cliDecimal(float64(s.p.AirFactorMilli)/1000)
	case strings.HasPrefix(line, "set af "):
		return cliSetMilli(strings.TrimPrefix(line, "set af "), 9, &s.p.AirFactorMilli, "ERROR: af must be 0-9")
	case line == "get rxdelay":
		return "> " + cliDecimal(float64(s.p.RXDelayMilli)/1000)
	case strings.HasPrefix(line, "set rxdelay "):
		return cliSetMilli(strings.TrimPrefix(line, "set rxdelay "), 20, &s.p.RXDelayMilli, "Error, must be 0-20")
	case line == "get path.hash.mode":
		return fmt.Sprintf("> %d", s.p.PathHashMode)
	case strings.HasPrefix(line, "set path.hash.mode "):
		return s.cliSetPathHashMode(strings.TrimPrefix(line, "set path.hash.mode "))
	case line == "get multi.acks":
		return fmt.Sprintf("> %d", s.p.MultiACKs)
	case strings.HasPrefix(line, "set multi.acks "):
		return s.cliSetMultiACKs(strings.TrimPrefix(line, "set multi.acks "))
	}
	return cliUnknown
}

func (s *service) cliSetName(name string) string {
	if !mesh.ValidNodeName(name) {
		return "Error, bad chars"
	}
	if len(name) > maxStationName {
		name = name[:maxStationName]
	}
	s.p.NodeName = name
	return "OK"
}

// The frequency keeps three decimals, as the relay's over-the-air get freq.
func (s *service) cliRadio() string {
	w := s.p.Waveform
	return fmt.Sprintf("> %s,%s,%d,%d",
		strconv.FormatFloat(float64(w.FrequencyHz)/1e6, 'f', 3, 64),
		strconv.FormatFloat(float64(w.BandwidthHz)/1e3, 'f', -1, 64),
		w.SpreadingFactor, w.CodingRate)
}

// A plain OK: the reference's CLI waits for a reboot, this applies at once.
func (s *service) cliSetRadio(args string) string {
	const refused = "Error, invalid radio params"
	parts := strings.SplitN(args, ",", 5)
	if len(parts) < 4 {
		return refused
	}
	mhz, errFreq := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	khz, errBW := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	sf, errSF := strconv.ParseUint(strings.TrimSpace(parts[2]), 10, 8)
	cr, errCR := strconv.ParseUint(strings.TrimSpace(parts[3]), 10, 8)
	if errFreq != nil || errBW != nil || errSF != nil || errCR != nil ||
		!(mhz >= 150 && mhz <= 2500) || !(khz >= 7 && khz <= 500) {
		return refused
	}
	if !accepted(s.setRadioParams(companion.SetRadioParams{
		FrequencyKHz: uint32(math.Round(mhz * 1000)), BandwidthHz: uint32(math.Round(khz * 1000)),
		Spreading: uint8(sf), CodingRate: uint8(cr),
	})) {
		return refused
	}
	return "OK"
}

// Refused where the typed command refuses it; the reference takes any figure.
func (s *service) cliSetTx(arg string) string {
	dbm, err := strconv.ParseInt(strings.TrimSpace(arg), 10, 8)
	if err != nil || !accepted(s.setRadioPower(int8(dbm))) {
		return "Error, invalid tx power"
	}
	return "OK"
}

func cliSetMilli(arg string, ceiling float64, into *uint32, refusal string) string {
	v, err := strconv.ParseFloat(strings.TrimSpace(arg), 64)
	if err != nil || !(v >= 0 && v <= ceiling) {
		return refusal
	}
	*into = uint32(math.Round(v * 1000))
	return "OK"
}

func (s *service) cliSetPathHashMode(arg string) string {
	mode, err := strconv.ParseUint(strings.TrimSpace(arg), 10, 8)
	if err != nil || mode > 2 {
		return "Error, must be 0,1, or 2"
	}
	s.p.PathHashMode = int(mode)
	return "OK"
}

// Refused past a byte, where the reference wraps.
func (s *service) cliSetMultiACKs(arg string) string {
	n, err := strconv.ParseUint(strings.TrimSpace(arg), 10, 8)
	if err != nil {
		return "Error, must be 0-255"
	}
	s.p.MultiACKs = int(n)
	return "OK"
}

// Always a decimal point, as the reference's ftoa: "1.0", "0.5".
func cliDecimal(v float64) string {
	out := strconv.FormatFloat(v, 'f', -1, 64)
	if !strings.Contains(out, ".") {
		out += ".0"
	}
	return out
}

func accepted(responses []companion.Response) bool {
	if len(responses) == 0 {
		return false
	}
	status, ok := responses[0].(companion.StatusResponse)
	return ok && status == companion.StatusResponse(companion.ResponseOK)
}
