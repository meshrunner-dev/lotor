package meshcore

import (
	"fmt"
	"strings"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

// cliUnknown is the reference's own word for a line it does not run. It
// travels as a reply rather than an error frame, exactly as there: the
// application prints the sentence it is given.
const cliUnknown = "Unknown command"

// runCLI serves one command line from the application. The words are
// the reference's own: its MyMesh::handleCommand answers five and
// delegates the rest to radio preferences and a board hook. A station
// shares its controller and owns no radio, so it serves what is hers
// and answers cliUnknown for the rest.
func (s *service) runCLI(line string) companion.CLIReply {
	line = strings.TrimLeft(line, " ")
	// Two characters and a bar are taken off before the words and put
	// back at the head of the answer, on the reference's own guard. It
	// never reads them, and nothing in the firmware writes them, so they
	// are the client's to mean something by.
	var tag string
	if len(line) > 4 && line[2] == '|' {
		tag, line = line[:3], line[3:]
	}
	return companion.CLIReply{Text: tag + s.cliAnswer(line)}
}

// cliAnswer runs one line, the pairing prefix already off. Each answer
// is the reference's own, shape included; where a station has no such
// thing to report it says nothing rather than inventing one.
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
	}
	return cliUnknown
}

// cliSetName renames the station. The reference refuses at this door
// the characters that separate fields in the text it prints, truncates
// the rest into its own field, and answers "OK" — a receipt, not a
// value. Its typed SetAdvertName validates nothing, and neither does
// ours: the asymmetry is the reference's own.
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
