package cli

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/relay"
	"meshrunner.dev/lotor/internal/sentinel"
)

func (s *session) status(ctx context.Context, _ input) error {
	tb := s.table()
	tb.row("daemon", "up "+uptime(s.deps.Started), "lotor "+s.deps.Version)
	for _, r := range s.relays() {
		tb.row("relay", r.Name, r.State(), "radio "+r.Radio,
			fmt.Sprintf("%.3f MHz sf%d bw%.1fk",
				float64(r.Waveform.FrequencyHz)/1e6, r.Waveform.SpreadingFactor,
				float64(r.Waveform.BandwidthHz)/1e3),
			"floor "+floorText(r), "tx "+txModeText(r)+dutyText(r))
	}
	if s.deps.Sentinel == nil {
		tb.row("sentinel", "none")
	} else {
		path, retention := s.deps.Sentinel.Journal()
		if n, err := s.deps.Sentinel.FrameCount(ctx); err != nil {
			// A sick journal is one degraded row, never a blank view.
			tb.row("sentinel", "error", err.Error())
		} else {
			tb.row("sentinel", "journalling", path,
				fmt.Sprintf("%d frames", n), fmt.Sprintf("%s retention", retention))
		}
	}
	return tb.flush(s.out)
}

// relayList renders the relays as a table — the tree's print at the
// collection.
func (s *session) relayList() error {
	if len(s.relays()) == 0 {
		fmt.Fprint(s.out, "no relays configured\r\n")
		return nil
	}
	tb := s.table()
	tb.header("NAME", "PROTOCOL", "STATE", "RADIO")
	for _, r := range s.relays() {
		tb.row(r.Name, r.Protocol, r.State(), r.Radio)
	}
	return tb.flush(s.out)
}

// relayStatus is one relay as it is running — what print does not
// show, because print answers about the configuration.
func (s *session) relayStatus(ctx context.Context, in input) error {
	name := in.opts[scopeRelay]
	if name == "" {
		one, err := s.oneRelay("")
		if err != nil {
			return err
		}
		name = one.Name
	}
	r, err := s.findRelay(name)
	if err != nil {
		return err
	}
	tb := s.table()
	tb.row("state", r.State())
	if r.Err != nil {
		if cause := r.Err(); cause != "" {
			tb.row("cause", cause)
		}
	}
	tb.row("protocol", r.Protocol)
	if r.Identity != "" {
		tb.row("identity", r.Identity[:min(12, len(r.Identity))])
	}
	tb.row("radio", fmt.Sprintf("%s (%s)", r.Radio, r.Driver))
	tb.row("waveform", fmt.Sprintf("%.3f MHz  sf%d  bw %d  cr 4/%d  preamble %d  sync 0x%02x  crc %v",
		float64(r.Waveform.FrequencyHz)/1e6, r.Waveform.SpreadingFactor,
		r.Waveform.BandwidthHz, r.Waveform.CodingRate, r.Waveform.Preamble,
		r.Waveform.SyncWord, r.Waveform.CRC))
	tb.row("noise floor", floorText(r))
	if len(r.Scopes) > 0 {
		tb.row(cmdScopes, strings.Join(r.Scopes, ", "))
	}
	tb.row("tx mode", txModeText(r))
	if r.Duty != nil {
		if used, budget, ok := r.Duty(); ok {
			tb.row("tx duty", fmt.Sprintf("%.1f%% — %s of %s per sliding hour",
				100*float64(used)/float64(budget), used.Round(time.Millisecond), budget))
		}
	}
	if r.ChipStats != nil {
		if cs, ok := r.ChipStats(); ok {
			// The chip's own tally — an independent second opinion on
			// the journal's counts.
			tb.row("chip stats", fmt.Sprintf("rx %d  crc-err %d  header-err %d",
				cs.Received, cs.CRCErrors, cs.HeaderErrors))
		}
	}
	if s.deps.Sentinel != nil {
		s.relayJournal(ctx, tb, r)
	}
	return tb.flush(s.out)
}

// relayJournal appends relay show's journal-backed rows: judgement
// totals and the corrupt-reception tally. A sick journal is degraded
// rows, never a missing section.
func (s *session) relayJournal(ctx context.Context, tb *table, r RelayInfo) {
	counts, err := s.deps.Sentinel.VerdictCounts(ctx, r.Name)
	if err != nil {
		tb.row("judged", "unavailable: "+err.Error())
		return
	}
	verdicts := make([]string, 0, len(counts))
	for v := range counts {
		verdicts = append(verdicts, v)
	}
	sort.Strings(verdicts)
	var line strings.Builder
	total := 0
	for _, v := range verdicts {
		fmt.Fprintf(&line, "  %d %s", counts[v], v)
		total += counts[v]
	}
	tb.row("judged", fmt.Sprintf("%d frames —%s", total, line.String()))

	noise, err := s.deps.Sentinel.Noise(ctx)
	if err != nil {
		tb.row("noise", "unavailable: "+err.Error())
		return
	}
	for _, nz := range noise {
		if nz.Relay == r.Name {
			tb.row("noise", fmt.Sprintf("%d corrupt receptions — last %s",
				nz.Count, ago(nz.LastAt)))
		}
	}

	drops, err := s.deps.Sentinel.TxDrops(ctx)
	if err != nil {
		tb.row("tx drops", "unavailable: "+err.Error())
		return
	}
	for _, d := range drops {
		if d.Relay == r.Name {
			tb.row("tx drops", fmt.Sprintf("%s ×%d — last %s", d.Reason, d.Count, ago(d.LastAt)))
		}
	}
}

// radioList renders the radios as a table — the tree's print at the
// collection.
func (s *session) radioList() error {
	if len(s.radios()) == 0 {
		fmt.Fprint(s.out, "no radios configured\r\n")
		return nil
	}
	tb := s.table()
	tb.header("NAME", "DRIVER", "ENVELOPE", "OWNER")
	for _, r := range s.radios() {
		owner := "unclaimed"
		if r.Relay != "" {
			owner = r.Relay
		}
		tb.row(r.Name, r.Driver, envelopeText(r.Envelope), owner)
	}
	return tb.flush(s.out)
}

// radioStatus is one radio as it is attached.
func (s *session) radioStatus(_ context.Context, in input) error {
	name := in.opts[scopeRadio]
	for _, r := range s.radios() {
		if r.Name != name {
			continue
		}
		tb := s.table()
		tb.row("driver", r.Driver)
		tb.row("envelope", envelopeText(r.Envelope))
		owner := "unclaimed"
		if r.Relay != "" {
			owner = r.Relay
		}
		tb.row("owner", owner)
		return tb.flush(s.out)
	}
	return fmt.Errorf("no radio %q", name)
}

// dutyText compacts the duty gauge for the status row; empty when
// unbudgeted or not transmitting.
func dutyText(r RelayInfo) string {
	if r.Duty == nil {
		return ""
	}
	used, budget, ok := r.Duty()
	if !ok {
		return ""
	}
	return fmt.Sprintf(" duty %.1f%%", 100*float64(used)/float64(budget))
}

// txModeText resolves a relay's gate for display; empty is dry.
func txModeText(r RelayInfo) string {
	if r.TXMode == "" {
		return "dry"
	}
	return r.TXMode
}

// floorText renders a relay's noise floor: the median, how far above
// it the site pulses, and the age — or an honest word for absence.
func floorText(r RelayInfo) string {
	if r.NoiseFloor == nil {
		return "unmeasured"
	}
	nf, ok := r.NoiseFloor()
	if !ok {
		return "calibrating"
	}
	return fmt.Sprintf("p50 %.0f dBm (p90-p50 %.1f dB, %s)", nf.DBm, nf.SpreadDB, ago(nf.At))
}

func envelopeText(e radio.Envelope) string {
	parts := []string{}
	if e.MaxTxPowerDBm != 0 {
		parts = append(parts, fmt.Sprintf("max %d dBm", e.MaxTxPowerDBm))
	}
	if e.FreqRangeLowHz != 0 || e.FreqRangeHiHz != 0 {
		parts = append(parts, fmt.Sprintf("%.0f-%.0f MHz",
			float64(e.FreqRangeLowHz)/1e6, float64(e.FreqRangeHiHz)/1e6))
	}
	if len(parts) == 0 {
		return "envelope undeclared"
	}
	return strings.Join(parts, ", ")
}

func (s *session) config(_ context.Context, in input) error {
	args := in.pos
	if len(args) < 3 || args[0] != verbShow || (args[1] != scopeRelay && args[1] != scopeRadio) {
		return errors.New("usage: config show relay|radio <name>")
	}
	return s.showTraces(args[1]+" "+args[2], false)
}

func (s *session) showTraces(key string, detail bool) error {
	traces, ok := s.traces()[key]
	if !ok {
		keys := make([]string, 0, len(s.traces()))
		for k := range s.traces() {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return fmt.Errorf("no %q (known: %v)", key, keys)
	}
	secret := s.secretAttrs(key)
	tb := s.table()
	tb.header("ATTRIBUTE", "VALUE", "SOURCE")
	const sourceColumn = 2
	for _, t := range traces {
		value := fmt.Sprintf("%v", t.Value)
		if secret[t.Key] && !detail {
			// A secret is shown when it is asked for by name and not
			// before: print masks, print detail does not.
			value = maskedValue
		}
		// The mark lands on the source, which is the cell it is about.
		tb.rowAs(weightOf(t.Source), sourceColumn, t.Key, value, t.Source)
	}
	return tb.flush(s.out)
}

// maskedValue stands in for what a secret attribute holds.
const maskedValue = "<secret>"

// secretAttrs resolves which of an object's attributes the schema
// marks secret, from a traces key ("relay meshcore-868").
func (s *session) secretAttrs(key string) map[string]bool {
	kind, name, ok := strings.Cut(key, " ")
	if !ok {
		return nil
	}
	k := s.kindByName(kind)
	if k == nil {
		return nil
	}
	out := map[string]bool{}
	for _, a := range k.AttrsFor(s.instances(kind)[name]) {
		if a.Secret {
			out[a.Name] = true
		}
	}
	return out
}

func (s *session) frames(ctx context.Context, in input) error {
	pos, opts := in.pos, in.opts
	if len(pos) > 0 {
		if pos[0] != "watch" {
			return fmt.Errorf("unknown argument %q — try frames --help", pos[0])
		}
		if _, ok := opts[optLast]; ok {
			return errors.New("last= is for the journal, not the live feed — try \"frames ?\"")
		}
		return s.watch(ctx, opts)
	}
	sen, err := s.needSentinel()
	if err != nil {
		return err
	}
	// The cap keeps one command from loading the whole journal; the
	// filters run in SQL, so a busy channel cannot starve them.
	const maxLast = 1000
	limit := 20
	if v, ok := opts[optLast]; ok {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 || limit > maxLast {
			return fmt.Errorf("last= wants 1..%d", maxLast)
		}
	}
	if v, ok := opts[scopeRelay]; ok {
		if _, err := s.relayFilter(ctx, v); err != nil {
			return err
		}
	}
	frames, err := sen.RecentFrames(ctx, sentinel.FrameQuery{
		Relay:   opts[scopeRelay],
		Type:    opts["type"],
		Verdict: opts["verdict"],
		Limit:   limit,
	})
	if err != nil {
		return err
	}
	if opts[optJSON] == optOn {
		return s.printJSON(frames)
	}
	if len(frames) == 0 {
		fmt.Fprint(s.out, "no frames match\r\n")
		return nil
	}
	tb := s.table()
	for _, f := range slices.Backward(frames) { // oldest first, like a log
		tb.row(f.At.Format("15:04:05"), f.Txn[:12], f.Type,
			fmt.Sprintf("%s /%d", f.Route, f.PathLen), verdictWithChain(f), who(f))
	}
	return tb.flush(s.out)
}

func verdictWithChain(f sentinel.Frame) string {
	if f.DuplicateOf != "" {
		return "duplicate → " + f.DuplicateOf
	}
	return f.Verdict
}

func who(f sentinel.Frame) string {
	switch {
	case f.Node != "":
		return fmt.Sprintf("%s (%s)", meshName(f.Node), f.Detail)
	case f.Detail != "":
		return f.Detail
	default:
		return ""
	}
}

func (s *session) txn(ctx context.Context, in input) error {
	args := in.pos
	if len(args) != 1 {
		return errors.New("usage: txn <prefix>")
	}
	sen, err := s.needSentinel()
	if err != nil {
		return err
	}
	chain, err := sen.Chain(ctx, args[0])
	if err != nil {
		return err
	}
	if len(chain) == 0 {
		// Nothing was heard under this id — but the daemon's own
		// adverts are emissions with no reception behind them, and an
		// operator reading one in a log line must be able to look it up.
		return s.originatedTxn(ctx, sen, args[0])
	}
	for _, f := range chain {
		fmt.Fprintf(s.out, "%s  heard %s  %d B  %.0f dBm  snr %.1f  signal %.0f dBm  Δf %+.0f Hz  airtime %s\r\n",
			f.Txn[:12], f.At.Format("15:04:05"), f.Bytes, f.RSSI, f.SNR,
			f.SignalRSSI, f.FreqErrHz, f.Airtime)
		scope := ""
		if f.Scope != "" {
			scope = " scope " + f.Scope
		}
		fmt.Fprintf(s.out, "  %s %s%s path_len %d — %s",
			f.Type, f.Route, scope, f.PathLen, verdictWithChain(f))
		if w := who(f); w != "" {
			fmt.Fprintf(s.out, " — %s", w)
		}
		fmt.Fprint(s.out, "\r\n")
		// The transaction's full life: what the pipeline sent for it.
		sent, err := sen.SentFor(ctx, f.Txn)
		if err != nil {
			return err
		}
		s.printSent(sent)
	}
	return nil
}

// originatedTxn renders a transaction the daemon started itself, or
// reports that nothing anywhere carries the prefix.
func (s *session) originatedTxn(ctx context.Context, sen *sentinel.Sentinel, prefix string) error {
	sent, err := sen.SentFor(ctx, prefix)
	if err != nil {
		return err
	}
	if len(sent) == 0 {
		return fmt.Errorf("no transaction matching %q", prefix)
	}
	fmt.Fprint(s.out, "originated — no reception behind it\r\n")
	s.printSent(sent)
	return nil
}

// printSent renders a transaction's emissions.
func (s *session) printSent(sent []sentinel.Sent) {
	for _, t := range sent {
		shadow := ""
		if t.Shadow {
			shadow = "  (shadow)"
		}
		fmt.Fprintf(s.out, "  sent %s  %s  %s  airtime %s  %d dBm%s\r\n",
			t.At.Format("15:04:05"), t.Relay, t.Kind, t.Airtime, t.PowerDBm, shadow)
	}
}

// working refuses a command when the relay itself is not running, and
// says so with the relay's own cause. A relay that never configured
// has no scopes, no neighbourhood and no scan to run — none of which
// is the reason, and every one of which reads like one. An operator
// chasing four different symptom messages is an operator kept away
// from the single line that explains all four.
func working(r RelayInfo) error {
	if r.State == nil || r.State() == relay.StateRunning {
		return nil
	}
	state := r.State()
	if r.Err != nil {
		if cause := r.Err(); cause != "" {
			return fmt.Errorf("relay %q is %s: %s", r.Name, state, cause)
		}
	}
	return fmt.Errorf("relay %q is %s — see \"relay %s\" for what it is waiting on",
		r.Name, state, r.Name)
}

// discover asks the neighbourhood who is there. It emits and returns:
// the answers are recorded as they land, whether or not anyone is
// still watching, so blocking the console for the window buys nothing
// the neighbourhood does not already keep. "watch" waits there and
// prints them live, which is worth having when what you want to know
// is who answers fast and who answers at all.
//
// The window itself is the protocol's, not ours: responders spread
// themselves deliberately, so a scan that gave up early would report
// a smaller room than it is standing in.
func (s *session) discover(ctx context.Context, in input) error {
	r, err := s.oneRelay(in.opts[scopeRelay])
	if err != nil {
		return err
	}
	if err := working(r); err != nil {
		return err
	}
	if r.Discover == nil {
		return fmt.Errorf("relay %q has no neighbourhood to scan", r.Name)
	}
	found, until, err := r.Discover()
	if err != nil {
		return err
	}
	if in.opts[optWatch] != optOn {
		fmt.Fprintf(s.out, "asked — answers land in the neighbourhood over the next %s\r\n",
			time.Until(until).Round(time.Second))
		return nil
	}
	// A watch holds the console, so a line has to be able to take it
	// back — and the line that does is itself a command, which could
	// be another watch. One at a time, as the frame stream does it.
	if s.watching {
		return errors.New("already watching")
	}
	s.watching = true
	defer func() { s.watching = false }()

	fmt.Fprintf(s.out, "listening %s (enter stops)…\r\n", time.Until(until).Round(time.Second))
	answered := 0
	for {
		select {
		case line, ok := <-s.lines:
			// Answers already in hand are printed before leaving: they
			// arrived, and dropping them on the way out would be the
			// one thing watching was for. Stopping the watch does not
			// stop the scan — the rest keep landing in the
			// neighbourhood behind us.
			answered += s.drainAnswers(found)
			fmt.Fprintf(s.out, "%d answered so far — the scan runs on\r\n", answered)
			if ok && line != "" {
				s.command(ctx, line)
			}
			return nil
		case n, ok := <-found:
			if !ok {
				if answered == 0 {
					fmt.Fprint(s.out, "nobody answered\r\n")
				}
				return nil
			}
			answered++
			s.printNeighbour(n)
		case <-time.After(time.Until(until) + time.Second):
			fmt.Fprintf(s.out, "%d answered\r\n", answered)
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

// printNeighbour renders one answer: who, and how well we hear them.
func (s *session) printNeighbour(n Neighbour) {
	// An answer names nobody; the name, when there is one, comes from
	// an advert this relay heard earlier.
	fmt.Fprintf(s.out, "%s  %.1f dB  %s\r\n",
		hex.EncodeToString(n.PubKey[:6]), n.SNR, meshName(n.Name))
}

// drainAnswers prints what has already landed without waiting for
// more, and reports how many that was.
func (s *session) drainAnswers(found <-chan Neighbour) int {
	printed := 0
	for {
		select {
		case n, ok := <-found:
			if !ok {
				return printed
			}
			s.printNeighbour(n)
			printed++
		default:
			return printed
		}
	}
}

// scopes shows what this relay carries, or asks a neighbour what it
// does. The asking half emits, so it is admin-gated at the table.
func (s *session) scopes(_ context.Context, in input) error {
	r, err := s.oneRelay(in.opts[scopeRelay])
	if err != nil {
		return err
	}
	if err := working(r); err != nil {
		return err
	}
	if len(in.pos) == 0 {
		if len(r.Scopes) == 0 {
			return fmt.Errorf("relay %q carries no scopes", r.Name)
		}
		fmt.Fprintf(s.out, "%s\r\n", strings.Join(r.Scopes, ", "))
		return nil
	}
	if in.pos[0] != "ask" || len(in.pos) != 2 {
		return errors.New("usage: scopes | scopes ask <pubkey-prefix>")
	}
	if s.deps.Privilege != Admin {
		return errors.New("asking a neighbour emits — use the local console socket")
	}
	if r.AskScopes == nil {
		return fmt.Errorf("relay %q has no scopes to ask about", r.Name)
	}
	prefix, err := hex.DecodeString(in.pos[1])
	if err != nil {
		return fmt.Errorf("%q is not a hex key prefix", in.pos[1])
	}
	fmt.Fprint(s.out, "asking…\r\n")
	names, err := r.AskScopes(prefix)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		fmt.Fprint(s.out, "it carries nothing it will name\r\n")
		return nil
	}
	fmt.Fprintf(s.out, "%s\r\n", strings.Join(names, ", "))
	return nil
}

// advert queues one operator announcement: the reference CLI's
// gesture — zero-hop by default, "flood" to address the mesh.
func (s *session) advert(_ context.Context, in input) error {
	flood := false
	if len(in.pos) == 1 {
		if in.pos[0] != "flood" {
			return fmt.Errorf("unknown argument %q — try advert --help", in.pos[0])
		}
		flood = true
	}
	r, err := s.oneRelay(in.opts[scopeRelay])
	if err != nil {
		return err
	}
	if err := working(r); err != nil {
		return err
	}
	if r.TriggerAdvert == nil {
		return fmt.Errorf("relay %q has no transmit pipeline — its gate is dry", r.Name)
	}
	if err := r.TriggerAdvert(flood); err != nil {
		return err
	}
	kind := "zero-hop advert"
	if flood {
		kind = "flood advert"
	}
	fmt.Fprintf(s.out, "%s queued on %q — the duty budget has the last word\r\n", kind, r.Name)
	return nil
}

// neighbours renders the direct neighbourhood: who we hear with no
// relay in between, at what SNR, and how long ago.
func (s *session) neighbours(ctx context.Context, in input) error {
	r, err := s.oneRelay(in.opts[scopeRelay])
	if err != nil {
		return err
	}
	if err := working(r); err != nil {
		return err
	}
	if r.Neighbours == nil {
		return fmt.Errorf("relay %q does not keep a neighbourhood", r.Name)
	}
	rows := r.Neighbours()
	if len(rows) == 0 {
		fmt.Fprint(s.out, "nobody heard directly yet\r\n")
		return nil
	}
	named := s.nodeNames(ctx)
	tb := s.table()
	tb.header("KEY", "NAME", "SNR", "HEARD")
	for _, n := range rows {
		key := hex.EncodeToString(n.PubKey[:6])
		name := n.Name
		if name == "" {
			// The engine only ever learns a name from an advert heard
			// zero-hop. The journal heard the flooded ones too, so it
			// can put a name to a node that only ever answered a scan.
			name = named[key]
		}
		tb.row(key, meshName(name), fmt.Sprintf("snr %+.2f dB", n.SNR), ago(n.Heard))
	}
	return tb.flush(s.out)
}

// undoCmd inverts the newest configuration change.
func (s *session) undoCmd(ctx context.Context, _ input) error {
	if s.deps.Undo == nil {
		return errors.New("this daemon has no mutation channel")
	}
	msg, err := s.deps.Undo(ctx, "console")
	if err != nil {
		return err
	}
	fmt.Fprintf(s.out, "%s\r\n", msg)
	return nil
}

// nodeNames indexes the journal's directory by key prefix, so a
// neighbourhood row can borrow a name the engine never heard itself.
// A daemon running without a journal gets an empty map and rows
// without names, which is the point of the journal being optional.
func (s *session) nodeNames(ctx context.Context) map[string]string {
	if s.deps.Sentinel == nil {
		return nil
	}
	nodes, err := s.deps.Sentinel.Nodes(ctx)
	if err != nil {
		return nil
	}
	out := make(map[string]string, len(nodes))
	for _, n := range nodes {
		if len(n.PubKey) >= 12 && n.Name != "" {
			out[n.PubKey[:12]] = n.Name
		}
	}
	return out
}

// oneRelay resolves a command's target: the named relay, or the only
// one there is.
func (s *session) oneRelay(name string) (RelayInfo, error) {
	if name != "" {
		return s.findRelay(name)
	}
	if len(s.relays()) == 1 {
		return s.relays()[0], nil
	}
	return RelayInfo{}, errors.New("several relays — say which with relay=<name>")
}

func (s *session) nodes(ctx context.Context, in input) error {
	opts := in.opts
	sen, err := s.needSentinel()
	if err != nil {
		return err
	}
	nodes, err := sen.Nodes(ctx)
	if err != nil {
		return err
	}
	if opts[optJSON] == optOn {
		return s.printJSON(nodes)
	}
	tb := s.table()
	tb.header("NAME", "TYPE", "PUBKEY", "HEARD", "LAST", "BEST RSSI", "DRIFT")
	for _, n := range nodes {
		drift := "—"
		if n.DriftHz != 0 {
			drift = fmt.Sprintf("%+.0f Hz", n.DriftHz)
		}
		best := "—"
		if n.HasRSSI {
			best = fmt.Sprintf("%.0f dBm", n.BestRSSI)
		}
		tb.row(meshName(n.Name), n.Type, n.PubKey,
			fmt.Sprintf("%d×", n.Heard), ago(n.LastAt), best, drift)
	}
	return tb.flush(s.out)
}

func (s *session) sentinelStatus(ctx context.Context, _ input) error {
	sen, err := s.needSentinel()
	if err != nil {
		return err
	}
	path, retention := sen.Journal()
	n, err := sen.FrameCount(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(s.out, "journal %s — %d frames, %s retention\r\n", path, n, retention)
	noise, err := sen.Noise(ctx)
	if err != nil {
		return err
	}
	for _, nz := range noise {
		fmt.Fprintf(s.out, "noise   %s — %d corrupt receptions, last %s: %s\r\n",
			nz.Relay, nz.Count, ago(nz.LastAt), nz.LastErr)
	}
	return nil
}

// noise shows a relay's noise-floor history: the live value, then the
// asked window's consolidated buckets, oldest first.
// tx renders the transmit-airtime history: the sliding hour's spent
// seconds, one point per emission, consolidated through the tiers.
func (s *session) tx(ctx context.Context, in input) error {
	opts := in.opts
	name := opts[scopeRelay]
	if name == "" {
		if len(s.relays()) != 1 {
			return errors.New("relay=<name> is needed when several relays run")
		}
		name = s.relays()[0].Name
	}
	live, err := s.relayFilter(ctx, name)
	if err != nil {
		return err
	}
	span := 24 * time.Hour
	if v, ok := opts[optLast]; ok {
		if span, err = parseSpan(v); err != nil {
			return err
		}
	}
	sen, err := s.needSentinel()
	if err != nil {
		return err
	}
	buckets, err := sen.TxAirtimeHistory(ctx, name, time.Now().Add(-span))
	if err != nil {
		return err
	}
	if opts[optJSON] == optOn {
		return s.printJSON(struct{ Airtime []sentinel.MetricBucket }{buckets})
	}
	current := "archived — relay not running"
	if live {
		if r, err := s.findRelay(name); err == nil {
			current = txModeText(r) + dutyText(r)
		}
	}
	fmt.Fprintf(s.out, "current  %s\r\n", current)
	if len(buckets) == 0 {
		fmt.Fprint(s.out, "no emissions journalled yet\r\n")
		return nil
	}
	tb := s.table()
	for _, b := range buckets {
		tb.row(b.At.Format("02/01 15:04"),
			fmt.Sprintf("min %.1f s", b.Min), fmt.Sprintf("avg %.1f s", b.Avg),
			fmt.Sprintf("max %.1f s", b.Max), fmt.Sprintf("%d×", b.N))
	}
	return tb.flush(s.out)
}

func (s *session) noise(ctx context.Context, in input) error {
	opts := in.opts
	name := opts[scopeRelay]
	if name == "" {
		if len(s.relays()) != 1 {
			return errors.New("relay=<name> is needed when several relays run")
		}
		name = s.relays()[0].Name
	}
	live, err := s.relayFilter(ctx, name)
	if err != nil {
		return err
	}
	span := 24 * time.Hour
	if v, ok := opts[optLast]; ok {
		if span, err = parseSpan(v); err != nil {
			return err
		}
	}
	sen, err := s.needSentinel()
	if err != nil {
		return err
	}
	since := time.Now().Add(-span)
	buckets, err := sen.NoiseHistory(ctx, name, since)
	if err != nil {
		return err
	}
	spreads, err := sen.NoiseSpreadHistory(ctx, name, since)
	if err != nil {
		return err
	}
	starveds, err := sen.NoiseStarvedHistory(ctx, name, since)
	if err != nil {
		return err
	}
	if opts[optJSON] == optOn {
		return s.printJSON(struct {
			Floor, Spread, Starved []sentinel.MetricBucket
		}{buckets, spreads, starveds})
	}
	current := "archived — relay not running"
	if live {
		if r, err := s.findRelay(name); err == nil {
			current = floorText(r)
		}
	}
	fmt.Fprintf(s.out, "current  %s\r\n", current)
	if len(buckets) == 0 {
		fmt.Fprint(s.out, "no history yet\r\n")
		return nil
	}
	return s.noiseTable(buckets, spreads, starveds).flush(s.out)
}

// noiseTable renders the floor buckets with their companion series.
// Every point is a batch median: min/max bound the p50s the bucket
// saw, avg(p50) is their consolidation — the telemetry idiom naming
// the estimator and the fold separately. starved counts the batches
// the channel was too busy to let converge.
func (s *session) noiseTable(buckets, spreads, starveds []sentinel.MetricBucket) *table {
	spreadAt := make(map[int64]float64, len(spreads))
	for _, b := range spreads {
		spreadAt[b.At.UnixMilli()] = b.Avg
	}
	starvedAt := make(map[int64]float64, len(starveds))
	for _, b := range starveds {
		starvedAt[b.At.UnixMilli()] = b.Avg * float64(b.N)
	}
	tb := s.table()
	for _, b := range buckets {
		tb.row(b.At.Format("02/01 15:04"),
			fmt.Sprintf("min %.1f", b.Min), fmt.Sprintf("avg(p50) %.1f", b.Avg),
			fmt.Sprintf("max %.1f", b.Max),
			fmt.Sprintf("p90-p50 %.1f", spreadAt[b.At.UnixMilli()]),
			fmt.Sprintf("starved %.0f", starvedAt[b.At.UnixMilli()]),
			fmt.Sprintf("%d×", b.N))
	}
	return tb
}

// relayFilter validates a journal query's relay name: running relays
// first, then the names the journal itself knows — a removed relay's
// archive stays addressable. live reports whether it still runs.
func (s *session) relayFilter(ctx context.Context, name string) (live bool, err error) {
	if _, err := s.findRelay(name); err == nil {
		return true, nil
	}
	if s.deps.Sentinel != nil {
		if known, kerr := s.deps.Sentinel.KnownRelays(ctx); kerr == nil &&
			slices.Contains(known, name) {
			return false, nil
		}
	}
	return false, s.relayFilterError(ctx, name)
}

// relayFilterError names both worlds when they differ: what runs, and
// what only the archive remembers.
func (s *session) relayFilterError(ctx context.Context, name string) error {
	running := make([]string, 0, len(s.relays()))
	for _, r := range s.relays() {
		running = append(running, r.Name)
	}
	sort.Strings(running)
	var archived []string
	if s.deps.Sentinel != nil {
		if known, err := s.deps.Sentinel.KnownRelays(ctx); err == nil {
			for _, k := range known {
				if !slices.Contains(running, k) {
					archived = append(archived, k)
				}
			}
		}
	}
	if len(archived) == 0 {
		return fmt.Errorf("no relay %q (relays: %s)", name, strings.Join(running, ", "))
	}
	return fmt.Errorf("no relay %q (running: %s; archived: %s)",
		name, strings.Join(running, ", "), strings.Join(archived, ", "))
}

// parseSpan reads a history window: Go durations plus a d suffix for
// days, the unit journals think in.
func parseSpan(v string) (time.Duration, error) {
	if days, err := strconv.Atoi(strings.TrimSuffix(v, "d")); err == nil && strings.HasSuffix(v, "d") {
		if days < 1 {
			return 0, errors.New("last= wants a duration like 24h or 7d")
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, errors.New("last= wants a duration like 24h or 7d")
	}
	return d, nil
}

// watch streams judgements live from the bus until input arrives. An
// empty line just stops the watch; a command stops it and then runs,
// so a piped script never loses the line that ended the stream.
func (s *session) watch(ctx context.Context, opts map[string]string) error {
	if s.deps.Bus == nil {
		return errors.New("no bus attached")
	}
	if v, ok := opts[scopeRelay]; ok {
		// A typo'd relay name must error like it does on the query
		// path, not filter forever against a name nothing carries.
		if _, err := s.findRelay(v); err != nil {
			return err
		}
	}
	// The line that stops a watch runs as a command — which could be
	// another watch, nesting subscriptions without bound. One at a
	// time.
	if s.watching {
		return errors.New("already watching")
	}
	s.watching = true
	defer func() { s.watching = false }()
	fmt.Fprint(s.out, "watching (enter stops)…\r\n")
	sub := s.deps.Bus.Subscribe(64)
	defer sub.Close()

	// The bus counts what a slow terminal loses; the stream owns up to
	// it, in place, instead of presenting its gaps as radio silence.
	var reported uint64
	confess := func() {
		if d := sub.Dropped(); d > reported {
			fmt.Fprintf(s.out, "(%d events dropped — this watch fell behind the bus)\r\n",
				d-reported)
			reported = d
		}
	}
	defer confess()

	for {
		select {
		case <-ctx.Done():
			return nil
		case line, ok := <-s.lines:
			if ok && line != "" {
				s.command(ctx, line)
			}
			return nil
		case ev, ok := <-sub.C:
			if !ok {
				return nil
			}
			confess()
			if err := s.watchEvent(ev, opts); err != nil {
				return err
			}
		}
	}
}

// watchEvent prints one bus event when the watch's filters keep it.
func (s *session) watchEvent(ev bus.Event, opts map[string]string) error {
	switch e := ev.(type) {
	case bus.FrameJudged:
		if !watchMatch(e, opts) {
			return nil
		}
		if opts[optJSON] == optOn {
			return s.printJSON(e)
		}
		_, err := fmt.Fprintf(s.out, "%s\r\n", watchLine(e))
		return err
	case bus.FrameCorrupt:
		// Corrupt receptions carry no type and no verdict: a watch
		// filtered on either asked for judgements only.
		if _, ok := opts["type"]; ok {
			return nil
		}
		if _, ok := opts["verdict"]; ok {
			return nil
		}
		if v, ok := opts[scopeRelay]; ok && e.Relay != v {
			return nil
		}
		if opts[optJSON] == optOn {
			return s.printJSON(e)
		}
		_, err := fmt.Fprintf(s.out, "corrupt reception — %s\r\n", e.Err)
		return err
	}
	return nil
}

func watchMatch(j bus.FrameJudged, opts map[string]string) bool {
	if v, ok := opts["type"]; ok && j.Type != v {
		return false
	}
	if v, ok := opts[scopeRelay]; ok && j.Relay != v {
		return false
	}
	if v, ok := opts["verdict"]; ok && j.Verdict != v {
		return false
	}
	return true
}

func watchLine(j bus.FrameJudged) string {
	line := fmt.Sprintf("%s  %s %s /%d  %s",
		j.Txn.Short(), j.Type, j.Route, j.PathLen, j.Verdict)
	if j.DuplicateOf != "" {
		line += " → " + j.DuplicateOf
	}
	switch {
	case j.Node != "":
		line += fmt.Sprintf("  %s (%s)", meshName(j.Node), j.Detail)
	case j.Detail != "":
		line += "  " + j.Detail
	}
	return line
}

func (s *session) printJSON(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(s.out, "%s\r\n", raw)
	return err
}
