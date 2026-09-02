package cli

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/product"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/relay"
	"meshrunner.dev/lotor/internal/sentinel"
	"meshrunner.dev/lotor/internal/update"
)

const noneText = "none"

func (s *session) status(ctx context.Context, _ input) error {
	tb := s.table()
	build := product.Slug + " " + s.deps.Version
	if s.deps.Revision != "" {
		build += " (" + s.deps.Revision + ")"
	}
	tb.row("daemon", "up "+uptime(s.deps.Started), build)
	for _, r := range s.relays() {
		tb.row("relay", r.Name, r.State(), "radio "+r.Radio,
			fmt.Sprintf("%.3f MHz sf%d bw%.1fk",
				float64(r.Waveform.FrequencyHz)/1e6, r.Waveform.SpreadingFactor,
				float64(r.Waveform.BandwidthHz)/1e3),
			"floor "+floorText(r), "tx "+txModeText(r)+dutyText(r))
	}
	s.sentinelRow(ctx, tb)
	return tb.flush(s.out)
}

// sentinelRow is the journal's status line: size and reach when it is
// well, the failure story when it is not — degradation and recovery
// both, because a log rotation must not erase the episode.
func (s *session) sentinelRow(ctx context.Context, tb *table) {
	if s.deps.Sentinel == nil {
		tb.row("sentinel", noneText)
		return
	}
	path, retention := s.deps.Sentinel.Journal()
	n, err := s.deps.Sentinel.FrameCount(ctx)
	if err != nil {
		// A sick journal is one degraded row, never a blank view.
		tb.row("sentinel", "error", err.Error())
		return
	}
	h := s.deps.Sentinel.Health()
	if !h.Healthy {
		tb.row("sentinel", "degraded", path,
			fmt.Sprintf("%d writes failed — last %s: %s",
				h.Failures, ago(h.LastFailAt), h.LastErr))
		return
	}
	extra := ""
	if h.Failures > 0 {
		extra = fmt.Sprintf(" — recovered after %d failed writes", h.Failures)
	}
	if h.BusDropped > 0 {
		extra += fmt.Sprintf(" — %d events lost to backpressure", h.BusDropped)
	}
	tb.row("sentinel", "journalling", path,
		fmt.Sprintf("%d frames", n),
		fmt.Sprintf("%s retention, %s metrics", retention,
			s.deps.Sentinel.MetricsRetention())+extra)
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

func (s *session) stationList() error {
	if len(s.stations()) == 0 {
		fmt.Fprint(s.out, "no stations configured\r\n")
		return nil
	}
	tb := s.table()
	tb.header("NAME", "PROTOCOL", "STATE", "RF", "LISTEN")
	for _, st := range s.stations() {
		tb.row(st.Name, st.Protocol, st.State, st.RF, st.Listen)
	}
	return tb.flush(s.out)
}

func (s *session) stationStatus(name string) error {
	for _, st := range s.stations() {
		if st.Name != name {
			continue
		}
		tb := s.table()
		tb.row("state", st.State)
		if st.Cause != "" {
			tb.row("cause", st.Cause)
		}
		tb.row("protocol", st.Protocol)
		if st.Identity != "" {
			tb.row("identity", st.Identity[:min(12, len(st.Identity))])
		}
		tb.row("listen", st.Listen)
		client := noneText
		if st.Connected {
			client = st.Remote
		}
		tb.row("client", client)
		rf := st.RF
		if st.Radio != "" {
			rf += " — " + st.Radio
		}
		tb.row("rf", rf)
		if st.RFCause != "" {
			tb.row("rf cause", st.RFCause)
		}
		tb.row("waveform", fmt.Sprintf("%.3f MHz  sf%d  bw %d  cr 4/%d  preamble %d  sync 0x%02x  crc %v",
			float64(st.Waveform.FrequencyHz)/1e6, st.Waveform.SpreadingFactor,
			st.Waveform.BandwidthHz, st.Waveform.CodingRate, st.Waveform.Preamble,
			st.Waveform.SyncWord, st.Waveform.CRC))
		tb.row("mailbox", fmt.Sprintf("%d / %d", st.Mailbox, st.MailboxCap))
		return tb.flush(s.out)
	}
	return fmt.Errorf("no station %q", name)
}

func (s *session) applicationList() error {
	if len(s.applications()) == 0 {
		fmt.Fprint(s.out, "no applications configured\r\n")
		return nil
	}
	tb := s.table()
	tb.header("NAME", "TYPE", "PROTOCOL", "STATE", "RF", "RADIO")
	for _, a := range s.applications() {
		radioName := a.Radio
		if radioName == "" {
			radioName = noneText
		}
		tb.row(a.Name, a.Type, a.Protocol, a.State, a.RF, radioName)
	}
	return tb.flush(s.out)
}

func (s *session) applicationStatus(name string) error {
	for _, a := range s.applications() {
		if a.Name != name {
			continue
		}
		tb := s.table()
		tb.row("state", a.State)
		if a.Cause != "" {
			tb.row("cause", a.Cause)
		}
		tb.row("type", a.Type)
		tb.row("protocol", a.Protocol)
		if a.Identity != "" {
			tb.row("identity", a.Identity[:min(12, len(a.Identity))])
		}
		rf := a.RF
		if a.Radio != "" {
			rf += " — " + a.Radio
		}
		tb.row("rf", rf)
		if a.RFCause != "" {
			tb.row("rf cause", a.RFCause)
		}
		tb.row("waveform", fmt.Sprintf("%.3f MHz  sf%d  bw %d  cr 4/%d  preamble %d  sync 0x%02x  crc %v",
			float64(a.Waveform.FrequencyHz)/1e6, a.Waveform.SpreadingFactor,
			a.Waveform.BandwidthHz, a.Waveform.CodingRate, a.Waveform.Preamble,
			a.Waveform.SyncWord, a.Waveform.CRC))
		// The type's own line, in the order it chose to be read.
		keys := make([]string, 0, len(a.Summary))
		for k := range a.Summary {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			tb.row(k, a.Summary[k])
		}
		return tb.flush(s.out)
	}
	return fmt.Errorf("no application %q", name)
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
	if r.Regions != nil {
		if info, err := r.Regions(); err == nil && len(info.Served) > 0 {
			tb.row(cmdRegions, strings.Join(info.Served, ", "))
		}
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
	tb.header("NAME", "DRIVER", "ENVELOPE", "AUTHORITY", "STATIONS")
	for _, r := range s.radios() {
		authority := r.Authority
		if authority == "" {
			authority = "unclaimed"
		}
		stations := noneText
		if others := append(append([]string(nil), r.Stations...), r.Applications...); len(others) > 0 {
			stations = strings.Join(others, ", ")
		}
		tb.row(r.Name, r.Driver, envelopeText(r.Envelope), authority, stations)
	}
	return tb.flush(s.out)
}

// sensorList renders the configured parts as a table — the tree's
// print at the collection. No owner column: nothing claims a sensor.
func (s *session) sensorList() error {
	sensors := s.sensors()
	if len(sensors) == 0 {
		fmt.Fprint(s.out, "no sensors configured\r\n")
		return nil
	}
	tb := s.table()
	tb.header("NAME", "DRIVER", "SAMPLED", "LAST")
	for _, sn := range sensors {
		every := "default"
		if sn.SampleInterval > 0 {
			every = "every " + sn.SampleInterval.String()
		}
		tb.row(sn.Name, sn.Driver, every, sensorLast(sn))
	}
	return tb.flush(s.out)
}

// sensorStatus is one part as it is read: what it was told to be, and
// what it last answered.
func (s *session) sensorStatus(name string) error {
	for _, sn := range s.sensors() {
		if sn.Name != name {
			continue
		}
		tb := s.table()
		tb.row("driver", sn.Driver)
		every := "the default"
		if sn.SampleInterval > 0 {
			every = sn.SampleInterval.String()
		}
		tb.row("sampled", "every "+every)
		switch {
		case !sn.Running:
			// The part did not open. The journal says why; the
			// console says that it did not, so nobody reads an empty
			// list as a quiet part.
			why := sn.Cause
			if why == "" {
				why = "no reason recorded yet"
			}
			tb.row("state", "not running — "+why)
		case len(sn.Readings) == 0:
			tb.row("state", "running, nothing read yet")
		default:
			tb.row("state", "running")
			for _, r := range sn.Readings {
				tb.row(string(r.Quantity),
					fmt.Sprintf("%.3f %s — %s", r.Value, r.Quantity.Unit(), ago(r.At)))
			}
		}
		return tb.flush(s.out)
	}
	return fmt.Errorf("no sensor %q", name)
}

// sensorLast summarises a part in one cell: the listing says whether
// it answers at all, and status says what it answered.
func sensorLast(sn SensorInfo) string {
	switch {
	case !sn.Running:
		return "not running"
	case len(sn.Readings) == 0:
		return "nothing yet"
	default:
		return ago(sn.Readings[0].At)
	}
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
		tb.row("state", r.State)
		if r.Cause != "" {
			tb.row("cause", r.Cause)
		}
		authority := r.Authority
		if authority == "" {
			authority = "unclaimed"
		}
		tb.row("authority", authority)
		stations := noneText
		if len(r.Stations) > 0 {
			stations = strings.Join(r.Stations, ", ")
		}
		tb.row("stations", stations)
		applications := noneText
		if len(r.Applications) > 0 {
			applications = strings.Join(r.Applications, ", ")
		}
		tb.row("applications", applications)
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
	// The presence bit, not the value: a ceiling of exactly 0 dBm is
	// a declared board, and showing it "undeclared" told the operator
	// the opposite of what the envelope now knows.
	if e.MaxTxPowerSet {
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

func (s *session) showTraces(key string, secrets bool) error {
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
		if secret[t.Key] && !secrets {
			// A secret is shown when it is asked for by name and not
			// before.
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
	opts := in.opts
	if opts[optWatch] == optOn {
		for _, sel := range []string{optLast, optSince, optUntil, optAround, optSpan} {
			if _, ok := opts[sel]; ok {
				return fmt.Errorf("%s= reads the journal — watch is the live feed", sel)
			}
		}
		return s.watch(ctx, opts)
	}
	sen, err := s.needSentinel()
	if err != nil {
		return err
	}
	w, err := parseFrameSelectors(opts, time.Now())
	if err != nil {
		return err
	}
	if w.aroundPrefix != "" {
		if err := s.resolveAround(ctx, sen, &w); err != nil {
			return err
		}
	}
	limit, err := frameLimit(w)
	if err != nil {
		return err
	}
	if v, ok := opts[scopeRelay]; ok {
		if _, err := s.relayFilter(ctx, v); err != nil {
			return err
		}
	}
	fq := sentinel.FrameQuery{
		Relay:   opts[scopeRelay],
		Type:    opts[optFrameType],
		Verdict: opts[optVerdict],
		Since:   w.since,
		Until:   w.until,
		Limit:   limit,
	}
	frames, err := sen.RecentFrames(ctx, fq)
	if err != nil {
		return err
	}
	if len(frames) == 0 {
		fmt.Fprint(s.out, "no frames match\r\n")
		return nil
	}
	tb := s.table()
	for _, f := range slices.Backward(frames) { // oldest first, like a log
		tb.row(f.At.Format("15:04:05"), f.Correlation[:12], f.Type,
			fmt.Sprintf("%s /%d", f.Route, f.PathLen), verdictWithChain(f), frameContext(f))
	}
	if err := tb.flush(s.out); err != nil {
		return err
	}
	s.confessCap(ctx, sen, fq, w, len(frames))
	return nil
}

// frameLimit bounds what one command may load. The cap keeps a query
// from reading the whole journal; a window defaults to the cap rather
// than to a screenful, because the window is the ask and twenty rows
// of it would be a sample pretending to be an answer.
func frameLimit(w frameSelectors) (int, error) {
	const maxLast = 1000
	switch {
	case w.count > maxLast:
		return 0, fmt.Errorf("%s= wants 1..%d", optLast, maxLast)
	case w.count > 0:
		return w.count, nil
	case w.windowed():
		return maxLast, nil
	default:
		return 20, nil
	}
}

// confessCap owns up when a window held more than the listing shows:
// the total is part of the answer, and a truncation that says nothing
// reads as "that was everything".
func (s *session) confessCap(ctx context.Context, sen *sentinel.Sentinel,
	fq sentinel.FrameQuery, w frameSelectors, shown int,
) {
	if !w.windowed() || shown < fq.Limit {
		return
	}
	if n, err := sen.CountFrames(ctx, fq); err == nil && n > fq.Limit {
		fmt.Fprintf(s.out, "newest %d of %d shown — narrow the window\r\n", fq.Limit, n)
	}
}

// resolveAround turns a correlation prefix into the window either
// side of the moment it was heard.
func (s *session) resolveAround(ctx context.Context, sen *sentinel.Sentinel, w *frameSelectors) error {
	anchors, err := sen.RecentFrames(ctx, sentinel.FrameQuery{CorrelationPrefix: w.aroundPrefix, Limit: 2})
	if err != nil {
		return err
	}
	switch len(anchors) {
	case 0:
		return fmt.Errorf("no correlation starts with %q", w.aroundPrefix)
	case 1:
	default:
		return fmt.Errorf("%q names more than one correlation — give more of it", w.aroundPrefix)
	}
	span := w.span
	if span == 0 {
		span = time.Minute
	}
	w.since, w.until = anchors[0].At.Add(-span), anchors[0].At.Add(span)
	return nil
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

func frameContext(f sentinel.Frame) string {
	parts := make([]string, 0, 2)
	if from := who(f); from != "" {
		parts = append(parts, from)
	}
	if f.Binding != "" {
		local := "hand-over from " + f.Binding
		if f.CausedBy != "" {
			local += " caused by " + f.CausedBy[:min(len(f.CausedBy), correlation.ShortLen)]
		}
		parts = append(parts, local)
	}
	return strings.Join(parts, " — ")
}

func (s *session) correlation(ctx context.Context, in input) error {
	args := in.pos
	if len(args) != 1 {
		return errors.New("usage: correlation <prefix>")
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
		return s.originatedCorrelation(ctx, sen, args[0])
	}
	for _, f := range chain {
		if f.Binding == "" {
			fmt.Fprintf(s.out, "%s  heard %s  %d B  %.0f dBm  snr %.1f  signal %.0f dBm  Δf %+.0f Hz  airtime %s\r\n",
				f.Correlation[:12], f.At.Format("15:04:05"), f.Bytes, f.RSSI, f.SNR,
				f.SignalRSSI, f.FreqErrHz, f.Airtime)
		} else {
			fmt.Fprintf(s.out, "%s  handed over %s  %d B  from %s",
				f.Correlation[:12], f.At.Format("15:04:05"), f.Bytes, f.Binding)
			if f.CausedBy != "" {
				fmt.Fprintf(s.out, "  caused by %s", f.CausedBy[:min(len(f.CausedBy), correlation.ShortLen)])
			}
			fmt.Fprint(s.out, "\r\n")
		}
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
		// The correlation's full life: what the pipeline sent for it —
		// and what it gave up on, which is an outcome too: a reception
		// judged worth relaying that then dropped on duty must not
		// read as leading nowhere.
		sent, err := sen.SentFor(ctx, f.Correlation)
		if err != nil {
			return err
		}
		s.printSent(sent)
		drops, err := sen.DropsFor(ctx, f.Correlation)
		if err != nil {
			return err
		}
		s.printDrops(drops)
		if f.CausedBy != "" {
			sent, err := sen.SentFor(ctx, f.CausedBy)
			if err != nil {
				return err
			}
			s.printSent(sent)
		}
	}
	return nil
}

// originatedCorrelation renders a correlation the daemon started
// itself, or reports that nothing anywhere carries the prefix. An
// origination that only ever DROPPED still has a causal story.
func (s *session) originatedCorrelation(ctx context.Context, sen *sentinel.Sentinel, prefix string) error {
	sent, err := sen.SentFor(ctx, prefix)
	if err != nil {
		return err
	}
	drops, err := sen.DropsFor(ctx, prefix)
	if err != nil {
		return err
	}
	if len(sent) == 0 && len(drops) == 0 {
		return fmt.Errorf("no correlation matching %q", prefix)
	}
	fmt.Fprint(s.out, "originated — no reception behind it\r\n")
	s.printSent(sent)
	s.printDrops(drops)
	return nil
}

// printSent renders a correlation's emissions.
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

// printDrops renders a correlation's refused emissions.
func (s *session) printDrops(drops []sentinel.TxDropEvent) {
	for _, d := range drops {
		kind := d.Kind
		if kind == "" {
			kind = "?"
		}
		fmt.Fprintf(s.out, "  dropped %s  %s  %s  %s\r\n",
			d.At.Format("15:04:05"), d.Relay, kind, d.Reason)
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
	if !s.beginWatch() {
		return errors.New("already watching")
	}
	defer s.endWatch()

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

// regions shows the relay's region tree — the reference's own render —
// with the designations under it.
func (s *session) regions(_ context.Context, in input) error {
	r, err := s.oneRelay(in.opts[scopeRelay])
	if err != nil {
		return err
	}
	if err := working(r); err != nil {
		return err
	}
	if r.Regions == nil {
		return fmt.Errorf("relay %q has no regions", r.Name)
	}
	info, err := r.Regions()
	if err != nil {
		return err
	}
	for line := range strings.SplitSeq(strings.TrimRight(info.Tree, "\n"), "\n") {
		fmt.Fprintf(s.out, "%s\r\n", line)
	}
	def := info.Default
	if def == "" {
		def = nullRegionCLI
	}
	fmt.Fprintf(s.out, "default: %s  home: %s\r\n", def, info.Home)
	return nil
}

// The structured region commands expose every useful mutation in the
// tree while still speaking the ecosystem's one grammar at the relay
// boundary. That raw grammar is intentionally kept behind this local
// adapter and on the authenticated OTA protocol.
const wireRegion = "region"

func (s *session) regionPut(_ context.Context, in input) error {
	if len(in.pos) == 0 || in.pos[0] == "" {
		return errors.New("which name? put takes one region name")
	}
	name := in.pos[0]
	if err := regionCLIWord("name", name); err != nil {
		return err
	}
	line := wireRegion + " put " + name
	if parent := in.opts[optParent]; parent != "" {
		if err := regionCLIWord(optParent, parent); err != nil {
			return err
		}
		line += " " + parent
	}
	return s.runRegionLine(in, line)
}

func (s *session) regionDef(_ context.Context, in input) error {
	if len(in.pos) == 0 || in.pos[0] == "" {
		return errors.New("which definition? quote the complete expression")
	}
	return s.runRegionLine(in, wireRegion+" def "+in.pos[0])
}

// regionSet gives the runtime drawer the same attribute grammar as a
// configured object: set designates a region, unset clears that
// designation. The wire spellings stay an implementation detail.
func (s *session) regionSet(_ context.Context, site *drawerSite,
	set map[string]string, unset []string,
) error {
	r, err := s.oneRelay(site.instance)
	if err != nil {
		return err
	}
	if err := working(r); err != nil {
		return err
	}
	if r.RegionDesignations == nil {
		return fmt.Errorf("relay %q has no region designations to administer", r.Name)
	}
	var def, home *string
	if name, ok := set[attrDefault]; ok {
		if name == nullRegionCLI || name == wildcardRegionCLI {
			return fmt.Errorf("%s clears with unset, not %q", attrDefault, name)
		}
		if err := regionCLIWord(attrDefault, name); err != nil {
			return err
		}
		def = &name
	}
	if name, ok := set[attrHome]; ok {
		if name == nullRegionCLI || name == wildcardRegionCLI {
			return fmt.Errorf("%s clears with unset, not %q", attrHome, name)
		}
		if err := regionCLIWord(attrHome, name); err != nil {
			return err
		}
		home = &name
	}
	if slices.Contains(unset, attrDefault) {
		emptyDesignation := ""
		def = &emptyDesignation
	}
	if slices.Contains(unset, attrHome) {
		emptyDesignation := ""
		home = &emptyDesignation
	}
	reply, err := r.RegionDesignations(s.regionOwner(), def, home)
	if err != nil {
		return err
	}
	if reply == "" {
		reply = "updated — region designations"
	}
	fmt.Fprintf(s.out, "%s\r\n", reply)
	return nil
}

// A structured field must remain one CommonCLI word when it crosses
// the protocol boundary. Otherwise `name="fr idf"` would silently
// become region fr with parent idf — a different mutation.
func regionCLIWord(attr, value string) error {
	fields := strings.Fields(value)
	if len(fields) != 1 || fields[0] != value {
		return fmt.Errorf("%s must be one word", attr)
	}
	return nil
}

// runRegionLine is the structured table verbs' adapter to the wire
// grammar. Besides keeping replies uniform it verifies that the
// protocol claimed the line, so a wiring regression cannot report a
// silent success. Designations use their atomic door above.
func (s *session) runRegionLine(in input, line string) error {
	r, err := s.oneRelay(in.opts[scopeRelay])
	if err != nil {
		return err
	}
	if err := working(r); err != nil {
		return err
	}
	if r.RegionLine == nil {
		return fmt.Errorf("relay %q has no regions to administer", r.Name)
	}
	reply, handled, err := r.RegionLine(s.regionOwner(), line)
	if err != nil {
		return err
	}
	if !handled {
		return errors.New("the relay did not read that as a region line")
	}
	if reply == "" {
		reply = "(no reply)"
	}
	for part := range strings.SplitSeq(strings.TrimRight(reply, "\n"), "\n") {
		fmt.Fprintf(s.out, "%s\r\n", part)
	}
	return nil
}

// regionAllow, regionDeny and regionDrop are the drawer's verbs: each
// composes the wire's own line, so the replies match the air's.
func (s *session) regionAllow(ctx context.Context, in input) error {
	return s.regionItemVerb(ctx, in, "allowf")
}

func (s *session) regionDeny(ctx context.Context, in input) error {
	return s.regionItemVerb(ctx, in, "denyf")
}

func (s *session) regionDrop(ctx context.Context, in input) error {
	return s.regionItemVerb(ctx, in, "remove")
}

func (s *session) regionItemVerb(_ context.Context, in input, verb string) error {
	name := in.opts[optRegion]
	if name == "" {
		return fmt.Errorf("which one? %s=<name>", optRegion)
	}
	if err := regionCLIWord(optRegion, name); err != nil {
		return err
	}
	return s.runRegionLine(in, wireRegion+" "+verb+" "+name)
}

// askScopes puts the question on the air. It reads nothing we already
// hold: the answer is the neighbour's own, which is the whole point of
// asking rather than looking.
// grantAccess records an admin permission for a whole public key.
func (s *session) grantAccess(_ context.Context, in input) error {
	r, err := s.oneRelay(in.opts[scopeRelay])
	if err != nil {
		return err
	}
	if err := working(r); err != nil {
		return err
	}
	if r.GrantRole == nil {
		return fmt.Errorf("relay %q grants nothing", r.Name)
	}
	key := in.opts[optKey]
	if key == "" {
		return fmt.Errorf("whose key? %s=<64 hex characters>", optKey)
	}
	pub, err := hex.DecodeString(key)
	if err != nil || len(pub) != 32 {
		return fmt.Errorf("%s wants a whole 64-character hex public key", optKey)
	}
	role := in.opts[optRole]
	if role == "" {
		role = roleAdmin
	}
	if err := r.GrantRole(pub, role); err != nil {
		return err
	}
	fmt.Fprintf(s.out, "granted %s to %s\r\n", role, key[:12])
	return nil
}

// revokeAccess takes back a durable role and drops any live session
// under the same key, named by key prefix from the ACL drawer.
func (s *session) revokeAccess(_ context.Context, in input) error {
	r, prefix, err := s.relayAndKeyPrefix(in)
	if err != nil {
		return err
	}
	if r.Revoke == nil || r.Access == nil {
		return fmt.Errorf("relay %q keeps no access list", r.Name)
	}
	// A revoke needs the whole key the engine holds; the prefix names
	// which entry, the access list supplies the rest.
	rows, err := r.Access()
	if err != nil {
		return err
	}
	full, err := matchAccess(rows, prefix)
	if err != nil {
		return err
	}
	if err := r.Revoke(full[:]); err != nil {
		return err
	}
	fmt.Fprintf(s.out, "revoked %s\r\n", hex.EncodeToString(full[:6]))
	return nil
}

// closeAirSession ends the live conversation named by a key prefix.
// Unlike revoke it resolves against the session drawer and leaves any
// durable access-list entry under the same key alone.
func (s *session) closeAirSession(_ context.Context, in input) error {
	r, prefix, err := s.relayAndKeyPrefix(in)
	if err != nil {
		return err
	}
	if r.CloseSession == nil || r.AirSessions == nil {
		return fmt.Errorf("relay %q cannot close over-the-air sessions", r.Name)
	}
	rows, err := r.AirSessions()
	if err != nil {
		return err
	}
	full, err := matchAirSession(rows, prefix)
	if err != nil {
		return err
	}
	if err := r.CloseSession(full[:]); err != nil {
		return err
	}
	fmt.Fprintf(s.out, "closed %s\r\n", hex.EncodeToString(full[:6]))
	return nil
}

// relayAndKeyPrefix resolves the subject shared by ACL and session
// item verbs: a working relay and one hex public-key prefix.
func (s *session) relayAndKeyPrefix(in input) (RelayInfo, []byte, error) {
	r, err := s.oneRelay(in.opts[scopeRelay])
	if err != nil {
		return RelayInfo{}, nil, err
	}
	if err := working(r); err != nil {
		return RelayInfo{}, nil, err
	}
	key := in.opts[optKey]
	if key == "" {
		return RelayInfo{}, nil, fmt.Errorf("which one? %s=<key prefix>", optKey)
	}
	prefix, err := hex.DecodeString(key)
	if err != nil {
		return RelayInfo{}, nil, fmt.Errorf("%q is not a hex key prefix", key)
	}
	return r, prefix, nil
}

// matchAirSession finds the one live session a prefix names.
func matchAirSession(rows []AirSession, prefix []byte) ([32]byte, error) {
	keys := make([][32]byte, len(rows))
	for i := range rows {
		keys[i] = rows[i].PubKey
	}
	return matchKeyPrefix(keys, prefix, "session")
}

// accessSet changes the role of the entry being stood on — set
// role=<word> with no key typed at all: the drawer names the entry,
// the access list supplies the whole key, and the same grant door
// records the change. Typing a key stays grant's business, whole key
// only; this door has no key to mistype.
func (s *session) accessSet(_ context.Context, site *drawerSite, set map[string]string) error {
	role, named := set[optRole]
	if !named || len(set) != 1 {
		return fmt.Errorf("only the role is settable here — %s %s=%s|%s|%s",
			verbSet, optRole, roleAdmin, roleReadWrite, roleReadOnly)
	}
	r, err := s.oneRelay(site.instance)
	if err != nil {
		return err
	}
	if err := working(r); err != nil {
		return err
	}
	if r.GrantRole == nil || r.Access == nil {
		return fmt.Errorf("relay %q keeps no access list", r.Name)
	}
	prefix, err := hex.DecodeString(site.item)
	if err != nil {
		return fmt.Errorf("%q is not a hex key prefix", site.item)
	}
	rows, err := r.Access()
	if err != nil {
		return err
	}
	full, err := matchAccess(rows, prefix)
	if err != nil {
		return err
	}
	if err := r.GrantRole(full[:], role); err != nil {
		return err
	}
	fmt.Fprintf(s.out, "%s is now %s\r\n", site.item, role)
	return nil
}

// matchAccess finds the one entry a prefix names, refusing a prefix
// that names none or several.
func matchAccess(rows []Access, prefix []byte) ([32]byte, error) {
	keys := make([][32]byte, len(rows))
	for i := range rows {
		keys[i] = rows[i].PubKey
	}
	return matchKeyPrefix(keys, prefix, "entry")
}

// matchKeyPrefix resolves one full public key from a displayed
// prefix, refusing absence and ambiguity with the subject's word.
func matchKeyPrefix(keys [][32]byte, prefix []byte, subject string) ([32]byte, error) {
	var found [32]byte
	hits := 0
	for _, key := range keys {
		if bytesHasPrefix(key[:], prefix) {
			found, hits = key, hits+1
		}
	}
	switch hits {
	case 0:
		return found, fmt.Errorf("no %s starts with %x", subject, prefix)
	case 1:
		return found, nil
	default:
		return found, fmt.Errorf("%x names more than one entry — give more of it", prefix)
	}
}

// bytesHasPrefix reports whether b starts with prefix.
func bytesHasPrefix(b, prefix []byte) bool {
	if len(prefix) > len(b) {
		return false
	}
	for i := range prefix {
		if b[i] != prefix[i] {
			return false
		}
	}
	return true
}

func (s *session) askRegions(_ context.Context, in input) error {
	r, err := s.oneRelay(in.opts[scopeRelay])
	if err != nil {
		return err
	}
	if err := working(r); err != nil {
		return err
	}
	key := in.opts[optNeighbour]
	if key == "" {
		return fmt.Errorf("which one? %s=<key prefix>", optNeighbour)
	}
	if r.AskRegions == nil {
		return fmt.Errorf("relay %q has no regions to ask about", r.Name)
	}
	prefix, err := hex.DecodeString(key)
	if err != nil {
		return fmt.Errorf("%q is not a hex key prefix", key)
	}
	fmt.Fprint(s.out, "asking…\r\n")
	names, err := r.AskRegions(prefix)
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

// advert queues one operator announcement: zero-hop by default,
// "flood" to address the mesh.
func (s *session) advert(_ context.Context, in input) error {
	flood := in.opts[optFlood] == optOn
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

// states renders the lifecycle tables the sentinel already retains as
// one interleaved timeline. Persisting transitions without a reader
// made the broker and relay history an SQL-only promise.
func (s *session) states(ctx context.Context, in input) error {
	sen, err := s.needSentinel()
	if err != nil {
		return err
	}
	now := time.Now()
	w := frameSelectors{}
	if _, err := w.readLast(in.opts, now); err != nil {
		return err
	}
	const stateHistoryCap = 1000
	limit := 20
	since := time.Time{}
	switch {
	case w.count > stateHistoryCap:
		return fmt.Errorf("%s= wants 1..%d", optLast, stateHistoryCap)
	case w.count > 0:
		limit = w.count
	case !w.since.IsZero():
		limit, since = stateHistoryCap, w.since
	}
	states, err := sen.StateHistory(ctx, since, limit)
	if err != nil {
		return err
	}
	if in.opts[optJSON] == optOn {
		return s.printJSON(states)
	}
	if len(states) == 0 {
		fmt.Fprint(s.out, "no lifecycle transitions journalled yet\r\n")
		return nil
	}
	tb := s.table()
	tb.header("WHEN", "KIND", "NAME", "STATE", "CAUSE")
	for _, st := range slices.Backward(states) {
		tb.row(st.At.Format("02/01 15:04:05"), st.Kind, st.Name, st.State, st.Cause)
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
	if len(buckets) == 0 && len(spreads) == 0 && len(starveds) == 0 {
		fmt.Fprint(s.out, "no history yet\r\n")
		return nil
	}
	return s.noiseTable(buckets, spreads, starveds).flush(s.out)
}

// noiseTable renders the union of the three series: a bucket exists
// wherever ANY of them measured something. An hour of pure starvation
// is precisely the hour worth reading — the channel too busy for a
// single floor batch — and a table walking only the floor's
// timestamps erased it.
// Every point is a batch median: min/max bound the p50s the bucket
// saw, avg(p50) is their consolidation — the telemetry idiom naming
// the estimator and the fold separately. starved counts the batches
// the channel was too busy to let converge.
func (s *session) noiseTable(buckets, spreads, starveds []sentinel.MetricBucket) *table {
	floorAt := make(map[int64]sentinel.MetricBucket, len(buckets))
	axis := make(map[int64]bool, len(buckets))
	for _, b := range buckets {
		floorAt[b.At.UnixMilli()] = b
		axis[b.At.UnixMilli()] = true
	}
	spreadAt := make(map[int64]float64, len(spreads))
	for _, b := range spreads {
		spreadAt[b.At.UnixMilli()] = b.Avg
		axis[b.At.UnixMilli()] = true
	}
	starvedAt := make(map[int64]float64, len(starveds))
	for _, b := range starveds {
		starvedAt[b.At.UnixMilli()] = b.Avg * float64(b.N)
		axis[b.At.UnixMilli()] = true
	}
	moments := make([]int64, 0, len(axis))
	for at := range axis {
		moments = append(moments, at)
	}
	slices.Sort(moments)
	// Every cell renders from its OWN (value, present) pair: a bucket
	// holding only spread must show that spread, a floor without one
	// must not invent 0.0, and absent starvation is unknown — not a
	// count that happens to be zero.
	cell := func(label string, v float64, present bool, format string) string {
		if !present {
			return label + " —"
		}
		return label + " " + fmt.Sprintf(format, v)
	}
	tb := s.table()
	for _, at := range moments {
		when := time.UnixMilli(at).Format("02/01 15:04")
		spread, hasSpread := spreadAt[at]
		starved, hasStarved := starvedAt[at]
		b, hasFloor := floorAt[at]
		count := "0×"
		if hasFloor {
			count = fmt.Sprintf("%d×", b.N)
		}
		tb.row(when,
			cell("min", b.Min, hasFloor, "%.1f"),
			cell("avg(p50)", b.Avg, hasFloor, "%.1f"),
			cell("max", b.Max, hasFloor, "%.1f"),
			cell("p90-p50", spread, hasSpread, "%.1f"),
			cell("starved", starved, hasStarved, "%.0f"),
			count)
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
	if !s.beginWatch() {
		return errors.New("already watching")
	}
	defer s.endWatch()
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
		_, err := fmt.Fprintf(s.out, "%s\r\n", watchLine(e))
		return err
	case bus.FrameCorrupt:
		// Corrupt receptions carry no type and no verdict: a watch
		// filtered on either asked for judgements only.
		if _, ok := opts[optFrameType]; ok {
			return nil
		}
		if _, ok := opts[optVerdict]; ok {
			return nil
		}
		if v, ok := opts[scopeRelay]; ok && e.Relay != v {
			return nil
		}
		_, err := fmt.Fprintf(s.out, "%s  corrupt reception — %s\r\n", e.Correlation.Short(), e.Err)
		return err
	}
	return nil
}

func watchMatch(j bus.FrameJudged, opts map[string]string) bool {
	if v, ok := opts[optFrameType]; ok && j.Type != v {
		return false
	}
	if v, ok := opts[scopeRelay]; ok && j.Relay != v {
		return false
	}
	if v, ok := opts[optVerdict]; ok && j.Verdict != v {
		return false
	}
	return true
}

func watchLine(j bus.FrameJudged) string {
	line := fmt.Sprintf("%s  %s %s /%d  %s",
		j.Correlation.Short(), j.Type, j.Route, j.PathLen, j.Verdict)
	if j.DuplicateOf != "" {
		line += " → " + j.DuplicateOf
	}
	if j.Binding != "" {
		line += "  hand-over from " + j.Binding
		if !j.CausedBy.IsZero() {
			line += " caused by " + j.CausedBy.Short()
		}
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

// completionBudget bounds how long a suggestion may take to gather. A
// completion that hangs is worse than one that offers nothing: the
// terminal is waiting on it with the operator's hand still on TAB.
const completionBudget = 200 * time.Millisecond

// frameTypes and frameVerdicts read the vocabulary out of the journal
// rather than from a list kept beside it. What was never recorded
// cannot be filtered for, and a console that offers it says otherwise.
func (s *session) frameTypes() []string {
	types, _ := s.frameVocabulary()
	return types
}

func (s *session) frameVerdicts() []string {
	_, verdicts := s.frameVocabulary()
	return verdicts
}

func (s *session) frameVocabulary() (types, verdicts []string) {
	if s.deps.Sentinel == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), completionBudget)
	defer cancel()
	types, verdicts, err := s.deps.Sentinel.FrameVocabulary(ctx)
	if err != nil {
		return nil, nil
	}
	return types, verdicts
}

// updateConfig is the /update singleton as resolved, defaults filled:
// what the operator did not set still has one answer.
func (s *session) updateConfig() (channel, url, token string) {
	channel, url = "release", config.DefaultUpdateURL
	for _, t := range s.traces()[kindUpdate] {
		v := fmt.Sprintf("%v", t.Value)
		switch t.Key {
		case "channel":
			channel = v
		case "url":
			url = v
		case "token":
			token = v
		}
	}
	return channel, url, token
}

// updateCheck asks the configured channel what it offers and compares
// with what runs. It fetches, verifies and reports — nothing is
// downloaded and nothing changes.
func (s *session) updateCheck(ctx context.Context, _ input) error {
	channel, url, token := s.updateConfig()
	trust := s.deps.UpdateTrust
	if trust == nil {
		trust = func() ([]update.PublicKey, error) { return update.Trusted(update.TrustedKeysDir) }
	}
	keys, err := trust()
	if err != nil {
		return err
	}
	client := &update.Client{Base: url, Token: token, Trusted: keys}
	got, err := client.Check(ctx, channel, "")
	if err != nil {
		return err
	}
	m := got.Manifest
	platform := runtime.GOOS + "/" + runtime.GOARCH
	art, err := m.ArtifactFor(platform)
	if err != nil {
		return err
	}
	running := s.deps.Version
	if !update.Newer(m, running, 0) {
		fmt.Fprintf(s.out, "running %s — channel %s offers %s, nothing newer\r\n",
			running, channel, m.Version)
		return nil
	}
	tb := s.table()
	tb.row("available", m.Version)
	tb.row("running", running)
	tb.row("channel", channel)
	tb.row("published", m.Published.Format("2006-01-02 15:04:05"))
	tb.row("artifact", fmt.Sprintf("%s, %d bytes", platform, art.Size))
	tb.row("signed by", "key "+got.Key.Hex())
	if m.Notes != "" {
		tb.row("notes", m.Notes)
	}
	return tb.flush(s.out)
}

// updateInstall fetches what the channel offers and stages it for the
// privileged installer: download, hash, signature, and a run of the
// new binary's own selfcheck, all before anything privileged is
// asked. The installer re-verifies everything against its own trust
// store — the staging daemon is not part of the trust chain.
func (s *session) updateInstall(ctx context.Context, in input) error {
	if s.deps.StateDir == "" {
		return errors.New("this daemon keeps no state directory — nowhere to stage")
	}
	channel, url, token := s.updateConfig()
	trust := s.deps.UpdateTrust
	if trust == nil {
		trust = func() ([]update.PublicKey, error) { return update.Trusted(update.TrustedKeysDir) }
	}
	keys, err := trust()
	if err != nil {
		return err
	}
	client := &update.Client{Base: url, Token: token, Trusted: keys}
	checked, err := client.Check(ctx, channel, "")
	if err != nil {
		return err
	}
	m := checked.Manifest
	if !update.Newer(m, s.deps.Version, 0) && in.opts[optForce] != optOn {
		return fmt.Errorf("running %s — channel %s offers %s, nothing newer; %s forces",
			s.deps.Version, channel, m.Version, optForce)
	}
	art, err := m.ArtifactFor(update.Platform())
	if err != nil {
		return err
	}
	dir := update.StageDir(s.deps.StateDir)
	fmt.Fprintf(s.out, "fetching %s (%d bytes)…\r\n", m.Version, art.Size)
	done := s.showFetchProgress(client)
	staged, err := client.Download(ctx, art, dir)
	done()
	if err != nil {
		return err
	}
	fmt.Fprint(s.out, "hash verified — proving the new binary starts…\r\n")
	if s.deps.DBPath != "" {
		// The "variable" subprocess is the point: the staged binary
		// proving it starts, before anyone privileged is asked.
		//nolint:gosec // the staged binary is the subject
		probe := exec.CommandContext(ctx, staged, "update", "selfcheck", "--db", s.deps.DBPath)
		if out, err := probe.CombinedOutput(); err != nil {
			_ = update.ClearStage(dir)
			return fmt.Errorf("the new binary fails its selfcheck: %s (%w)",
				strings.TrimSpace(string(out)), err)
		}
	}
	if err := update.WriteStage(dir, checked, update.Platform()); err != nil {
		return err
	}
	fmt.Fprintf(s.out,
		"%s staged — the installer takes it from here, and the daemon will restart\r\n",
		m.Version)
	return nil
}

// showFetchProgress draws a download in place and returns the call
// that takes the line back. A megabyte over a slow link is a long
// silence otherwise, and an operator watching a relay update needs to
// know the difference between slow and stuck.
//
// Only a terminal gets it: repainting a line means nothing to a pipe,
// and a script's transcript is better off with the two plain lines
// this sits between. The blank return is deliberate — a session that
// draws nothing still has something to call at the end.
func (s *session) showFetchProgress(client *update.Client) func() {
	if !s.colors {
		return func() {}
	}
	var last time.Time
	paint := func(done, total int64) {
		fmt.Fprintf(s.out, "\r  %s %3d%%  %s / %s\x1b[K",
			progressBar(done, total), percentOf(done, total),
			humanBytes(done), humanBytes(total))
	}
	client.Progress = func(done, total int64) {
		// Paced, not per chunk: the console may be a telnet session
		// on the far end of the same radio link the relay serves. The
		// last chunk always paints, so the bar reaches its end even
		// when the whole download fits inside one interval.
		now := time.Now()
		if done < total && now.Sub(last) < progressEvery {
			return
		}
		last = now
		paint(done, total)
	}
	return func() {
		client.Progress = nil
		fmt.Fprint(s.out, "\r\x1b[K")
	}
}

// progressEvery paces the repaint. Fast enough to read as motion,
// slow enough that a narrow link carries bytes rather than frames.
const progressEvery = 100 * time.Millisecond

// progressCells is the bar's width, chosen to leave the whole line
// well inside eighty columns beside the percentage and the sizes.
const progressCells = 24

// progressBar draws the fraction as a bar. A total of zero — a
// manifest that never said how big — draws an empty one rather than
// dividing by it.
func progressBar(done, total int64) string {
	filled := 0
	if total > 0 {
		filled = int(min(done, total) * progressCells / total)
	}
	return "▕" + strings.Repeat("█", filled) +
		strings.Repeat("░", progressCells-filled) + "▏"
}

// percentOf is the fraction as whole percent, bounded to 100 so a
// server sending one byte too many cannot report 101.
func percentOf(done, total int64) int {
	if total <= 0 {
		return 0
	}
	return int(min(done, total) * 100 / total)
}

// humanBytes renders a size the way an operator reads one: three
// significant figures at most, and the unit that keeps them.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value, exp := float64(n)/unit, 0
	for value >= unit && exp < 3 {
		value /= unit
		exp++
	}
	return fmt.Sprintf("%.1f %s", value, [...]string{"kiB", "MiB", "GiB", "TiB"}[exp])
}

// mqttStatus shows one observer connection as it runs: the broker
// session's state, and what it will admit to having done.
func (s *session) mqttStatus(name string) error {
	for _, mq := range s.mqtts() {
		if mq.Name != name {
			continue
		}
		tb := s.table()
		// The same ladder the list renders: a row without a broker
		// session is down, not "connecting" — an observer that never
		// started is not on its way anywhere.
		state := observerState(mq)
		tb.row("broker", mq.URL)
		tb.row("state", state)
		if mq.Down != "" {
			tb.row("cause", mq.Down)
		}
		tb.row("relay", mq.Relay)
		if mq.Counters != nil {
			published, pubErrors, busDropped, filtered, last := mq.Counters()
			tb.row("published", strconv.FormatUint(published, 10))
			tb.row("publish errors", strconv.FormatUint(pubErrors, 10))
			tb.row("bus dropped", strconv.FormatUint(busDropped, 10))
			tb.row("filtered", strconv.FormatUint(filtered, 10))
			if !last.IsZero() {
				tb.row("last published", ago(last))
			}
		}
		return tb.flush(s.out)
	}
	return fmt.Errorf("no observer %q", name)
}

// mqttList names the observer connections and how each is doing.
func (s *session) mqttList() error {
	mqs := s.mqtts()
	if len(mqs) == 0 {
		fmt.Fprint(s.out, "no observers configured\r\n")
		return nil
	}
	for _, mq := range mqs {
		if mq.Disabled {
			fmt.Fprint(s.out, "Flags: X - disabled\r\n")
			break
		}
	}
	tb := s.table()
	tb.header("", "NAME", "BROKER", "RELAY", "STATE", "PUBLISHED")
	for _, mq := range mqs {
		flag, published := "", "-"
		if mq.Disabled {
			flag = "X"
		}
		if mq.Counters != nil {
			n, _, _, _, _ := mq.Counters()
			published = strconv.FormatUint(n, 10)
		}
		tb.row(flag, mq.Name, mq.URL, mq.Relay, observerState(mq), published)
	}
	return tb.flush(s.out)
}

// observerState is the one ladder both the list and the detail read,
// so they cannot disagree about the same observer: parked, connected,
// connecting — or down, with MQTTInfo.Down saying why.
func observerState(mq MQTTInfo) string {
	switch {
	case mq.Disabled:
		return "disabled"
	case mq.Connected != nil && mq.Connected():
		return "connected"
	case mq.Connected != nil:
		return "connecting"
	}
	return "down"
}

// regionOwner names this console session at the region door: a
// staging is an exclusive transaction keyed by owner, and every
// session must be its own — two consoles sharing a constant would
// speak into each other's.
func (s *session) regionOwner() string {
	if s.id != "" {
		return "console:" + s.id
	}
	return "console"
}
