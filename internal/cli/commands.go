package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/sentinel"
)

func (s *session) status(ctx context.Context) error {
	tb := &table{}
	tb.row("daemon", "up "+uptime(s.deps.Started), "lotor "+s.deps.Version)
	for _, r := range s.deps.Relays {
		tb.row("relay", r.Name, r.State(), "radio "+r.Radio,
			fmt.Sprintf("%.3f MHz sf%d bw%.1fk",
				float64(r.Waveform.FrequencyHz)/1e6, r.Waveform.SpreadingFactor,
				float64(r.Waveform.BandwidthHz)/1e3))
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

func (s *session) relay(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] == "list" {
		tb := &table{}
		for _, r := range s.deps.Relays {
			tb.row(r.Name, r.Protocol, r.State(), "radio "+r.Radio)
		}
		return tb.flush(s.out)
	}
	if args[0] != verbShow || len(args) < 2 {
		return errors.New("usage: relay list | relay show <name>")
	}
	r, err := s.findRelay(args[1])
	if err != nil {
		return err
	}
	tb := &table{}
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
	if s.deps.Sentinel != nil {
		counts, err := s.deps.Sentinel.VerdictCounts(ctx, r.Name)
		if err != nil {
			tb.row("judged", "unavailable: "+err.Error())
			return tb.flush(s.out)
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
	}
	return tb.flush(s.out)
}

func (s *session) radio(args []string) error {
	if len(args) == 0 || args[0] == "list" {
		tb := &table{}
		for _, r := range s.deps.Radios {
			tb.row(r.Name, r.Driver, envelopeText(r.Envelope), "relay "+r.Relay)
		}
		return tb.flush(s.out)
	}
	if args[0] != verbShow || len(args) < 2 {
		return errors.New("usage: radio list | radio show <name>")
	}
	for _, r := range s.deps.Radios {
		if r.Name == args[1] {
			tb := &table{}
			tb.row("driver", r.Driver)
			tb.row("envelope", envelopeText(r.Envelope))
			tb.row("relay", r.Relay)
			if err := tb.flush(s.out); err != nil {
				return err
			}
			break
		}
	}
	return s.showTraces("radio " + args[1])
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

func (s *session) config(args []string) error {
	if len(args) < 3 || args[0] != verbShow || (args[1] != scopeRelay && args[1] != scopeRadio) {
		return errors.New("usage: config show relay|radio <name>")
	}
	return s.showTraces(args[1] + " " + args[2])
}

func (s *session) showTraces(key string) error {
	traces, ok := s.deps.Traces[key]
	if !ok {
		keys := make([]string, 0, len(s.deps.Traces))
		for k := range s.deps.Traces {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return fmt.Errorf("no %q (known: %v)", key, keys)
	}
	tb := &table{}
	tb.row("key", "value", "source")
	for _, t := range traces {
		tb.row(t.Key, fmt.Sprintf("%v", t.Value), t.Source)
	}
	return tb.flush(s.out)
}

func (s *session) frames(ctx context.Context, args []string) error {
	pos, opts, err := flags(args)
	if err != nil {
		return err
	}
	if len(pos) > 0 && pos[0] == "watch" {
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
	if v, ok := opts["last"]; ok {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 || limit > maxLast {
			return fmt.Errorf("--last wants 1..%d", maxLast)
		}
	}
	if v, ok := opts[scopeRelay]; ok {
		if _, err := s.findRelay(v); err != nil {
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
	if opts["json"] == optOn {
		return s.printJSON(frames)
	}
	if len(frames) == 0 {
		fmt.Fprint(s.out, "no frames match\r\n")
		return nil
	}
	tb := &table{}
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
		return fmt.Sprintf("%s (%s)", quoted(f.Node), f.Detail)
	case f.Detail != "":
		return f.Detail
	default:
		return ""
	}
}

func (s *session) txn(ctx context.Context, args []string) error {
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
		return fmt.Errorf("no transaction matching %q", args[0])
	}
	for _, f := range chain {
		fmt.Fprintf(s.out, "%s  heard %s  %d B  %.0f dBm  snr %.1f  airtime %s\r\n",
			f.Txn[:12], f.At.Format("15:04:05"), f.Bytes, f.RSSI, f.SNR, f.Airtime)
		fmt.Fprintf(s.out, "  %s %s path_len %d — %s", f.Type, f.Route, f.PathLen, verdictWithChain(f))
		if w := who(f); w != "" {
			fmt.Fprintf(s.out, " — %s", w)
		}
		fmt.Fprint(s.out, "\r\n")
	}
	return nil
}

func (s *session) nodes(ctx context.Context, args []string) error {
	_, opts, err := flags(args)
	if err != nil {
		return err
	}
	sen, err := s.needSentinel()
	if err != nil {
		return err
	}
	nodes, err := sen.Nodes(ctx)
	if err != nil {
		return err
	}
	if opts["json"] == optOn {
		return s.printJSON(nodes)
	}
	tb := &table{}
	tb.row("name", "type", "pubkey", "heard", "last", "best rssi")
	for _, n := range nodes {
		tb.row(quoted(n.Name), n.Type, n.PubKey,
			fmt.Sprintf("%d×", n.Heard), ago(n.LastAt),
			fmt.Sprintf("%.0f dBm", n.BestRSSI))
	}
	return tb.flush(s.out)
}

func (s *session) sentinelStatus(ctx context.Context) error {
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
	return nil
}

// watch streams judgements live from the bus until input arrives. An
// empty line just stops the watch; a command stops it and then runs,
// so a piped script never loses the line that ended the stream.
func (s *session) watch(ctx context.Context, opts map[string]string) error {
	if s.deps.Bus == nil {
		return errors.New("no bus attached")
	}
	fmt.Fprint(s.out, "watching (enter stops)…\r\n")
	sub := s.deps.Bus.Subscribe(64)
	defer sub.Close()

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
			j, isJudged := ev.(bus.FrameJudged)
			if !isJudged || !watchMatch(j, opts) {
				continue
			}
			if opts["json"] == optOn {
				if err := s.printJSON(j); err != nil {
					return err
				}
				continue
			}
			fmt.Fprintf(s.out, "%s\r\n", watchLine(j))
		}
	}
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
		line += fmt.Sprintf("  %s (%s)", quoted(j.Node), j.Detail)
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
