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
	if s.deps.Sentinel != nil {
		path, retention := s.deps.Sentinel.Journal()
		n, err := s.deps.Sentinel.FrameCount(ctx)
		if err != nil {
			return err
		}
		tb.row("sentinel", "journalling", path,
			fmt.Sprintf("%d frames", n), fmt.Sprintf("%s retention", retention))
	} else {
		tb.row("sentinel", "none")
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
	tb.row("protocol", r.Protocol)
	tb.row("radio", fmt.Sprintf("%s (%s)", r.Radio, r.Driver))
	tb.row("waveform", fmt.Sprintf("%.3f MHz  sf%d  bw %d  cr 4/%d  preamble %d  sync 0x%02x  crc %v",
		float64(r.Waveform.FrequencyHz)/1e6, r.Waveform.SpreadingFactor,
		r.Waveform.BandwidthHz, r.Waveform.CodingRate, r.Waveform.Preamble,
		r.Waveform.SyncWord, r.Waveform.CRC))
	if s.deps.Sentinel != nil {
		counts, err := s.deps.Sentinel.VerdictCounts(ctx, r.Name)
		if err != nil {
			return err
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
		for _, r := range s.deps.Relays {
			tb.row(r.Radio, r.Driver, "relay "+r.Name)
		}
		return tb.flush(s.out)
	}
	if args[0] != verbShow || len(args) < 2 {
		return errors.New("usage: radio list | radio show <name>")
	}
	return s.showTraces("radio " + args[1])
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
	limit := 20
	if v, ok := opts["last"]; ok {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 {
			return errors.New("--last wants a positive number")
		}
	}
	if v, ok := opts[scopeRelay]; ok {
		if _, err := s.findRelay(v); err != nil {
			return err
		}
	}
	frames, err := sen.RecentFrames(ctx, "", limit*4)
	if err != nil {
		return err
	}
	frames = filterFrames(frames, opts)
	if len(frames) > limit {
		frames = frames[:limit]
	}
	if opts["json"] == optOn {
		return s.printJSON(frames)
	}
	tb := &table{}
	for _, f := range slices.Backward(frames) { // oldest first, like a log
		tb.row(f.At.Format("15:04:05"), f.Txn[:12], f.Type,
			fmt.Sprintf("%s /%d", f.Route, f.PathLen), verdictWithChain(f), who(f))
	}
	return tb.flush(s.out)
}

func filterFrames(frames []sentinel.Frame, opts map[string]string) []sentinel.Frame {
	out := frames[:0]
	for _, f := range frames {
		if v, ok := opts[scopeRelay]; ok && f.Relay != v {
			continue
		}
		if v, ok := opts["type"]; ok && f.Type != v {
			continue
		}
		if v, ok := opts["verdict"]; ok && f.Verdict != v {
			continue
		}
		out = append(out, f)
	}
	return out
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
		return fmt.Sprintf("%s (%s)", printable(f.Node), f.Detail)
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
		tb.row(printable(n.Name), n.Type, n.PubKey,
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

// watch streams judgements live from the bus until any input arrives.
func (s *session) watch(ctx context.Context, opts map[string]string) error {
	if s.deps.Bus == nil {
		return errors.New("no bus attached")
	}
	fmt.Fprint(s.out, "watching (enter stops)…\r\n")
	sub := s.deps.Bus.Subscribe(64)
	defer sub.Close()

	stop := make(chan struct{})
	go func() {
		_, _ = s.in.ReadString('\n')
		close(stop)
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-stop:
			return nil
		case ev, ok := <-sub.C:
			if !ok {
				return nil
			}
			j, isJudged := ev.(bus.FrameJudged)
			if !isJudged {
				continue
			}
			if v, ok := opts["type"]; ok && j.Type != v {
				continue
			}
			line := fmt.Sprintf("%s %s /%d  %s", j.Type, j.Route, j.PathLen, j.Verdict)
			if j.DuplicateOf != "" {
				line += " → " + j.DuplicateOf
			}
			if j.Node != "" {
				line += fmt.Sprintf("  %s (%s)", printable(j.Node), j.Detail)
			} else if j.Detail != "" {
				line += "  " + j.Detail
			}
			fmt.Fprintf(s.out, "%s\r\n", line)
		}
	}
}

func (s *session) printJSON(v any) error {
	enc := json.NewEncoder(s.out)
	return enc.Encode(v)
}
