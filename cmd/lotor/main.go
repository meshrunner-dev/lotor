// Command lotor runs the Lotor mesh relay daemon: it loads the
// configuration, resolves each relay's layered radio and protocol
// settings, and supervises every relay until interrupted. This stage
// is receive-only by construction — the radio seam exposes no
// transmit.
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kong"

	"meshrunner.dev/pkg/meshcore"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/protocol"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/relay"
	"meshrunner.dev/lotor/internal/sentinel"
	"meshrunner.dev/lotor/internal/single"

	_ "meshrunner.dev/lotor/internal/protocol/meshcore"
	_ "meshrunner.dev/lotor/internal/radio/sx126x"
)

// version identifies this build in the CLI banner and status.
const version = "0.1.0-dev"

// commandLine is the Kong grammar. Bare `lotor` prints this help and
// does nothing else — running the daemon is an explicit choice.
type commandLine struct {
	Run      runCmd           `cmd:""                              help:"Run the relay daemon in the foreground (what the systemd unit does)."`
	Console  consoleCmd       `cmd:""                              help:"Open the console of a running daemon."`
	Attach   consoleCmd       `cmd:""                              help:"Alias of console."                                                    hidden:""`
	Identity identityCmd      `cmd:""                              help:"Node identity tools."`
	Version  kong.VersionFlag `help:"Print the version and leave."`
}

type identityCmd struct {
	New identityNewCmd `cmd:"" help:"Mint a fresh node identity to paste into the configuration."`
}

type identityNewCmd struct{}

// Run mints a seed and shows what to paste: the identity line for the
// relay's config, and the public key the mesh will know the node by.
func (c *identityNewCmd) Run() error {
	seed := make([]byte, meshcore.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return err
	}
	id, err := meshcore.LocalIdentityFromSeed(seed)
	if err != nil {
		return err
	}
	fmt.Printf("identity: %x   # the private key — guard this line\n", seed)
	fmt.Printf("pubkey:   %x\n", id.PubKey[:])
	return nil
}

type runCmd struct {
	Config   string `default:"/etc/lotor/config.yaml" help:"Configuration file."`
	LogLevel string `default:"info"                   help:"Zap level: debug, info, warn, error."`
}

func (c *runCmd) Run() error { return run(c.Config, c.LogLevel) }

type consoleCmd struct {
	Addr string `arg:"" help:"CLI address (default ${default_cli})." optional:""`
}

func (c *consoleCmd) Run() error { return console(c.Addr) }

func main() {
	root := commandLine{}
	parser, err := kong.New(&root,
		kong.Name("lotor"),
		kong.Description("A mesh relay daemon."),
		kong.Vars{
			"version":     version,
			"default_cli": config.DefaultCLIListen,
		},
		kong.UsageOnError(),
	)
	if err != nil {
		panic(err)
	}
	args := os.Args[1:]
	if len(args) == 0 {
		args = []string{"--help"}
	}
	kctx, err := parser.Parse(args)
	parser.FatalIfErrorf(err)
	kctx.FatalIfErrorf(kctx.Run())
}

// console connects the terminal to a running daemon's CLI — the
// console-port gesture of network gear — and behaves the way a
// terminal should: Ctrl+D half-closes and lets the session finish,
// and the daemon closing (quit) ends the process immediately — no
// netcat-variant guesswork.
func console(addr string) error {
	if addr == "" {
		addr = config.DefaultCLIListen
	}
	var d net.Dialer
	conn, err := d.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	go func() {
		_, _ = io.Copy(conn, os.Stdin)
		if t, ok := conn.(*net.TCPConn); ok {
			_ = t.CloseWrite() // Ctrl+D: send EOF, keep reading the goodbye
		}
	}()
	_, _ = io.Copy(os.Stdout, conn)
	return nil
}

func run(configPath, logLevel string) error {
	log, err := newLogger(logLevel)
	if err != nil {
		return err
	}
	defer func() { _ = log.Sync() }()

	release, err := acquireInstanceLock(configPath)
	if err != nil {
		return err
	}
	defer release()

	f, err := config.Load(configPath)
	if err != nil {
		return err
	}

	b := bus.New()
	deps := cli.Deps{
		Version: version,
		Started: time.Now(),
		Bus:     b,
		Traces:  map[string][]config.Trace{},
	}
	relays := buildRelays(f, b, log, &deps)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// The sentinel outlives the publishers: phase one stops relays and
	// CLI sessions, phase two lets the journal drain what they left on
	// the bus before it closes. The last frames of a session are
	// journalled, not discarded.
	journalCtx, journalDone := context.WithCancel(context.Background())
	defer journalDone()

	var producers, journal sync.WaitGroup
	if err := startConsumers(ctx, journalCtx, f, &deps, b, &producers, &journal, log); err != nil {
		return err
	}
	for _, r := range relays {
		producers.Go(func() { r.Run(ctx) })
	}
	log.Info("daemon up", zap.Int("relays", len(relays)))
	<-ctx.Done()
	log.Info("shutting down")
	producers.Wait()
	journalDone()
	journal.Wait()
	return nil
}

// acquireInstanceLock refuses a second daemon on the same
// configuration; the lock dies with the process, crash included.
func acquireInstanceLock(configPath string) (func(), error) {
	abs, err := filepath.Abs(configPath)
	if err != nil {
		return nil, err
	}
	return single.Acquire(context.Background(), "lotor", abs)
}

// buildRelays assembles every configured relay. A broken one is a
// visible casualty, never a dead daemon: it exists, in the error
// state, with its cause.
func buildRelays(f *config.File, b *bus.Bus, log *zap.Logger, deps *cli.Deps) []*relay.Relay {
	relays := make([]*relay.Relay, 0, len(f.Relays))
	for name, rc := range f.Relays {
		r, info, err := assemble(name, rc, f.Radios[rc.Radio], b, log, deps)
		if err != nil {
			log.Error("relay configuration failed",
				zap.String("relay", name), zap.Error(err))
			r = relay.Stillborn(name, err, b, log)
			info = cli.RelayInfo{
				Name: name, Protocol: rc.Protocol, Radio: rc.Radio,
				State: r.State, Err: r.Err,
			}
		}
		relays = append(relays, r)
		deps.Relays = append(deps.Relays, info)
	}
	sort.Slice(deps.Relays, func(i, j int) bool { return deps.Relays[i].Name < deps.Relays[j].Name })
	sort.Slice(deps.Radios, func(i, j int) bool { return deps.Radios[i].Name < deps.Radios[j].Name })
	return relays
}

// startConsumers brings up the optional bus consumers — sentinel and
// CLI — that the configuration asked for. The sentinel runs on its
// own context so it drains after every publisher has stopped.
func startConsumers(ctx, journalCtx context.Context, f *config.File, deps *cli.Deps,
	b *bus.Bus, producers, journal *sync.WaitGroup, log *zap.Logger,
) error {
	if f.Sentinel != nil {
		sent, err := sentinel.Open(ctx, f.Sentinel.Journal, f.Sentinel.Retention,
			f.Sentinel.MaxFrames, b, log.Named("sentinel"))
		if err != nil {
			return fmt.Errorf("sentinel: %w", err)
		}
		deps.Sentinel = sent
		journal.Go(func() { sent.Run(journalCtx) })
		log.Info("sentinel journalling",
			zap.String("journal", f.Sentinel.Journal),
			zap.Duration("retention", f.Sentinel.Retention))
	}
	if f.CLI != nil {
		addr := f.CLI.Listen
		d := *deps
		producers.Go(func() {
			if err := cli.ServeTelnet(ctx, addr, d, log.Named("cli")); err != nil {
				log.Error("cli listener failed", zap.Error(err))
			}
		})
	}
	return nil
}

// assemble resolves one relay's layered configurations — hardware
// against the driver's presets, waveform against the protocol's —
// validates every override scope and the envelope at load time, and
// logs every resolved key with its provenance, so a running config is
// always explainable from the log alone.
func assemble(name string, rc config.Relay, radioSpec config.Radio,
	b *bus.Bus, log *zap.Logger, deps *cli.Deps,
) (*relay.Relay, cli.RelayInfo, error) {
	none := cli.RelayInfo{}
	drv, err := radio.Lookup(radioSpec.Driver)
	if err != nil {
		return nil, none, err
	}
	radioCfg, radioTraces, err := radioSpec.Layered.Resolve(drv.Presets)
	if err != nil {
		return nil, none, fmt.Errorf("radio %q: %w", rc.Radio, err)
	}

	builder, err := protocol.Lookup(rc.Protocol)
	if err != nil {
		return nil, none, err
	}
	relayCfg, relayTraces, err := rc.Layered.Resolve(builder.Presets)
	if err != nil {
		return nil, none, err
	}
	deps.Traces["radio "+rc.Radio] = radioTraces
	deps.Traces["relay "+name] = relayTraces

	// Every override scope is checked, not just the selected one: a
	// typo under tomorrow's profile fails today.
	if err := checkScopes(radioSpec.Layered, drv.Presets,
		func(cfg map[string]any) error { _, e := drv.Inspect(cfg); return e }); err != nil {
		return nil, none, fmt.Errorf("radio %q: %w", rc.Radio, err)
	}
	if err := checkScopes(rc.Layered, builder.Presets, builder.Check); err != nil {
		return nil, none, err
	}

	rlog := log.With(zap.String("relay", name))
	logTraces(rlog, "radio config", radioTraces)
	logTraces(rlog, "relay config", relayTraces)
	announceOverrides(rlog, radioTraces, relayTraces)

	eng, err := builder.Build(name, relayCfg, b, rlog)
	if err != nil {
		return nil, none, err
	}
	env, err := bindEnvelope(drv, radioCfg, eng)
	if err != nil {
		return nil, none, fmt.Errorf("radio %q: %w", rc.Radio, err)
	}

	r := relay.New(name, drv, radioCfg, eng, b, log)
	info := cli.RelayInfo{
		Name:     name,
		Protocol: rc.Protocol,
		Radio:    rc.Radio,
		Driver:   radioSpec.Driver,
		Waveform: eng.Waveform(),
		State:    r.State,
		Err:      r.Err,
		Identity: eng.Identity(),
	}
	deps.Radios = append(deps.Radios, cli.RadioInfo{
		Name: rc.Radio, Driver: radioSpec.Driver, Envelope: env, Relay: name,
	})
	return r, info, nil
}

// bindEnvelope validates the engine's choices against the board's
// envelope at load: a waveform or an explicit power the board cannot
// serve is a configuration error, not a runtime surprise.
func bindEnvelope(drv radio.Driver, radioCfg map[string]any,
	eng protocol.Engine,
) (radio.Envelope, error) {
	env, err := drv.Inspect(radioCfg)
	if err != nil {
		return env, err
	}
	if err := env.Allows(eng.Waveform()); err != nil {
		return env, err
	}
	if dbm, explicit := eng.TxPower(); explicit && env.MaxTxPowerDBm != 0 && dbm > env.MaxTxPowerDBm {
		return env, fmt.Errorf("tx_power_dbm %d exceeds the radio's %d dBm cap — refusing, not clamping",
			dbm, env.MaxTxPowerDBm)
	}
	return env, nil
}

// checkScopes dry-runs every override scope through a validator.
func checkScopes(l config.Layered, presets map[string]map[string]any,
	check func(map[string]any) error,
) error {
	if check == nil {
		return nil
	}
	for scope := range l.Overrides {
		alt := config.Layered{Profile: scope, Overrides: l.Overrides}
		cfg, _, err := alt.Resolve(presets)
		if err != nil {
			return err
		}
		if err := check(cfg); err != nil {
			return fmt.Errorf("override scope %q: %w", scope, err)
		}
	}
	return nil
}

func logTraces(log *zap.Logger, msg string, traces []config.Trace) {
	for _, t := range traces {
		log.Debug(msg, zap.String("key", t.Key),
			zap.Any("value", t.Value), zap.String("source", t.Source))
	}
}

// announceOverrides names non-stock values at INFO on every start:
// a relay running off the beaten path says so where operators look.
func announceOverrides(log *zap.Logger, traceSets ...[]config.Trace) {
	var keys []string
	for _, ts := range traceSets {
		for _, t := range ts {
			if strings.HasPrefix(t.Source, "override:") {
				keys = append(keys, t.Key)
			}
		}
	}
	if len(keys) > 0 {
		sort.Strings(keys)
		log.Info("relay runs non-stock values", zap.Strings("overridden", keys))
	}
}

func newLogger(level string) (*zap.Logger, error) {
	lvl, err := zapcore.ParseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("log level: %w", err)
	}
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(lvl)
	cfg.Encoding = "console"
	// Retry loops make errors an expected operational state; the
	// message and fields carry the story, a stack trace adds nothing.
	cfg.DisableStacktrace = true
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	return cfg.Build()
}
