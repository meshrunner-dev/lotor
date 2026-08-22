// Command lotor runs the Lotor mesh relay daemon: it loads the
// configuration, resolves each relay's layered radio and protocol
// settings, and supervises every relay until interrupted. This stage
// is receive-only by construction — the radio seam exposes no
// transmit.
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kong"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/protocol"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/relay"
	"meshrunner.dev/lotor/internal/sentinel"

	_ "meshrunner.dev/lotor/internal/protocol/meshcore"
	_ "meshrunner.dev/lotor/internal/radio/sx126x"
)

// version identifies this build in the CLI banner and status.
const version = "0.1.0-dev"

// commandLine is the Kong grammar. Bare `lotor` prints this help and
// does nothing else — running the daemon is an explicit choice.
type commandLine struct {
	Run     runCmd           `cmd:""                              help:"Run the relay daemon in the foreground (what the systemd unit does)."`
	Console consoleCmd       `cmd:""                              help:"Open the console of a running daemon."`
	Attach  consoleCmd       `cmd:""                              help:"Alias of console."                                                    hidden:""`
	Version kong.VersionFlag `help:"Print the version and leave."`
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

	f, err := config.Load(configPath)
	if err != nil {
		return err
	}

	b := bus.New()
	started := time.Now()
	relays := make([]*relay.Relay, 0, len(f.Relays))
	deps := cli.Deps{
		Version: version,
		Started: started,
		Bus:     b,
		Traces:  map[string][]config.Trace{},
	}
	for name, rc := range f.Relays {
		r, info, err := assemble(name, rc, f.Radios[rc.Radio], b, log, deps.Traces)
		if err != nil {
			return fmt.Errorf("relay %q: %w", name, err)
		}
		relays = append(relays, r)
		deps.Relays = append(deps.Relays, info)
	}
	sort.Slice(deps.Relays, func(i, j int) bool { return deps.Relays[i].Name < deps.Relays[j].Name })

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	if err := startConsumers(ctx, f, &deps, b, &wg, log); err != nil {
		return err
	}
	for _, r := range relays {
		wg.Go(func() { r.Run(ctx) })
	}
	log.Info("daemon up", zap.Int("relays", len(relays)))
	<-ctx.Done()
	log.Info("shutting down")
	wg.Wait()
	return nil
}

// startConsumers brings up the optional bus consumers — sentinel and
// CLI — that the configuration asked for.
func startConsumers(ctx context.Context, f *config.File, deps *cli.Deps,
	b *bus.Bus, wg *sync.WaitGroup, log *zap.Logger,
) error {
	if f.Sentinel != nil {
		sent, err := sentinel.Open(ctx, f.Sentinel.Journal, f.Sentinel.Retention, b,
			log.Named("sentinel"))
		if err != nil {
			return fmt.Errorf("sentinel: %w", err)
		}
		deps.Sentinel = sent
		wg.Go(func() { sent.Run(ctx) })
		log.Info("sentinel journalling",
			zap.String("journal", f.Sentinel.Journal),
			zap.Duration("retention", f.Sentinel.Retention))
	}
	if f.CLI != nil {
		addr := f.CLI.Listen
		d := *deps
		wg.Go(func() {
			if err := cli.ServeTelnet(ctx, addr, d, log.Named("cli")); err != nil {
				log.Error("cli listener failed", zap.Error(err))
			}
		})
	}
	return nil
}

// assemble resolves one relay's layered configurations — hardware
// against the driver's presets, waveform against the protocol's —
// and logs every resolved key with its provenance, so a running
// config is always explainable from the log alone.
func assemble(name string, rc config.Relay, radioSpec config.Radio,
	b *bus.Bus, log *zap.Logger, traces map[string][]config.Trace,
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
	traces["radio "+rc.Radio] = radioTraces
	traces["relay "+name] = relayTraces

	rlog := log.With(zap.String("relay", name))
	for _, t := range radioTraces {
		rlog.Debug("radio config", zap.String("key", t.Key),
			zap.Any("value", t.Value), zap.String("source", t.Source))
	}
	for _, t := range relayTraces {
		rlog.Debug("relay config", zap.String("key", t.Key),
			zap.Any("value", t.Value), zap.String("source", t.Source))
	}

	eng, err := builder.Build(name, relayCfg, b, rlog)
	if err != nil {
		return nil, none, err
	}
	r := relay.New(name, drv, radioCfg, eng, b, log)
	info := cli.RelayInfo{
		Name:     name,
		Protocol: rc.Protocol,
		Radio:    rc.Radio,
		Driver:   radioSpec.Driver,
		Waveform: eng.Waveform(),
		State:    r.State,
	}
	return r, info, nil
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
