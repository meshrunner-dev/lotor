// Command lotor runs the Lotor mesh relay daemon: it loads the
// configuration, resolves each relay's layered radio and protocol
// settings, and supervises every relay until interrupted. This stage
// is receive-only by construction — the radio seam exposes no
// transmit.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/protocol"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/relay"
	"meshrunner.dev/lotor/internal/sentinel"

	_ "meshrunner.dev/lotor/internal/protocol/meshcore"
	_ "meshrunner.dev/lotor/internal/radio/sx126x"
)

func main() {
	configPath := flag.String("config", "/etc/lotor/config.yaml", "configuration file")
	logLevel := flag.String("log-level", "info", "zap level: debug, info, warn, error")
	flag.Parse()

	if err := run(*configPath, *logLevel); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
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
	relays := make([]*relay.Relay, 0, len(f.Relays))
	for name, rc := range f.Relays {
		r, err := assemble(name, rc, f.Radios[rc.Radio], b, log)
		if err != nil {
			return fmt.Errorf("relay %q: %w", name, err)
		}
		relays = append(relays, r)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	if f.Sentinel != nil {
		sent, err := sentinel.Open(f.Sentinel.Journal, f.Sentinel.Retention, b,
			log.Named("sentinel"))
		if err != nil {
			return fmt.Errorf("sentinel: %w", err)
		}
		wg.Go(func() { sent.Run(ctx) })
		log.Info("sentinel journalling",
			zap.String("journal", f.Sentinel.Journal),
			zap.Duration("retention", f.Sentinel.Retention))
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

// assemble resolves one relay's layered configurations — hardware
// against the driver's presets, waveform against the protocol's —
// and logs every resolved key with its provenance, so a running
// config is always explainable from the log alone.
func assemble(name string, rc config.Relay, radioSpec config.Radio,
	b *bus.Bus, log *zap.Logger,
) (*relay.Relay, error) {
	drv, err := radio.Lookup(radioSpec.Driver)
	if err != nil {
		return nil, err
	}
	radioCfg, radioTraces, err := radioSpec.Layered.Resolve(drv.Presets)
	if err != nil {
		return nil, fmt.Errorf("radio %q: %w", rc.Radio, err)
	}

	builder, err := protocol.Lookup(rc.Protocol)
	if err != nil {
		return nil, err
	}
	relayCfg, relayTraces, err := rc.Layered.Resolve(builder.Presets)
	if err != nil {
		return nil, err
	}

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
		return nil, err
	}
	return relay.New(name, drv, radioCfg, eng, b, log), nil
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
