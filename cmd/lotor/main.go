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
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"golang.org/x/term"

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
	"meshrunner.dev/lotor/internal/update"

	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/mqtt"
	enginemc "meshrunner.dev/lotor/internal/protocol/meshcore"
	_ "meshrunner.dev/lotor/internal/radio/sx126x"
	"meshrunner.dev/lotor/internal/schema"
	lotorversion "meshrunner.dev/lotor/internal/version"
)

// version identifies this build in the CLI banner and status; the
// firmware string a companion reads over the air is the same one.
var version = lotorversion.Version

// commandLine is the Kong grammar. Bare `lotor` prints this help and
// does nothing else — running the daemon is an explicit choice.
type commandLine struct {
	Run      runCmd           `cmd:""                              help:"Run the relay daemon in the foreground (what the systemd unit does)."`
	Console  consoleCmd       `cmd:""                              help:"Open the console of a running daemon."`
	Attach   consoleCmd       `cmd:""                              help:"Alias of console."                                                         hidden:""`
	Config   configCmd        `cmd:""                              help:"Configuration database tools."`
	Identity identityCmd      `cmd:""                              help:"Node identity tools."`
	Update   updateCmd        `cmd:""                              help:"The update machinery's own gears — the units turn these, not operators."`
	Version  kong.VersionFlag `help:"Print the version and leave."`
}

// updateCmd is the privileged half of self-update, plus the health
// probe both halves lean on. Operators drive updates from the
// console (/update check, /update install); these subcommands are
// what the systemd units run.
type updateCmd struct {
	Apply     updateApplyCmd     `cmd:"" help:"Verify the staged update and install it over the target (root; the path unit's job)."`
	Rollback  updateRollbackCmd  `cmd:"" help:"Put the previous binary back (root; the OnFailure unit's job)."`
	Selfcheck updateSelfcheckCmd `cmd:"" help:"Prove this binary starts: parse the configuration and leave."`
}

type updateApplyCmd struct {
	State   string `default:"/var/lib/lotor"       help:"The daemon's state directory, where the stage waits."`
	Target  string `default:"/usr/local/bin/lotor" help:"The binary to replace."`
	Restart bool   `default:"true"                 help:"Restart lotor.service after installing."              negatable:""`
}

// Run is the privilege boundary made of code: it re-verifies the
// stage against its own trust store — the embedded keys and whatever
// root deposited — and only then touches the binary. The daemon that
// staged the files could be lying from end to end and gain nothing.
func (c *updateApplyCmd) Run() error {
	trusted, err := update.Trusted(update.TrustedKeysDir)
	if err != nil {
		return err
	}
	dir := update.StageDir(c.State)
	ready, err := update.VerifyStaged(dir, trusted)
	if err != nil {
		return fmt.Errorf("stage refused: %w", err)
	}
	if err := update.Apply(dir, c.Target); err != nil {
		return err
	}
	if err := update.WritePending(c.State, ready.Version); err != nil {
		return err
	}
	if err := update.ClearStage(dir); err != nil {
		return err
	}
	fmt.Printf("installed %s over %s — the previous binary stands by as .prev\n",
		ready.Version, c.Target)
	if !c.Restart {
		return nil
	}
	return exec.CommandContext(context.Background(), "systemctl", "restart", "lotor.service").Run()
}

type updateRollbackCmd struct {
	State   string `default:"/var/lib/lotor"       help:"The daemon's state directory."`
	Target  string `default:"/usr/local/bin/lotor" help:"The binary to restore."`
	Restart bool   `default:"true"                 help:"Restart lotor.service after restoring." negatable:""`
}

func (c *updateRollbackCmd) Run() error {
	// Fired by OnFailure, which knows only that the service died —
	// not why. Without an update on probation the binary is not the
	// suspect, and rolling it back would punish it for a radio fault.
	if update.ReadPending(c.State) == nil {
		fmt.Println("no update on probation — leaving the binary alone")
		return nil
	}
	if err := update.Rollback(c.Target); err != nil {
		return err
	}
	_ = update.ClearPending(c.State)
	fmt.Printf("rolled %s back to its previous binary\n", c.Target)
	if !c.Restart {
		return nil
	}
	return exec.CommandContext(context.Background(), "systemctl", "restart", "lotor.service").Run()
}

type updateSelfcheckCmd struct {
	DB string `default:"${default_db}" help:"Configuration database."`
}

// Run proves the binary executes on this machine and reads this
// configuration: the stage runs it on the downloaded binary before
// asking anyone privileged to act. It opens nothing but the database.
func (c *updateSelfcheckCmd) Run() error {
	// The staged binary proves it can lift and read this store — on a
	// copy. Migrating the live store from here would pull it out from
	// under the running daemon, and out from under the rollback path
	// that may still restart it.
	ctx := context.Background()
	source, err := confdb.Open(ctx, c.DB)
	if err != nil {
		return err
	}
	probe := c.DB + ".selfcheck"
	_ = os.Remove(probe)
	err = source.CopyTo(ctx, probe)
	_ = source.Close()
	if err != nil {
		return err
	}
	defer func() {
		matches, _ := filepath.Glob(probe + "*")
		for _, m := range matches {
			_ = os.Remove(m)
		}
	}()
	store, f, err := openConfig(probe, zap.NewNop())
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	// A relay-less configuration still starts a daemon, so it still
	// passes a selfcheck.
	if err := f.Validate(false); err != nil {
		return fmt.Errorf("configuration: %w", err)
	}
	fmt.Println("selfcheck ok")
	return nil
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
	DB       string `default:"${default_db}" help:"Configuration database."`
	LogLevel string `default:"info"          help:"Log level: trace, debug, info, warn, error."`
}

func (c *runCmd) Run() error { return run(c.DB, c.LogLevel) }

type configCmd struct {
	Import configImportCmd `cmd:"" help:"Migrate a YAML configuration file into the database, whole."`
}

type configImportCmd struct {
	Path  string `arg:""                                                     help:"The YAML configuration to import."`
	DB    string `default:"${default_db}"                                    help:"Configuration database."`
	Force bool   `help:"Replace a configuration the database already holds."`
}

// Run migrates a file into the store. The file's own validation runs
// first — a configuration that would not have booted must not become
// one that will not boot from the database instead.
func (c *configImportCmd) Run() error {
	f, err := config.Load(c.Path)
	if err != nil {
		return err
	}
	// The daemon reads the store at boot and holds the instance lock
	// while it runs; importing behind its back would change nothing
	// until the next restart and surprise everyone then.
	release, err := acquireInstanceLock(c.DB)
	if err != nil {
		return fmt.Errorf("%w — stop it, import, start it again", err)
	}
	defer release()

	ctx := context.Background()
	store, err := confdb.Open(ctx, c.DB)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	if empty, err := store.Empty(ctx); err != nil {
		return err
	} else if !empty && !c.Force {
		return fmt.Errorf("%w (%s) — --force replaces it", confdb.ErrNotEmpty, c.DB)
	}
	if err := store.ImportFile(ctx, f, "import:"+c.Path); err != nil {
		return err
	}
	fmt.Printf("imported into %s: %d radio(s), %d relay(s)", c.DB, len(f.Radios), len(f.Relays))
	if f.Sentinel != nil {
		fmt.Print(", sentinel")
	}
	if f.CLI != nil {
		fmt.Print(", cli")
	}
	fmt.Println()
	fmt.Println("the YAML file is no longer read — keep it as a keepsake or delete it")
	return nil
}

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
			"default_db":  confdb.DefaultPath,
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
// console-port gesture of network gear. On a real terminal it goes
// raw: every keystroke reaches the daemon's line editor, which owns
// echo, history and the cursor. Piped input flows line-wise, and the
// daemon closing (quit) ends the process immediately — no
// netcat-variant guesswork.
func console(addr string) error {
	network := "tcp"
	switch {
	case addr == "":
		// The local admin socket first — the OS's permissions are the
		// authentication; a daemon without one falls back to telnet.
		if _, err := os.Stat(config.DefaultConsoleSocket); err == nil {
			network, addr = "unix", config.DefaultConsoleSocket
		} else {
			addr = config.DefaultCLIListen
		}
	case strings.Contains(addr, "/"):
		network = "unix"
	}
	var d net.Dialer
	conn, err := d.DialContext(context.Background(), network, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	if fd := int(os.Stdin.Fd()); term.IsTerminal(fd) {
		state, err := term.MakeRaw(fd)
		if err != nil {
			return err
		}
		defer func() { _ = term.Restore(fd, state) }()
	}

	go func() {
		// Telnet reserves 0xFF: a data byte that high (8-bit meta
		// keys, latin-1 pastes) must travel doubled, or the daemon's
		// stripper eats the keystroke behind it.
		_, _ = io.Copy(cli.EscapeIAC(conn), os.Stdin)
		if t, ok := conn.(interface{ CloseWrite() error }); ok {
			_ = t.CloseWrite() // stdin EOF: let the session finish its goodbye
		}
	}()
	_, _ = io.Copy(os.Stdout, cli.StripIAC(conn))
	return nil
}

// openLocked takes the instance lock, then the store: the two doors a
// second daemon must not pass, opened as one step.
func openLocked(dbPath string, log *zap.Logger,
) (release func(), store *confdb.Store, f *config.File, err error) {
	release, err = acquireInstanceLock(dbPath)
	if err != nil {
		return nil, nil, nil, err
	}
	store, f, err = openConfig(dbPath, log)
	if err != nil {
		release()
		return nil, nil, nil, err
	}
	return release, store, f, nil
}

func run(dbPath, logLevel string) error {
	log, levelKnob, err := newLogger(logLevel)
	if err != nil {
		return err
	}
	defer func() { _ = log.Sync() }()

	release, store, f, err := openLocked(dbPath, log)
	if err != nil {
		return err
	}
	defer release()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// The sentinel outlives the publishers: phase one stops relays and
	// CLI sessions, phase two lets the journal drain what they left on
	// the bus before it closes. The last frames of a session are
	// journalled, not discarded.
	journalCtx, journalDone := context.WithCancel(context.Background())
	defer journalDone()

	b := bus.New()
	var journal sync.WaitGroup
	// The sentinel comes up before the relays so the duty ledgers can
	// resume the sliding hour from the journal at arming.
	sen, err := startSentinel(ctx, journalCtx, f, b, &journal, log)
	if err != nil {
		_ = store.Close()
		return err
	}

	mgr := newManager(store, f, b, sen, buildKinds(), log)
	mgr.adoptLevelKnob(levelKnob)
	deps := consoleDeps(mgr, b, sen)
	deps.DBPath = dbPath
	deps.StateDir = filepath.Dir(dbPath)
	watchProbation(ctx, deps.StateDir, log)
	mgr.Start(ctx)

	var producers sync.WaitGroup
	if err := startListeners(ctx, f, &deps, &producers, log); err != nil {
		return err
	}
	log.Info("daemon up", zap.Int("relays", len(f.Relays)))
	<-ctx.Done()
	log.Info("shutting down")
	producers.Wait()
	mgr.Wait()
	journalDone()
	journal.Wait()
	mgr.Close()
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

// buildKinds assembles the configuration vocabulary the console (and
// later channels) navigate: the structural attributes from the config
// package, the contributed ones from whichever protocol or driver an
// instance chose, straight from their registries.
func buildKinds() []schema.Kind {
	sortedNames := func(presets map[string]map[string]any) []string {
		names := make([]string, 0, len(presets))
		for n := range presets {
			names = append(names, n)
		}
		sort.Strings(names)
		return names
	}
	kinds := make([]schema.Kind, 0, 8)
	kinds = append(kinds, []schema.Kind{
		{
			Name: confdb.KindRelay, Doc: "one protocol instance, owning one radio",
			Attrs: config.RelayAttrs(), ChoiceAttr: attrProtocol,
			Contributed: func(choice string) []schema.Attr {
				b, err := protocol.Lookup(choice)
				if err != nil {
					return nil
				}
				return b.Schema
			},
			Profiles: func(choice string) []string {
				b, err := protocol.Lookup(choice)
				if err != nil {
					return nil
				}
				return sortedNames(b.Presets)
			},
		},
		{
			Name: confdb.KindRadio, Doc: "one physical transceiver attachment",
			Attrs: config.RadioAttrs(), ChoiceAttr: attrDriver,
			Contributed: func(choice string) []schema.Attr {
				d, err := radio.Lookup(choice)
				if err != nil {
					return nil
				}
				return d.Schema
			},
			Profiles: func(choice string) []string {
				d, err := radio.Lookup(choice)
				if err != nil {
					return nil
				}
				return sortedNames(d.Presets)
			},
		},
	}...)
	kinds = append(kinds, schema.Kind{
		Name: confdb.KindMQTT, Doc: "observer connections publishing the mesh to MQTT brokers",
		Attrs: append([]schema.Attr{
			{Name: attrProfile, Type: schema.String,
				Doc: `the community-broker preset; "custom" starts from nothing`},
			{Name: attrDisabled, Type: schema.Bool,
				Doc: "parked: the configuration is kept, the connection does not run"},
		}, mqtt.Schema()...),
		Profiles: func(string) []string { return sortedNames(mqtt.Presets()) },
	})
	return append(kinds, singletonKinds()...)
}

// singletonKinds are the blocks that exist once: no instance step, no
// choice attribute, just their own attributes.
func singletonKinds() []schema.Kind {
	return []schema.Kind{
		{
			Name: confdb.KindSentinel, Doc: "the observation journal", Singleton: true,
			Attrs: config.SentinelAttrs(),
		},
		{
			Name: confdb.KindCLI, Doc: "the operator listener", Singleton: true,
			Attrs: config.CLIAttrs(),
		},
		{
			Name: confdb.KindSystem, Doc: "what this installation calls itself", Singleton: true,
			Attrs: config.SystemAttrs(),
		},
		{
			Name: confdb.KindUpdate, Doc: "where this relay looks for newer versions of itself",
			Singleton: true, Attrs: config.UpdateAttrs(),
		},
	}
}

// openConfig opens the store the daemon will hold for its whole life
// — the manager writes every configuration change through it — and
// loads what it says. The instance lock keeps an import from racing a
// running daemon.
func openConfig(dbPath string, log *zap.Logger) (*confdb.Store, *config.File, error) {
	ctx := context.Background()
	store, err := confdb.Open(ctx, dbPath)
	if err != nil {
		return nil, nil, err
	}
	// The shape heals before the first load: a store written by a
	// past binary carries keys this one no longer speaks.
	if err := store.Migrate(ctx, storeMigrations()); err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	f, err := store.Load(ctx)
	if err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	if len(f.Relays) == 0 {
		log.Warn("no relays configured — the console is up, waiting to be told what to run",
			zap.String("db", dbPath))
	}
	return store, f, nil
}

// watchProbation commits an update once the new binary has held the
// service up for a while. The marker was armed by the installer just
// before the restart; surviving the grace is what "the update took"
// means, and the OnFailure rollback is what happens when it does not.
func watchProbation(ctx context.Context, stateDir string, log *zap.Logger) {
	p := update.ReadPending(stateDir)
	if p == nil {
		return
	}
	const grace = 90 * time.Second
	log.Info("update on probation", zap.String("version", p.Version), zap.Time("since", p.Since))
	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(grace):
			if err := update.ClearPending(stateDir); err != nil {
				log.Warn("could not commit the update", zap.Error(err))
				return
			}
			log.Info("update committed", zap.String("version", p.Version))
		}
	}()
}

// consoleDeps is everything the operator surfaces may consult: the
// live views and the mutation door, all of them the manager's.
func consoleDeps(mgr *manager, b *bus.Bus, sen *sentinel.Sentinel) cli.Deps {
	return cli.Deps{
		Version:    version,
		Started:    time.Now(),
		Sessions:   cli.NewSessions(),
		Bus:        b,
		Sentinel:   sen,
		Kinds:      mgr.kinds,
		LiveRelays: mgr.RelayInfos,
		LiveRadios: mgr.RadioInfos,
		LiveMQTTs:  mgr.MQTTInfos,
		History:    mgr.History,
		Log:        mgr.log.Named("cli"),
		LiveTraces: mgr.Traces,
		Mutate:     mgr.Mutate,
		Undo:       mgr.Undo,
		Create:     mgr.Create,
		Remove:     mgr.Remove,
		SystemName: mgr.SystemName,
	}
}

// startSentinel brings up the observation journal when the
// configuration asks for one. It runs on its own context so it drains
// after every publisher has stopped.
func startSentinel(ctx, journalCtx context.Context, f *config.File,
	b *bus.Bus, journal *sync.WaitGroup, log *zap.Logger,
) (*sentinel.Sentinel, error) {
	if f.Sentinel == nil {
		return nil, nil //nolint:nilnil // absence is the configured mode, not a fault
	}
	sent, err := sentinel.Open(ctx, f.Sentinel.Journal, f.Sentinel.Retention,
		f.Sentinel.MaxFrames, b, log.Named("sentinel"))
	if err != nil {
		return nil, fmt.Errorf("sentinel: %w", err)
	}
	journal.Go(func() { sent.Run(journalCtx) })
	log.Info("sentinel journalling",
		zap.String("journal", f.Sentinel.Journal),
		zap.Duration("retention", f.Sentinel.Retention))
	return sent, nil
}

// startListeners brings up the operator surfaces the configuration
// asked for.
func startListeners(ctx context.Context, f *config.File, deps *cli.Deps,
	producers *sync.WaitGroup, log *zap.Logger,
) error {
	if f.CLI != nil {
		addr := f.CLI.Listen
		d := *deps
		d.Privilege = cli.ReadOnly
		producers.Go(func() {
			if err := cli.ServeTelnet(ctx, addr, d, log.Named("cli")); err != nil {
				log.Error("cli listener failed", zap.Error(err))
			}
		})
	}
	return startConsole(ctx, f, deps, producers, log)
}

// startConsole opens the local admin console socket: the OS's file
// permissions authenticate, so whoever may open it is admin.
func startConsole(ctx context.Context, f *config.File, deps *cli.Deps,
	producers *sync.WaitGroup, log *zap.Logger,
) error {
	path, explicit := f.ConsoleSocket()
	if path == "" {
		return nil
	}
	ln, err := listenConsole(ctx, path)
	if err != nil {
		// A host that cannot serve the always-on default (unwritable
		// /run) degrades loudly; an explicit path is a promise.
		if explicit {
			return fmt.Errorf("console socket: %w", err)
		}
		log.Warn("console socket unavailable", zap.String("socket", path), zap.Error(err))
		return nil
	}
	log.Info("console listening", zap.String("socket", path))
	d := *deps
	d.Privilege = cli.Admin
	producers.Go(func() {
		if err := cli.ServeListener(ctx, ln, d); err != nil {
			log.Error("console listener failed", zap.Error(err))
		}
	})
	return nil
}

// listenConsole binds the unix socket, permissions first: the mode is
// the console's whole authentication.
func listenConsole(ctx context.Context, path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	// The instance lock guarantees no live daemon shares this config:
	// whatever socket sits at the path is a previous life's leftover.
	_ = os.Remove(path)
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return ln, nil
}

// assemble resolves one relay's layered configurations — hardware
// against the driver's presets, waveform against the protocol's —
// validates every override scope and the envelope at load time, and
// logs every resolved key with its provenance, so a running config is
// always explainable from the log alone.
// resolvedConfigs is everything the layers say before hardware gets
// involved: the effective key sets, their provenance, and the checks
// every override scope passed. The manager runs this alone to judge a
// mutation; assemble runs it on the way to the radio.
type resolvedConfigs struct {
	drv         radio.Driver
	builder     protocol.Builder
	radioCfg    map[string]any
	relayCfg    map[string]any
	radioTraces []config.Trace
	relayTraces []config.Trace
}

func resolveConfigs(rc config.Relay, radioSpec config.Radio) (*resolvedConfigs, error) {
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
	// Every override scope is checked, not just the selected one: a
	// typo under tomorrow's profile fails today.
	if err := checkScopes(radioSpec.Layered, drv.Presets,
		func(cfg map[string]any) error { _, e := drv.Inspect(cfg); return e }); err != nil {
		return nil, fmt.Errorf("radio %q: %w", rc.Radio, err)
	}
	// Every relay scope is judged against the board too, not just the
	// selected one: a power tomorrow's profile cannot key is a relay
	// that dies the day someone switches to it.
	fits, err := envelopeCheck(drv, radioCfg, builder)
	if err != nil {
		return nil, fmt.Errorf("radio %q: %w", rc.Radio, err)
	}
	if err := checkScopes(rc.Layered, builder.Presets, func(cfg map[string]any) error {
		if builder.Check != nil {
			if err := builder.Check(cfg); err != nil {
				return err
			}
		}
		return fits(cfg)
	}); err != nil {
		return nil, err
	}
	// The selected scope may hold no overrides at all, and then the
	// loop above never saw it.
	if err := fits(relayCfg); err != nil {
		return nil, fmt.Errorf("radio %q: %w", rc.Radio, err)
	}
	return &resolvedConfigs{
		drv: drv, builder: builder,
		radioCfg: radioCfg, relayCfg: relayCfg,
		radioTraces: radioTraces, relayTraces: relayTraces,
	}, nil
}

// assembled is one relay ready to run, with everything the console
// gets to know about it.
type assembled struct {
	relay       *relay.Relay
	info        cli.RelayInfo
	radio       cli.RadioInfo
	radioTraces []config.Trace
	relayTraces []config.Trace
	// relayCfg is the resolved engine configuration, kept for the
	// lock-free deep check an over-the-air set runs on a copy.
	relayCfg map[string]any
}

// otaRunner runs one administration line for a logged-in admin and
// returns what to answer.
type otaRunner = func(line string, admin []byte) string

func assemble(ctx context.Context, name string, rc config.Relay, radioSpec config.Radio,
	b *bus.Bus, log *zap.Logger, sen *sentinel.Sentinel, sessions enginemc.SessionStore,
	commands otaRunner,
) (*assembled, error) {
	rlog := log.With(zap.String("relay", name))
	p, err := prepare(name, rc, radioSpec, spentAirtime(ctx, name, rc, sen), b, rlog)
	if err != nil {
		return nil, err
	}
	logTraces(rlog, "radio config", p.res.radioTraces)
	logTraces(rlog, "relay config", p.res.relayTraces)
	announceOverrides(rlog, p.res.radioTraces, p.res.relayTraces)
	// The session table persists where the protocol keeps one and a
	// store was offered: a companion outlives the bounce, its replay
	// guard with it.
	if sessions != nil {
		if a, ok := p.eng.(interface {
			AttachSessions(store enginemc.SessionStore) error
		}); ok {
			// A store that cannot be read is a stillborn relay, not a
			// relay that quietly starts with no sessions: the table
			// holds every admin's replay guard, and coming up without
			// it rewinds those clocks to zero.
			if err := a.AttachSessions(sessions); err != nil {
				return nil, err
			}
		}
	}
	// Administration from the air runs through the same door the
	// console uses; a protocol with no such door simply has none.
	if commands != nil {
		if a, ok := p.eng.(interface{ AttachCommands(run otaRunner) }); ok {
			a.AttachCommands(commands)
		}
	}
	r := relay.New(name, p.res.drv, p.res.radioCfg, p.eng, b, log, rc.NoiseHistory, p.policy.Mode)
	return &assembled{
		relay:    r,
		relayCfg: p.res.relayCfg,
		info:     relayInfo(name, rc, radioSpec, r, p.eng),
		radio: cli.RadioInfo{
			Name: rc.Radio, Driver: radioSpec.Driver, Envelope: p.env, Relay: name,
		},
		radioTraces: p.res.radioTraces,
		relayTraces: p.res.relayTraces,
	}, nil
}

// prepared is the pre-hardware half of one relay assembly: what a
// configuration must prove before it may replace a running relay —
// or be persisted at all.
type prepared struct {
	res    *resolvedConfigs
	eng    protocol.Engine
	env    radio.Envelope
	policy protocol.TXPolicy
}

// prepare resolves, builds and arms one relay against everything but
// the hardware itself: resolution, engine build, envelope binding,
// transmit resolution, arming. preflight runs it as a judgement and
// discards the result; assemble keeps it and goes on to the radio —
// so the two cannot drift about what a configuration demands, and a
// mutation that would leave a stillborn successor is refused before
// anything persists. spent seeds the duty ledger and may be nil for
// a pure judgement: the rows change what the pipeline may spend,
// never whether the configuration is sound.
func prepare(name string, rc config.Relay, radioSpec config.Radio,
	spent []protocol.Spent, b *bus.Bus, log *zap.Logger,
) (*prepared, error) {
	res, err := resolveConfigs(rc, radioSpec)
	if err != nil {
		return nil, err
	}
	eng, err := res.builder.Build(name, res.relayCfg, b, log)
	if err != nil {
		return nil, err
	}
	env, err := bindEnvelope(res.drv, res.radioCfg, eng)
	if err != nil {
		return nil, fmt.Errorf("radio %q: %w", rc.Radio, err)
	}
	policy, err := resolveTX(rc, env, eng, res.drv, res.radioCfg)
	if err != nil {
		return nil, err
	}
	policy.Spent = spent
	if err := armEngine(rc.Protocol, policy, eng); err != nil {
		return nil, err
	}
	return &prepared{res: res, eng: eng, env: env, policy: policy}, nil
}

// preflight is prepare as a pure judgement: a throwaway bus, a silent
// log, the built engine discarded. What it refuses, assemble would
// have refused two seconds later — after the store was written and
// the running relay stopped.
func preflight(name string, rc config.Relay, radioSpec config.Radio) error {
	_, err := prepare(name, rc, radioSpec, nil, bus.New(), zap.NewNop())
	return err
}

// relayInfo is what the CLI gets to know about an assembled relay.
func relayInfo(name string, rc config.Relay, radioSpec config.Radio,
	r *relay.Relay, eng protocol.Engine,
) cli.RelayInfo {
	return cli.RelayInfo{
		Name:             name,
		Protocol:         rc.Protocol,
		Radio:            rc.Radio,
		Driver:           radioSpec.Driver,
		Waveform:         eng.Waveform(),
		State:            r.State,
		Err:              r.Err,
		NoiseFloor:       r.NoiseFloor,
		ChipStats:        r.ChipStats,
		TXMode:           r.TXMode(),
		Duty:             dutyOf(eng),
		Scopes:           scopesOf(eng),
		DefaultScope:     defaultScopeOf(eng),
		Sign:             signOf(eng),
		AskScopes:        askScopesOf(eng),
		Discover:         discoverOf(eng),
		ScanWindow:       scanWindowOf(eng),
		TriggerAdvert:    advertTrigger(eng),
		Neighbours:       neighboursOf(eng),
		RemoveNeighbours: removeNeighboursOf(eng),
		AirSessions:      airSessionsOf(eng),
		Access:           accessOf(eng),
		Grant:            grantOf(eng),
		GrantRole:        grantRoleOf(eng),
		Revoke:           roleDoor(eng, enginemc.PermGuest),
		Identity:         eng.Identity(),
		Started:          time.Now(),
		NodeName:         nodeNameOf(eng),
		Traffic:          trafficOf(eng),
	}
}

// scanWindowOf exposes the open scan window, for the joiners.
func scanWindowOf(eng protocol.Engine) func() (time.Time, bool) {
	w, ok := eng.(interface{ ScanWindow() (time.Time, bool) })
	if !ok {
		return nil
	}
	return w.ScanWindow
}

// defaultScopeOf reads which scope the relay itself speaks under.
func defaultScopeOf(eng protocol.Engine) string {
	d, ok := eng.(interface{ DefaultScope() string })
	if !ok {
		return ""
	}
	return d.DefaultScope()
}

// signOf exposes the engine's identity signature when it has one to
// sign with; nil otherwise.
func signOf(eng protocol.Engine) func([]byte) []byte {
	sg, ok := eng.(interface{ IdentitySign(message []byte) []byte })
	if !ok {
		return nil
	}
	return sg.IdentitySign
}

// nodeNameOf reads what the relay calls itself on the air, when the
// protocol has a name at all.
func nodeNameOf(eng protocol.Engine) string {
	n, ok := eng.(interface{ NodeName() string })
	if !ok {
		return ""
	}
	return n.NodeName()
}

// trafficOf exposes the engine's lifetime tally, for the observers'
// heartbeat; nil when the protocol keeps none.
func trafficOf(eng protocol.Engine) func() (uint32, uint32, uint32, time.Duration, time.Duration) {
	t, ok := eng.(interface{ TrafficStats() enginemc.StatsSnapshot })
	if !ok {
		return nil
	}
	return func() (sent, received, recvErrors uint32, txAir, rxAir time.Duration) {
		s := t.TrafficStats()
		return s.SentFlood + s.SentDirect, s.RecvTotal, s.RecvErrors,
			s.TxAirtime, s.RxAirtime
	}
}

// removeNeighboursOf exposes an engine's neighbour removal.
func removeNeighboursOf(eng protocol.Engine) func(prefix []byte) int {
	r, ok := eng.(interface{ RemoveNeighbours(prefix []byte) int })
	if !ok {
		return nil
	}
	return r.RemoveNeighbours
}

// neighboursOf exposes an engine's direct neighbourhood when it keeps
// one, converted to the console's own row type.
func neighboursOf(eng protocol.Engine) func() []cli.Neighbour {
	n, ok := eng.(interface{ Neighbours() []enginemc.Neighbour })
	if !ok {
		return nil
	}
	return func() []cli.Neighbour {
		rows := n.Neighbours()
		if rows == nil {
			return nil
		}
		out := make([]cli.Neighbour, len(rows))
		for i, r := range rows {
			out[i] = cli.Neighbour{PubKey: r.PubKey, Name: r.Name, SNR: r.SNR, Heard: r.Heard}
		}
		return out
	}
}

// accessOf exposes an engine's access list — grants and sessions —
// when it keeps one.
func accessOf(eng protocol.Engine) func() ([]cli.Access, error) {
	a, ok := eng.(interface {
		AccessList() ([]enginemc.ACLEntry, error)
	})
	if !ok {
		return nil
	}
	return func() ([]cli.Access, error) {
		rows, err := a.AccessList()
		if err != nil {
			return nil, err
		}
		out := make([]cli.Access, len(rows))
		for i, r := range rows {
			out[i] = cli.Access{
				PubKey: r.PubKey, Role: enginemc.RoleName(r.Perms),
				Granted: r.Granted, LastActive: r.LastActive,
			}
		}
		return out, nil
	}
}

// grantRoleOf translates a role's word at the boundary — RoleByte is
// the one dictionary — and refuses granting the guest word, which is
// a removal and has its own verb.
func grantRoleOf(eng protocol.Engine) func(pubKey []byte, role string) error {
	g := grantOf(eng)
	if g == nil {
		return nil
	}
	return func(pubKey []byte, role string) error {
		perms, ok := enginemc.RoleByte(role)
		if !ok || perms == enginemc.PermGuest {
			return fmt.Errorf("no role %q — admin, read-write or read-only", role)
		}
		return g(pubKey, perms)
	}
}

// roleDoor fixes one named role onto the grant door — the console's
// grant and revoke, with the number written exactly once, in the
// protocol's own constants.
func roleDoor(eng protocol.Engine, perms byte) func(pubKey []byte) error {
	g := grantOf(eng)
	if g == nil {
		return nil
	}
	return func(pubKey []byte) error { return g(pubKey, perms) }
}

// grantOf exposes an engine's grant door, the permission byte passed
// through whole.
func grantOf(eng protocol.Engine) func(pubKey []byte, perms byte) error {
	g, ok := eng.(interface {
		Grant(pubKey []byte, perms byte) error
	})
	if !ok {
		return nil
	}
	return g.Grant
}

// airSessionsOf exposes an engine's over-the-air session table when
// it keeps one, converted to the console's own row type.
func airSessionsOf(eng protocol.Engine) func() ([]cli.AirSession, error) {
	a, ok := eng.(interface {
		ClientSessions() ([]enginemc.ClientSession, error)
	})
	if !ok {
		return nil
	}
	return func() ([]cli.AirSession, error) {
		rows, err := a.ClientSessions()
		if err != nil {
			return nil, err
		}
		out := make([]cli.AirSession, len(rows))
		for i, r := range rows {
			out[i] = cli.AirSession{
				PubKey: r.PubKey, Admin: r.Admin,
				Path: r.Path, HasPath: r.HasPath,
				PathLearned: r.PathLearned, LastActive: r.LastActive,
			}
		}
		return out, nil
	}
}

// advertTrigger exposes an engine's operator-advert order when it has
// a transmit pipeline; nil otherwise.
func advertTrigger(eng protocol.Engine) func(bool) error {
	a, ok := eng.(interface{ RequestAdvert(flood bool) error })
	if !ok {
		return nil
	}
	return a.RequestAdvert
}

// scopesOf exposes the transport scopes an engine carries, when it
// speaks a protocol that has any.
func scopesOf(eng protocol.Engine) []string {
	s, ok := eng.(interface{ Scopes() []string })
	if !ok {
		return nil
	}
	return s.Scopes()
}

// askScopesOf exposes the question an operator may put to a
// neighbour, when the protocol has scopes to ask about.
func askScopesOf(eng protocol.Engine) func(prefix []byte) ([]string, error) {
	a, ok := eng.(interface {
		Neighbour(prefix []byte) ([]byte, error)
		AskScopes(peer []byte) ([]string, error)
	})
	if !ok {
		return nil
	}
	return func(prefix []byte) ([]string, error) {
		peer, err := a.Neighbour(prefix)
		if err != nil {
			return nil, err
		}
		return a.AskScopes(peer)
	}
}

// discoverOf exposes the neighbourhood scan, when the protocol has
// one to run.
func discoverOf(eng protocol.Engine) func() (<-chan cli.Neighbour, time.Time, error) {
	d, ok := eng.(interface {
		Discover() (<-chan enginemc.Neighbour, time.Time, error)
	})
	if !ok {
		return nil
	}
	return func() (<-chan cli.Neighbour, time.Time, error) {
		found, until, err := d.Discover()
		if err != nil {
			return nil, time.Time{}, err
		}
		// The console has its own row type; translate as answers land
		// rather than making it wait for the whole window.
		out := make(chan cli.Neighbour, cap(found))
		go func() {
			defer close(out)
			for n := range found {
				out <- cli.Neighbour{PubKey: n.PubKey, Name: n.Name, SNR: n.SNR, Heard: n.Heard}
			}
		}()
		return out, until, nil
	}
}

// dutyOf exposes an engine's duty gauge when it has one.
func dutyOf(eng protocol.Engine) func() (time.Duration, time.Duration, bool) {
	d, ok := eng.(interface {
		Duty() (time.Duration, time.Duration, bool)
	})
	if !ok {
		return nil
	}
	return d.Duty
}

// spentAirtime reads the journal's memory of the sliding hour, when a
// journal runs at all: the budget must not restart with the process,
// or a crash-loop would launder it. Best effort — a sick journal
// degrades to an empty hour, never to a dead relay. Dry spends
// nothing and reads nothing.
func spentAirtime(ctx context.Context, relay string, rc config.Relay, sen *sentinel.Sentinel) []protocol.Spent {
	if rc.TXMode() == config.TXDry || sen == nil {
		return nil
	}
	rows, err := sen.SpentAirtime(ctx, relay)
	if err != nil {
		return nil
	}
	spent := make([]protocol.Spent, 0, len(rows))
	for _, r := range rows {
		spent = append(spent, protocol.Spent{At: r.At, Airtime: r.Airtime})
	}
	return spent
}

// armEngine hands a non-dry policy to the engine's pipeline; an
// engine without one, or one that refuses, is a stillborn relay.
func armEngine(protocolName string, policy protocol.TXPolicy, eng protocol.Engine) error {
	if policy.Mode == config.TXDry {
		return nil
	}
	armer, ok := eng.(protocol.Armer)
	if !ok {
		return fmt.Errorf("tx: protocol %q has no transmit pipeline", protocolName)
	}
	return armer.Arm(policy)
}

// resolveTX enforces the transmit gate locks at assembly and resolves
// the policy the engine is armed with: shadow and on-air need a power
// the pipeline can honestly account for — "auto" resolves against the
// board's declared ceiling — and on-air additionally refuses to exist
// without that ceiling. A violation is a stillborn relay, never a
// silent dry.
func resolveTX(rc config.Relay, env radio.Envelope, eng protocol.Engine,
	drv radio.Driver, radioCfg map[string]any,
) (protocol.TXPolicy, error) {
	policy := protocol.TXPolicy{Mode: rc.TXMode()}
	if policy.Mode == config.TXDry {
		return policy, nil
	}
	// What the radio itself requires before it can key at all: a
	// relay that could never transmit must not spend its life
	// reopening the radio and failing on the first frame.
	if drv.CheckTransmit != nil {
		if err := drv.CheckTransmit(radioCfg); err != nil {
			return policy, fmt.Errorf("tx: mode %s: %w", policy.Mode, err)
		}
	}
	policy.LBTThresholdDB = rc.TX.LBTThresholdDB
	policy.LBTExhausted = rc.TX.LBTExhausted
	policy.QueueDepth = rc.TX.QueueDepth
	policy.CAD = rc.TX.CAD == nil || *rc.TX.CAD
	dbm, explicit := eng.TxPower()
	if !explicit {
		if !env.MaxTxPowerSet {
			return policy, fmt.Errorf(
				"tx: mode %s with tx_power_dbm auto needs the radio's max_tx_power_dbm declared", policy.Mode)
		}
		dbm = env.MaxTxPowerDBm
	}
	if (policy.Mode == config.TXOnAir || policy.Mode == config.TXOnAirZeroHop) &&
		!env.MaxTxPowerSet {
		return policy, fmt.Errorf("tx: %s requires the radio's max_tx_power_dbm declared", policy.Mode)
	}
	policy.PowerDBm = dbm
	return policy, nil
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
	// What the chip can be programmed with, asked of the driver with
	// the very conversion Configure runs: an envelope only knows
	// about frequency and power, so a spreading factor or bandwidth
	// the part does not accept used to pass here and fail at every
	// Configure, forever.
	if drv.CheckWaveform != nil {
		if err := drv.CheckWaveform(eng.Waveform()); err != nil {
			return env, err
		}
	}
	dbm, explicit := eng.TxPower()
	if err := env.Permits(eng.Waveform(), dbm, explicit); err != nil {
		return env, err
	}
	return env, nil
}

// envelopeCheck reads the board once and returns the judgement that a
// relay configuration asks nothing the board cannot serve. assemble
// asks the same of the engine it built, but a refusal there is a
// relay that does not come up; asked here it is a mutation refused,
// the running relay untouched. What cannot answer passes.
func envelopeCheck(drv radio.Driver, radioCfg map[string]any,
	builder protocol.Builder,
) (func(map[string]any) error, error) {
	pass := func(map[string]any) error { return nil }
	if drv.Inspect == nil || builder.Asks == nil {
		return pass, nil
	}
	env, err := drv.Inspect(radioCfg)
	if err != nil {
		return nil, err
	}
	return func(cfg map[string]any) error {
		w, dbm, explicit, err := builder.Asks(cfg)
		if err != nil {
			return err
		}
		// Two questions, and the driver's comes first: whether the
		// chip can be programmed with this waveform at all, before
		// whether the board may key there.
		if drv.CheckWaveform != nil {
			if err := drv.CheckWaveform(w); err != nil {
				return err
			}
		}
		return env.Permits(w, dbm, explicit)
	}, nil
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

// newLogger builds the daemon's logger and hands back the level knob:
// the boot flag decides where it starts, the console may move it while
// the daemon runs.
func newLogger(level string) (*zap.Logger, zap.AtomicLevel, error) {
	lvl, err := logging.ParseLevel(level)
	if err != nil {
		return nil, zap.AtomicLevel{}, fmt.Errorf("log level: %w", err)
	}
	atomic := zap.NewAtomicLevelAt(lvl)
	cfg := zap.NewProductionConfig()
	cfg.Level = atomic
	cfg.Encoding = "console"
	// Retry loops make errors an expected operational state; the
	// message and fields carry the story, a stack trace adds nothing.
	cfg.DisableStacktrace = true
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.EncodeLevel = logging.EncodeLevel
	log, err := cfg.Build()
	return log, atomic, err
}
