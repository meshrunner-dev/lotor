// relsign is the release-signing tool: mint a key pair, sign a
// manifest, check one. The CI is its only real user — lotor itself
// only ever verifies — but the verify half makes a workflow able to
// prove its own output before publishing it.
//
// The signatures are stock-minisign compatible, so an operator can
// check a manifest by hand without this tool. The secret key file is
// relsign's own two-line form, made to live in a CI secret.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"meshrunner.dev/lotor/internal/update"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "relsign: %s\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: relsign keygen <dir> | sign <keyfile> <file> | verify <pubfile> <file>")
	}
	switch cmd, rest := args[0], args[1:]; cmd {
	case "keygen":
		return keygen(rest)
	case "sign":
		return sign(rest)
	case "verify":
		return verify(rest)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// keygen writes relsign.key and relsign.pub into dir. The secret is
// created 0600 and never printed: it goes into a CI secret by file,
// not by scrollback.
func keygen(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: relsign keygen <dir>")
	}
	sec, pub, err := update.GenerateKey()
	if err != nil {
		return err
	}
	keyPath := filepath.Join(args[0], "relsign.key")
	if _, err := statArg(keyPath); err == nil {
		return fmt.Errorf("%s already exists — a signing key is not a thing to overwrite", keyPath)
	}
	if err := writeArg(keyPath, update.MarshalSecret(sec), 0o600); err != nil {
		return err
	}
	if err := writeArg(filepath.Join(args[0], "relsign.pub"), update.MarshalPublic(pub), 0o644); err != nil {
		return err
	}
	fmt.Printf("key %s\nsecret: %s\npublic: %s\n",
		pub.Hex(), keyPath, filepath.Join(args[0], "relsign.pub"))
	return nil
}

// The paths below are the operator's own arguments: a CLI that
// refuses the file you named is a CLI that cannot be used, which is
// what the linter's taint reading would make of this one.

func readArg(path string) ([]byte, error) {
	return os.ReadFile(path) //nolint:gosec // operator-chosen path
}

func writeArg(path string, data []byte, mode os.FileMode) error {
	return os.WriteFile(path, data, mode) //nolint:gosec // operator-chosen path
}

func statArg(path string) (os.FileInfo, error) {
	return os.Stat(path) //nolint:gosec // operator-chosen path
}

// sign writes <file>.minisig beside the file.
func sign(args []string) error {
	if len(args) != 2 {
		return errors.New("usage: relsign sign <keyfile> <file>")
	}
	keyText, err := readArg(args[0])
	if err != nil {
		return err
	}
	key, err := update.ParseSecret(keyText)
	if err != nil {
		return err
	}
	content, err := readArg(args[1])
	if err != nil {
		return err
	}
	sig := update.Sign(content, key, "file:"+filepath.Base(args[1]))
	return writeArg(args[1]+".minisig", sig, 0o644)
}

// verify checks <file> against <file>.minisig under one public key.
func verify(args []string) error {
	if len(args) != 2 {
		return errors.New("usage: relsign verify <pubfile> <file>")
	}
	pubText, err := readArg(args[0])
	if err != nil {
		return err
	}
	pub, err := update.ParsePublicKey(pubText)
	if err != nil {
		return err
	}
	content, err := readArg(args[1])
	if err != nil {
		return err
	}
	sigText, err := readArg(args[1] + ".minisig")
	if err != nil {
		return err
	}
	key, err := update.Verify(content, sigText, []update.PublicKey{pub})
	if err != nil {
		return err
	}
	fmt.Printf("verified under key %s\n", key.Hex())
	return nil
}
