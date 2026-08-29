#!/usr/bin/env bash
# Verify one built binary WITHOUT running it — `go version -m` reads
# the embedded metadata, so an ARM binary checks fine from an amd64
# runner. Everything the build claimed must be what the bytes carry:
# version, full revision, a clean tree, target and toolchain.
#
#   verify-binary.sh <file> <version> <revision> <goos> <goarch> [goarm]
set -euo pipefail
file="$1" version="$2" revision="$3" goos="$4" goarch="$5" goarm="${6:-}"

meta="$(go version -m -json "$file")"
get() { echo "$meta" | jq -r "$1"; }
setting() { echo "$meta" | jq -r ".Settings[] | select(.Key == \"$1\") | .Value"; }

fail=0
expect() { # name got want
  if [ "$2" != "$3" ]; then echo "verify: $1 = '$2', want '$3'" >&2; fail=1; fi
}
expect vcs.revision "$(setting vcs.revision)" "$revision"
expect vcs.modified "$(setting vcs.modified)" "false"
expect GOOS "$(setting GOOS)" "$goos"
expect GOARCH "$(setting GOARCH)" "$goarch"
[ -n "$goarm" ] && expect GOARM "$(setting GOARM)" "$goarm"
expect toolchain "$(get .GoVersion)" "$(go env GOVERSION)"
# -trimpath keeps -ldflags out of the embedded settings (flags may
# carry local paths), so the stamp is checked where it actually lives:
# the -X value is a literal in the binary's data.
if ! grep -qaF "$version" "$file"; then
  echo "verify: the bytes do not carry the version ${version}" >&2; fail=1
fi
[ "$fail" -eq 0 ] && echo "verify: ${file} ok"
exit "$fail"
