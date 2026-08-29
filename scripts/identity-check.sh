#!/usr/bin/env bash
# Verify the mechanical identity invariants in the places Go cannot
# reach: the systemd units and the human documentation must agree with
# internal/product — the official URL, the service name, the installed
# binary and the state directory. README and DESIGN stay handwritten;
# this only checks that their hard facts have not drifted.
set -euo pipefail
cd "$(dirname "$0")/.."

meta() { go run ./internal/product/cmd/meta -field "$1"; }
slug="$(meta slug)"; homepage="$(meta homepage)"
binary="$(meta binary)"; statedir="$(meta state-dir)"; service="$(meta service)"

fail=0
say() { echo "identity: $*" >&2; fail=1; }

unit="contrib/systemd/${service}.service"
[ -f "$unit" ] || say "no unit at $unit"
grep -q "ExecStart=${binary}" "$unit" || say "$unit does not exec ${binary}"
grep -q "StateDirectory=${slug}\$" "$unit" || say "$unit does not own ${statedir}"

for doc in README.md DESIGN.md; do
  [ -f "$doc" ] || continue
  grep -q "$homepage" "$doc" || say "$doc has lost the official URL ${homepage}"
done

# The update base must be the product's, wherever a default names one.
grep -rq "updates.meshrunner.dev/${slug}" internal/product/product.go ||
  say "product.UpdateBase drifted"

# The publish chain must ASK for the slug, never spell it: a renamed
# product would otherwise build under the new name and publish under
# the old. The workflows and their scripts read product/meta; any
# literal occurrence of the slug there is a drift waiting to happen.
hardcoded="$(grep -rn "\b${slug}_\|\b${slug}/" .github/workflows/ .github/scripts/   | grep -v "internal/product" || true)"
[ -z "$hardcoded" ] || say "the publish chain spells the slug instead of asking product/meta:
$hardcoded"

[ "$fail" -eq 0 ] && echo "identity: ok"
exit "$fail"
