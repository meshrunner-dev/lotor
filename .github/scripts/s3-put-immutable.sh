#!/usr/bin/env bash
# Put one object into the train's bucket and refuse to overwrite: the
# buckets are immutable by design. Every build owns its version and
# every version owns its keys, so an existing object can only mean a
# version collision or a replayed job — both must fail loudly, neither
# may replace bytes a manifest might already name. head-then-put
# rather than a conditional PUT because every S3-compatible host
# speaks HEAD; the race this leaves open needs two concurrent writers
# on one version, which the version scheme already rules out.
#
# Expects AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY in the environment,
# plus S3_ENDPOINT and S3_BUCKET — the per-train values an Actions
# environment carries, so the fast train's credentials cannot touch
# the stable train's bytes any more than its key can sign for it.
set -euo pipefail
key="$1" file="$2"

s3() { aws s3api "$1" --endpoint-url "$S3_ENDPOINT" --bucket "$S3_BUCKET" "${@:2}"; }

if s3 head-object --key "$key" >/dev/null 2>&1; then
  echo "s3://${S3_BUCKET}/${key} already exists — artifacts are immutable, refusing" >&2
  exit 1
fi
# Immutable bytes may say so to every cache on the way.
s3 put-object --key "$key" --body "$file" \
  --content-type application/gzip \
  --cache-control "public, max-age=31536000, immutable" >/dev/null
echo "uploaded s3://${S3_BUCKET}/${key}"
