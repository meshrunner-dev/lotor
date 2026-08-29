#!/usr/bin/env bash
# Sweep the fast train's bucket: try manifests older than two weeks
# go, and the version they point at goes with them; dev versions
# expire on the same clock, all but the one the live manifest names.
# The manifest is the authority throughout: nothing a live manifest
# names is ever deleted. Runs from a checkout, so the product's slug
# comes from the source of truth like everywhere else.
set -euo pipefail
cd "$(dirname "$0")/../.."
product="$(go run ./internal/product/cmd/meta -field slug)"

eval "$(ssh-agent -s)"
ssh-add - <<< "$DEPLOY_KEY"
mkdir -p ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null
git clone --depth 1 "git@github.com:${GITHUB_REPOSITORY_OWNER}/updates.git" /tmp/updates
cutoff=$(date -u -d "14 days ago" +%s)

# Every object under one version's prefix, deleted by name.
sweep_version() {
  aws s3api list-objects-v2 --endpoint-url "$S3_ENDPOINT" --bucket "$S3_BUCKET" \
    --prefix "${product}/$1/" --query 'Contents[].Key' --output text 2>/dev/null \
    | tr '\t' '\n' | while read -r key; do
    [ -n "$key" ] && [ "$key" != "None" ] || continue
    aws s3api delete-object --endpoint-url "$S3_ENDPOINT" --bucket "$S3_BUCKET" \
      --key "$key" >/dev/null
    echo "deleted s3://${S3_BUCKET}/${key}"
  done
}

# Try channels: a stale manifest goes, and the one version it names
# goes with it — derived from the manifest itself, so the sweep never
# guesses at keys.
for dir in "/tmp/updates/${product}"/try-*/; do
  [ -d "$dir" ] || continue
  published=$(jq -r .published "$dir/manifest.json" 2>/dev/null || echo "")
  [ -n "$published" ] && [ "$(date -u -d "$published" +%s)" -gt "$cutoff" ] && continue
  version=$(jq -r .version "$dir/manifest.json" 2>/dev/null || echo "")
  [ -n "$version" ] && sweep_version "$version"
  echo "sweeping $(basename "$dir")"
  rm -r "$dir"
done

# Dev versions: every push minted one, and each carries its instant
# in its own name — no object metadata needed. All but the version
# the live manifest names expire.
current=$(jq -r .version "/tmp/updates/${product}/dev/manifest.json" 2>/dev/null || echo "")
aws s3api list-objects-v2 --endpoint-url "$S3_ENDPOINT" --bucket "$S3_BUCKET" \
  --prefix "${product}/" --query 'Contents[].Key' --output text 2>/dev/null \
  | tr '\t' '\n' | sed -n "s#^${product}/\([^/]*-dev\.[^/]*\)/.*#\1#p" | sort -u \
  | while read -r version; do
  [ "$version" = "$current" ] && continue
  stamp=$(echo "$version" | sed -n 's/.*-dev\.\([0-9]\{14\}\)\..*/\1/p')
  [ -n "$stamp" ] || continue
  born=$(date -u -d "${stamp:0:8} ${stamp:8:2}:${stamp:10:2}:${stamp:12:2}" +%s)
  [ "$born" -gt "$cutoff" ] && continue
  sweep_version "$version"
done

cd /tmp/updates
git add -A
git diff --cached --quiet && exit 0
git -c user.name=relbot -c user.email=bots@meshrunner.dev \
  commit -m "chore: sweep stale try channels"
git push
rm -rf /tmp/updates
