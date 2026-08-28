#!/usr/bin/env bash
# Publish manifest.json{,.minisig} from the working directory onto the
# update host, under lotor/<channel>/, and refresh channels.json — the
# discovery file listing what is published. The host is a plain static
# tree in the updates repository: leaving GitHub one day is an rsync
# of this tree and new artifact URLs in fresh manifests, nothing else.
set -euo pipefail
channel="$1"

eval "$(ssh-agent -s)" >/dev/null
ssh-add - <<< "$DEPLOY_KEY" 2>/dev/null
mkdir -p ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null

git clone --depth 1 "git@github.com:${GITHUB_REPOSITORY_OWNER}/updates.git" /tmp/updates
mkdir -p "/tmp/updates/lotor/${channel}"
cp manifest.json manifest.json.minisig "/tmp/updates/lotor/${channel}/"

(cd /tmp/updates/lotor && ls -d */ | sed 's#/$##' \
  | jq -R . | jq -s '{product: "lotor", channels: .}' > channels.json)

cd /tmp/updates
git add -A
git -c user.name=relbot -c user.email=bots@meshrunner.dev \
  commit -m "lotor ${channel}: $(jq -r .version "lotor/${channel}/manifest.json")"
git push
rm -rf /tmp/updates
