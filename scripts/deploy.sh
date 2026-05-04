#!/bin/zsh

set -e

BINARY=/home/eugene/.local/bin/clip-sync
SERVICE=clip-sync
REPO_ROOT=$(git -C "$(dirname $0)" rev-parse --show-toplevel)

cd "$REPO_ROOT"

if [[ -z "$1" ]]; then
    echo "usage: deploy.sh <commit message>"
    exit 1
fi

echo "==> test"
go test ./...

echo "==> build"
go build -o "$BINARY" .

echo "==> restart $SERVICE"
systemctl --user restart "$SERVICE"
systemctl --user is-active --quiet "$SERVICE" && echo "    service running" || { echo "    service failed"; exit 1; }

echo "==> commit and push"
git add -A
git commit -m "$1"
git push

echo "==> done"
