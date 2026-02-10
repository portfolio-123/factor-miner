#!/usr/bin/env bash
set -e

IMAGE=factorminer
REGISTRY=quay300.portfolio123.com/misc

if [[ -z "$1" ]]; then
  echo "Usage: $0 <tag>"
  exit 1
fi

TAG="$1"

docker build -t "$IMAGE:$TAG" -f deploy.Dockerfile ../
docker image tag "$IMAGE:$TAG" "$REGISTRY/$IMAGE:$TAG"
docker image push "$REGISTRY/$IMAGE:$TAG"
