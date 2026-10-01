#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR="${1:-release}"
: "${RELEASE_TAG:?RELEASE_TAG is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
: "${GITHUB_SHA:?GITHUB_SHA is required}"
IMAGE_REPOSITORY="$(printf '%s' "$GITHUB_REPOSITORY" | tr '[:upper:]' '[:lower:]')"

mkdir -p "$OUTPUT_DIR"
cp .env.prod.template "$OUTPUT_DIR/.env.prod.template"
sed -i.bak "s/^IMAGE_TAG=.*/IMAGE_TAG=${RELEASE_TAG}/" "$OUTPUT_DIR/.env.prod.template"
sed -i.bak "s|^GITHUB_REPOSITORY=.*|GITHUB_REPOSITORY=${IMAGE_REPOSITORY}|" "$OUTPUT_DIR/.env.prod.template"
rm -f "$OUTPUT_DIR/.env.prod.template.bak"

{
  printf '{\n'
  printf '  "commit": "%s",\n' "$GITHUB_SHA"
  printf '  "tag": "%s",\n' "$RELEASE_TAG"
  printf '  "images": [\n'
  for image in ingestion spark-streaming features training serving frontend orchestration; do
    comma=,
    if [[ "$image" == "orchestration" ]]; then comma=; fi
    printf '    "ghcr.io/%s/%s:%s"%s\n' "$IMAGE_REPOSITORY" "$image" "$RELEASE_TAG" "$comma"
  done
  printf '  ]\n'
  printf '}\n'
} > "$OUTPUT_DIR/release.json"
