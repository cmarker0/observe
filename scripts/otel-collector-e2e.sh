#!/usr/bin/env bash
# Runs src/recorder/otel-collector.int-spec.ts against a real OpenTelemetry
# Collector. Uses $OTELCOL_BIN when set; otherwise downloads the core
# distribution once into node_modules/.cache/otelcol.
set -euo pipefail

VERSION="${OTELCOL_VERSION:-0.137.0}"
cd "$(dirname "$0")/.."

if [[ -z "${OTELCOL_BIN:-}" ]]; then
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) platform=linux_amd64 ;;
    Linux-aarch64) platform=linux_arm64 ;;
    Darwin-x86_64) platform=darwin_amd64 ;;
    Darwin-arm64) platform=darwin_arm64 ;;
    *) echo "No collector build for $(uname -s)-$(uname -m); set OTELCOL_BIN." >&2; exit 1 ;;
  esac
  dir="node_modules/.cache/otelcol/${VERSION}"
  OTELCOL_BIN="${dir}/otelcol"
  if [[ ! -x "${OTELCOL_BIN}" ]]; then
    mkdir -p "${dir}"
    curl -fsSL "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v${VERSION}/otelcol_${VERSION}_${platform}.tar.gz" |
      tar -xz -C "${dir}" otelcol
  fi
fi

OTELCOL_BIN="$(cd "$(dirname "${OTELCOL_BIN}")" && pwd)/$(basename "${OTELCOL_BIN}")" \
  exec npx vitest run --config vitest.int.config.ts src/recorder/otel-collector.int-spec.ts
