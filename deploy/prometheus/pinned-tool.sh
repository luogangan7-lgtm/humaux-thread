#!/bin/sh
# pinned-tool.sh <promtool|prometheus|alertmanager|amtool|otelcol> [args...]
#
# Runs one pinned observability binary (ADR-0061 D-I) after verifying it, then execs it with the
# remaining arguments (relative paths resolve against the caller's cwd). Never resolves from PATH.
# Pins: HUMAUX_TEST_<TOOL>_{BIN,SHA256,VERSION}, exported by the TW live_env.sh; amtool ships in the
# Alertmanager tarball and is checked against HUMAUX_TEST_ALERTMANAGER_VERSION (research addendum W2).
#
# Exit codes: 2 = not_applicable (a pin variable is unset or empty; the variable is named, §57.1);
#             1 = fail (unknown tool, path not an absolute executable file, sha256 or version mismatch);
#             otherwise the tool's own exit code.
set -u

tool=${1:-}
case "$tool" in
  promtool)     p=PROMTOOL;     v=PROMTOOL ;;
  prometheus)   p=PROMETHEUS;   v=PROMETHEUS ;;
  alertmanager) p=ALERTMANAGER; v=ALERTMANAGER ;;
  amtool)       p=AMTOOL;       v=ALERTMANAGER ;;
  otelcol)      p=OTELCOL;      v=OTELCOL ;;
  *) echo "pinned-tool: fail — unknown tool '$tool' (promtool|prometheus|alertmanager|amtool|otelcol)" >&2; exit 1 ;;
esac
shift

for name in "HUMAUX_TEST_${p}_BIN" "HUMAUX_TEST_${p}_SHA256" "HUMAUX_TEST_${v}_VERSION"; do
  eval "val=\${$name:-}"
  if [ -z "$val" ]; then
    echo "pinned-tool: not_applicable — missing object: $name" >&2
    exit 2
  fi
done
eval "bin=\$HUMAUX_TEST_${p}_BIN; want=\$HUMAUX_TEST_${p}_SHA256; ver=\$HUMAUX_TEST_${v}_VERSION"

case "$bin" in
  /*) ;;
  *) echo "pinned-tool: fail — HUMAUX_TEST_${p}_BIN is not an absolute path" >&2; exit 1 ;;
esac
if [ ! -f "$bin" ] || [ ! -x "$bin" ]; then
  echo "pinned-tool: fail — $bin is not an executable file" >&2
  exit 1
fi
got=$(shasum -a 256 "$bin" | cut -d' ' -f1)
if [ "$got" != "$want" ]; then
  echo "pinned-tool: fail — sha256 mismatch for $tool ($bin)" >&2
  exit 1
fi
if ! "$bin" --version 2>&1 | grep -qF "version $ver"; then
  echo "pinned-tool: fail — $bin --version does not report version $ver" >&2
  exit 1
fi
exec "$bin" "$@"
