#!/bin/sh
# check-compose.sh [compose-file [otel-collector.yml [alertmanager.yml]]] — the `compose_observability`
# gate (ADR-0061 D-G, D-I).
#
# Renders the compose file (default deploy/compose/observability.yml) with `docker compose config`
# (client-side; also catches YAML and interpolation errors) and reads the two configs the compose
# services mount (defaults: this directory's otel-collector.yml and alertmanager.yml; neither carries a
# deployer placeholder on a listener line, so the repo file is the rendered one). Fails, naming the
# service or file and the offending value, on:
#   - an image that is not `name:tag@sha256:<64 hex>` (a tag-only image is not a pin);
#   - a Prometheus / Alertmanager / Collector tag different from the pinned
#     HUMAUX_TEST_{PROMETHEUS,ALERTMANAGER,OTELCOL}_VERSION, or HUMAUX_TEST_PROMTOOL_VERSION !=
#     HUMAUX_TEST_PROMETHEUS_VERSION (rules are tested on the version that is deployed);
#   - any `0.0.0.0`; a Prometheus or Alertmanager command without `--web.listen-address=127.0.0.1:`;
#     an Alertmanager command without an empty `--cluster.listen-address=`;
#   - any of --web.enable-remote-write-receiver, --web.enable-otlp-receiver, --web.enable-admin-api,
#     --web.enable-lifecycle (an unauthenticated write path could mute INV-1 or BackupFailure);
#   - an otel-collector.yml listener (`endpoint:` of a receiver protocol, `host:` of the telemetry
#     reader) that is not 127.0.0.1, fewer than the three such lines D-G names (grpc, http, telemetry),
#     or any `0.0.0.0` in the file (ADR-0061 review-fix 3, F1);
#   - an alertmanager.yml line that opens a listener or a cluster peer (`listen`, `cluster`, `peer`,
#     `0.0.0.0`): gossip is a CLI flag only, so the config must not carry one either.
# The two deployer directories are given dummy values: only the file's own content is under test.
#
# Exit codes: 0 = pass; 1 = fail (each violation printed); 2 = not_applicable (a version pin variable
#             is unset, or docker is absent — the missing object is named, §57.1).
set -u
here=$(cd "$(dirname "$0")" && pwd)
file=${1:-$here/../compose/observability.yml}
otel=${2:-$here/otel-collector.yml}
am=${3:-$here/alertmanager.yml}
for f in "$otel" "$am"; do
  [ -r "$f" ] || { echo "check-compose: fail — cannot read $f"; exit 1; }
done

for name in HUMAUX_TEST_PROMTOOL_VERSION HUMAUX_TEST_PROMETHEUS_VERSION HUMAUX_TEST_ALERTMANAGER_VERSION HUMAUX_TEST_OTELCOL_VERSION; do
  eval "val=\${$name:-}"
  if [ -z "$val" ]; then
    echo "check-compose: not_applicable — missing object: $name"
    exit 2
  fi
done
if ! command -v docker >/dev/null 2>&1; then
  echo "check-compose: not_applicable — missing object: docker (docker compose config)"
  exit 2
fi

json=$(HUMAUX_OBSERVABILITY_RENDERED_DIR=/check-compose/rendered HUMAUX_ALERTMANAGER_URL_DIR=/check-compose/urls \
  docker compose -f "$file" config --format json) || { echo "check-compose: fail — docker compose config rejected $file"; exit 1; }

printf '%s' "$json" | OTEL_FILE="$otel" AM_FILE="$am" python3 -c '
import json, os, re, sys
d = json.load(sys.stdin)
env = os.environ
pins = {
    "prom/prometheus": "v" + env["HUMAUX_TEST_PROMETHEUS_VERSION"],
    "prom/alertmanager": "v" + env["HUMAUX_TEST_ALERTMANAGER_VERSION"],
    "otel/opentelemetry-collector": env["HUMAUX_TEST_OTELCOL_VERSION"],
}
forbidden = ("--web.enable-remote-write-receiver", "--web.enable-otlp-receiver",
             "--web.enable-admin-api", "--web.enable-lifecycle")
fails = []
if env["HUMAUX_TEST_PROMTOOL_VERSION"] != env["HUMAUX_TEST_PROMETHEUS_VERSION"]:
    fails.append("HUMAUX_TEST_PROMTOOL_VERSION != HUMAUX_TEST_PROMETHEUS_VERSION")
for name, svc in sorted(d.get("services", {}).items()):
    image = svc.get("image", "")
    m = re.fullmatch(r"([^@]+):([^:@/]+)@sha256:[0-9a-f]{64}", image)
    if not m:
        fails.append("%s: image is not name:tag@sha256:<digest>: %s" % (name, image))
        repo = image.split("@")[0].rsplit(":", 1)[0]
    else:
        repo, tag = m.group(1), m.group(2)
        if repo not in pins:
            fails.append("%s: image repo %s has no pin" % (name, repo))
        elif tag != pins[repo]:
            fails.append("%s: tag %s != pinned %s" % (name, tag, pins[repo]))
    cmd = svc.get("command") or []
    for arg in cmd:
        if arg.split("=")[0] in forbidden:
            fails.append("%s: forbidden flag %s" % (name, arg))
    if repo in ("prom/prometheus", "prom/alertmanager") and not any(a.startswith("--web.listen-address=127.0.0.1:") for a in cmd):
        fails.append("%s: no --web.listen-address=127.0.0.1:<port>" % name)
    if repo == "prom/alertmanager" and "--cluster.listen-address=" not in cmd:
        fails.append("%s: no empty --cluster.listen-address= (gossip would bind 0.0.0.0:9094)" % name)
for line in json.dumps(d, indent=1).splitlines():
    if "0.0.0.0" in line:
        fails.append("0.0.0.0 in rendered config: %s" % line.strip())
otel = env["OTEL_FILE"]
listeners = 0
for n, line in enumerate(open(otel).read().splitlines(), 1):
    code = line.split("#", 1)[0]
    if "0.0.0.0" in code:
        fails.append("%s:%d: 0.0.0.0: %s" % (otel, n, line.strip()))
    m = re.match(r"\s*(endpoint|host):\s*(\S+)", code)
    if m:
        listeners += 1
        if not m.group(2).strip("\"\x27").startswith("127.0.0.1"):
            fails.append("%s:%d: %s %s is not 127.0.0.1" % (otel, n, m.group(1), m.group(2)))
if listeners < 3:
    fails.append("%s: %d listener lines (endpoint:/host:), need the grpc, http and telemetry three bound to 127.0.0.1" % (otel, listeners))
am = env["AM_FILE"]
for n, line in enumerate(open(am).read().splitlines(), 1):
    code = line.split("#", 1)[0]
    if re.search(r"listen|cluster|peer|0\.0\.0\.0", code):
        fails.append("%s:%d: listener or cluster setting: %s" % (am, n, line.strip()))
for f in fails:
    print("check-compose: fail — " + f)
if fails:
    sys.exit(1)
print("check-compose: pass — %d services pinned by digest, loopback-only, no write ingress; collector %d listeners on 127.0.0.1; alertmanager.yml opens no listener" % (len(d.get("services", {})), listeners))
'
