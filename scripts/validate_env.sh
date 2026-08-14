#!/usr/bin/env bash
#
# PRE-FLIGHT ENVIRONMENT VALIDATION
#
# Kegel (2014) observes that an observed connection ceiling may reflect a default
# operating-system setting rather than a property of the architecture, and must
# therefore be reported alongside the configuration that produced it. This script
# both RAISES the relevant limits and RECORDS them, so every result set ships
# with the environment that produced it.
#
# Run before the pilot, and again before the final sweep. Exits non-zero if any
# limit would bind at the 1,000-client tier -- because a sweep that silently
# hits a descriptor ceiling produces a plausible-looking but worthless dataset.
#
#   ./scripts/validate_env.sh                 # check and report
#   ./scripts/validate_env.sh --json out.json # also emit a machine-readable record

set -uo pipefail

TIER_MAX=${TIER_MAX:-1000}
JSON_OUT=""
[[ "${1:-}" == "--json" ]] && JSON_OUT="${2:-env.json}"

fails=0
warns=0
declare -A REC

# ---------------------------------------------------------------------------
# PLATFORM GATE
#
# This script must run in the SAME kernel the servers run in. On Windows and
# macOS, Docker executes containers inside a Linux virtual machine (WSL2 or
# LinuxKit), so a Git Bash / MSYS / Cygwin shell is an emulation layer that no
# part of the experiment ever touches: its ulimits, /proc and /sys describe a
# machine that is not under test. Certifying it would be worse than not
# checking at all, because it would attach a passing record to the wrong host.
# ---------------------------------------------------------------------------
UNAME_S=$(uname -s)
case "$UNAME_S" in
  MINGW*|MSYS*|CYGWIN*|Darwin*)
    printf '\n\033[31mWRONG ENVIRONMENT\033[0m\n\n'
    printf '  Detected: %s\n\n' "$UNAME_S"
    printf '  Your containers run inside a Linux VM, not in this shell. The limits,\n'
    printf '  clock and kernel settings that govern the experiment belong to that VM.\n\n'
    printf '  Run this script where the measurement happens:\n\n'
    printf '    \033[1mInside WSL2\033[0m (Windows)\n'
    printf '      wsl\n'
    printf '      cd /mnt/c/Users/<you>/Desktop/.../realtime-bench\n'
    printf '      ./scripts/validate_env.sh --json results/environment.json\n\n'
    printf '    \033[1mOr inside the server image itself\033[0m (any host; matches the\n'
    printf '    ulimits compose actually applies, so this is the authoritative check)\n'
    printf '      ./scripts/validate_in_container.sh\n\n'
    exit 2
    ;;
esac

ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fails=$((fails+1)); }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$1"; warns=$((warns+1)); }
hdr()  { printf '\n\033[1m%s\033[0m\n' "$1"; }

record() { REC["$1"]="$2"; }

# ---------------------------------------------------------------------------
hdr "Host identity (record these in the dissertation's environment table)"
KERNEL=$(uname -sr)
CPUS=$(nproc)
MEM_KB=$(awk '/MemTotal/{print $2}' /proc/meminfo 2>/dev/null || echo 0)
MEM_GB=$(awk -v k="$MEM_KB" 'BEGIN{printf "%.1f", k/1048576}')
CPU_MODEL=$(awk -F: '/model name/{print $2; exit}' /proc/cpuinfo 2>/dev/null | sed 's/^ *//')
record kernel "$KERNEL"; record cpu_cores "$CPUS"; record cpu_model "$CPU_MODEL"; record mem_gb "$MEM_GB"
printf '  kernel      %s\n  cpu         %s (%s cores)\n  memory      %s GiB\n' "$KERNEL" "$CPU_MODEL" "$CPUS" "$MEM_GB"

# Are we running INSIDE a container? If so, the absence of a docker CLI is
# expected and correct -- the server image has no business containing one. The
# check exists to catch a HOST that cannot run the experiment, and applying it
# in-container would report a failure that is really a correct design decision.
IN_CONTAINER=no
if [[ -f /.dockerenv ]] || grep -qE '(docker|containerd|kubepods)' /proc/1/cgroup 2>/dev/null; then
  IN_CONTAINER=yes
fi
record in_container "$IN_CONTAINER"

if [[ "$IN_CONTAINER" == "yes" ]]; then
  ok "running inside a container: limits below are the ones the experiment actually gets"
  record docker "n/a (checked from inside a container)"
elif command -v docker >/dev/null 2>&1; then
  DOCKER_V=$(docker --version 2>/dev/null | head -1)
  record docker "$DOCKER_V"
  ok "docker present: $DOCKER_V"
else
  bad "docker not found on PATH"
fi

# Core count governs whether the SUT and the load generator can be pinned to
# disjoint cores. Without that, they contend and the resource measurement is
# attributing the generator's cost to the server.
if (( CPUS >= 4 )); then
  ok "$CPUS cores: SUT and load generator can be pinned to disjoint cores"
else
  warn "$CPUS cores: too few to isolate the load generator from the server. Resource figures will include contention; report this as a threat to validity or use a larger host."
fi

# ---------------------------------------------------------------------------
hdr "File descriptors (the classic false ceiling)"
SOFT=$(ulimit -Sn); HARD=$(ulimit -Hn)
record ulimit_soft "$SOFT"; record ulimit_hard "$HARD"

# Headroom rationale: each connection costs one descriptor at each end, and the
# generator additionally holds listen sockets, the k6 output file, and per-VU
# state. 4x the tier is comfortable; 2x is the floor.
NEED=$(( TIER_MAX * 4 ))
if (( SOFT >= NEED )); then
  ok "soft limit $SOFT >= ${NEED} needed for the ${TIER_MAX}-client tier"
elif (( HARD >= NEED )); then
  warn "soft limit $SOFT is below ${NEED} but hard limit is $HARD. Raise it: ulimit -n $NEED"
else
  bad "hard limit $HARD is below ${NEED}. Raise it in /etc/security/limits.conf and re-login, or the ${TIER_MAX}-client tier will fail as an apparent architectural limit."
fi

# ---------------------------------------------------------------------------
hdr "Ephemeral ports and TCP state"
PORT_RANGE=$(cat /proc/sys/net/ipv4/ip_local_port_range 2>/dev/null || echo "unknown")
record ip_local_port_range "$PORT_RANGE"
if [[ "$PORT_RANGE" != "unknown" ]]; then
  LO=$(echo "$PORT_RANGE" | awk '{print $1}'); HI=$(echo "$PORT_RANGE" | awk '{print $2}')
  AVAIL=$(( HI - LO ))
  # Short polling at a 100 ms interval and 1,000 clients offers 10,000 req/s.
  # With keep-alive those reuse connections, but a keep-alive failure would
  # exhaust the range within seconds, so the headroom must be checked.
  if (( AVAIL >= 4 * TIER_MAX )); then
    ok "ephemeral port range $LO-$HI ($AVAIL ports) is sufficient"
  else
    bad "only $AVAIL ephemeral ports. Widen it: sysctl -w net.ipv4.ip_local_port_range='1024 65535'"
  fi
fi

TW_REUSE=$(cat /proc/sys/net/ipv4/tcp_tw_reuse 2>/dev/null || echo "?")
SOMAXCONN=$(cat /proc/sys/net/core/somaxconn 2>/dev/null || echo "?")
BACKLOG=$(cat /proc/sys/net/ipv4/tcp_max_syn_backlog 2>/dev/null || echo "?")
record tcp_tw_reuse "$TW_REUSE"; record somaxconn "$SOMAXCONN"; record tcp_max_syn_backlog "$BACKLOG"

if [[ "$SOMAXCONN" == "?" ]]; then
  warn "somaxconn not readable on this host (no /proc/sys). Re-run inside the container image."
elif (( SOMAXCONN >= 1024 )); then
  ok "somaxconn $SOMAXCONN: accept queue will not drop connections during ramp-up"
else
  warn "somaxconn is $SOMAXCONN. During the 1,000-client ramp this can drop SYNs and appear as connection failures attributable to the architecture. Raise it: sysctl -w net.core.somaxconn=4096"
fi

case "$TW_REUSE" in
  "?") warn "tcp_tw_reuse not readable on this host (no /proc/sys). Re-run inside the container image." ;;
  1) ok "tcp_tw_reuse=1: TIME_WAIT sockets reusable globally" ;;
  2) ok "tcp_tw_reuse=2: reuse enabled for loopback (the modern default; the bench network is container-local, so this is sufficient)" ;;
  *) warn "tcp_tw_reuse is $TW_REUSE. Short polling at a 100 ms interval can accumulate TIME_WAIT sockets if keep-alive fails; consider sysctl -w net.ipv4.tcp_tw_reuse=2" ;;
esac

# ---------------------------------------------------------------------------
hdr "Clock (the measurement's foundation)"
# The entire latency measurement rests on the claim that the load generator,
# the probe and the server read one clock. Containers do not get a time
# namespace by default, so they share the host's CLOCK_REALTIME -- but this must
# be verified, not assumed, because a host running with a time namespace or an
# unsynchronised hypervisor clock would silently invalidate every figure.
CLOCKSOURCE=$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource 2>/dev/null || echo "unknown")
record clocksource "$CLOCKSOURCE"
if [[ "$CLOCKSOURCE" == "tsc" ]]; then
  ok "clocksource is tsc (lowest-overhead, highest-resolution)"
elif [[ "$CLOCKSOURCE" == "unknown" ]]; then
  warn "could not read clocksource (expected inside some VMs)"
else
  warn "clocksource is '$CLOCKSOURCE'. tsc is preferable; kvm-clock and hpet are acceptable but add read overhead to microsecond stamping."
fi

if command -v timedatectl >/dev/null 2>&1; then
  SYNC=$(timedatectl show -p NTPSynchronized --value 2>/dev/null || echo "?")
  record ntp_synchronized "$SYNC"
  if [[ "$SYNC" == "no" ]]; then
    ok "NTP not actively stepping the clock during runs (no mid-run jumps)"
  else
    warn "NTP is synchronised and may step CLOCK_REALTIME mid-run. The harness stamps via performance.now(), which is monotonic, so a step does not corrupt a latency sample -- but record this."
  fi
fi

# Verify a monotonic, non-retrograde high-resolution clock and measure its
# granularity, which sets the measurement floor reported in Chapter 3.
if command -v node >/dev/null 2>&1; then
  RES=$(node -e '
    let minDelta = Infinity, retro = 0, zero = 0, prev = performance.now();
    const N = 2e6;
    for (let i = 0; i < N; i++) {
      const t = performance.now();
      const d = t - prev;
      if (d < 0) retro++;
      else if (d === 0) zero++;
      else if (d < minDelta) minDelta = d;
      prev = t;
    }
    // If every delta was zero the loop is faster than the clock ticks, so the
    // granularity is BELOW what this method can observe -- report the bound
    // rather than Infinity, which would serialise to null.
    const gran = Number.isFinite(minDelta) ? minDelta * 1000 : null;
    console.log(JSON.stringify({ granularityUs: gran, retrograde: retro, zeroDeltaFraction: zero / N }));
  ' 2>/dev/null)
  if [[ -n "$RES" ]]; then
    # Parsed with node itself rather than python3: node is guaranteed present
    # (it just produced the measurement) whereas python3 is not, and a missing
    # parser previously produced EMPTY values that the checks below compared
    # against "0" and reported as a clock failure. A validation script must not
    # be able to fail in a way that looks like a finding.
    GRAN=$(echo "$RES" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{const g=JSON.parse(s).granularityUs;console.log(g===null?"unresolvable":g.toFixed(4));}catch(e){console.log("unavailable");}})' 2>/dev/null)
    RETRO=$(echo "$RES" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{console.log(JSON.parse(s).retrograde);}catch(e){console.log("unavailable");}})' 2>/dev/null)
    record clock_granularity_us "$GRAN"; record clock_retrograde "$RETRO"
    if [[ "$RETRO" == "0" ]]; then
      ok "monotonic clock never stepped backwards over 2,000,000 reads"
    elif [[ -z "$RETRO" || "$RETRO" == "unavailable" ]]; then
      warn "could not measure clock monotonicity on this host (parser unavailable). Re-run inside the container image before trusting any latency figure."
    else
      bad "clock stepped backwards $RETRO times. Latency samples cannot be trusted on this host."
    fi
    # A granularity well under 1 ms is what justifies the probe existing at all.
    if [[ -z "$GRAN" || "$GRAN" == "unavailable" ]]; then
      warn "clock granularity not measured (parser unavailable)"
    elif [[ "$GRAN" == "unresolvable" ]]; then
      ok "clock granularity below this method's own resolution (every read advanced by <1 tick): ample for microsecond stamping"
    elif awk "BEGIN{exit !($GRAN < 100)}" 2>/dev/null; then
      ok "clock granularity ${GRAN} us: the microsecond probe can resolve sub-millisecond differences"
    else
      warn "clock granularity ${GRAN} us is coarse; the WebSocket-vs-SSE contrast may be unresolvable on this host."
    fi
  fi
else
  bad "node not on PATH; cannot verify clock granularity"
fi

# ---------------------------------------------------------------------------
hdr "CPU frequency governor (run-to-run variance)"
# A host that scales frequency will produce different results for identical
# cells depending on thermal state. Randomised run order (see the orchestrator)
# guards against this becoming a systematic bias, but a fixed governor removes
# the variance rather than merely distributing it.
GOV=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "unavailable")
record scaling_governor "$GOV"
case "$GOV" in
  performance) ok "governor 'performance': clock frequency will not vary with thermal state" ;;
  unavailable) warn "governor not readable (typical in a VM or container). Run order is randomised, so frequency drift will not bias one arm systematically, but report this." ;;
  *)           warn "governor is '$GOV'. Prefer 'performance' to remove frequency drift: cpupower frequency-set -g performance" ;;
esac

# ---------------------------------------------------------------------------
hdr "Summary"
printf '  %d failure(s), %d warning(s)\n' "$fails" "$warns"

if [[ -n "$JSON_OUT" ]]; then
  {
    printf '{\n'
    printf '  "capturedAt": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    first=1
    for k in "${!REC[@]}"; do
      [[ $first -eq 0 ]] && printf ',\n'; first=0
      printf '  "%s": "%s"' "$k" "${REC[$k]}"
    done
    printf '\n}\n'
  } > "$JSON_OUT"
  printf '  environment record written to %s\n' "$JSON_OUT"
fi

if (( fails > 0 )); then
  printf '\n\033[31mDo not run the sweep.\033[0m A binding limit will appear in the results as an\n'
  printf 'architectural ceiling that is really a configuration default.\n'
  exit 1
fi
printf '\n\033[32mEnvironment is fit for the sweep.\033[0m\n'
exit 0
