#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OPENMLS_DIR="$REPO_ROOT/OpenMLS_containerized"
SIGNAL_DIR="$REPO_ROOT/Signal_containerized"

OPENMLS_REGULAR="$SCRIPT_DIR/external/run_openmls.sh"
OPENMLS_REMOVE_JOIN="$SCRIPT_DIR/external/run_openmls_remove_rejoin.sh"
SIGNAL_REGULAR="$SCRIPT_DIR/external/run_signal.sh"

OUTPUT_ROOT="${OUTPUT_ROOT:-/home/diel/openmls_external_benchmark_output}"
DRY_RUN="${DRY_RUN:-0}"
BUILD_IMAGES="${BUILD_IMAGES:-0}"
BUILD_EXTERNAL_BINARIES="${BUILD_EXTERNAL_BINARIES:-0}"
CAMPAIGN_TAG="${CAMPAIGN_TAG:-$(date +%Y%m%d_%H%M%S)}"
CAMPAIGN_DIR="$OUTPUT_ROOT/external_campaign_$CAMPAIGN_TAG"
STAGES="${STAGES:-openmls,remove-rejoin,signal}"
STAGES="${STAGES//[[:space:]]/}"

PLATEAU_SIZES="${PLATEAU_SIZES:-4,64,256,512,1024}"
EXPECTED_PLATEAU_SIZES="4,64,256,512,1024"
OPENMLS_MIN_SIZE=4
OPENMLS_MAX_SIZE=1024
OPENMLS_STEP_SIZE=32
SIGNAL_MIN_SIZE=4
SIGNAL_MAX_SIZE=1024
SIGNAL_STEP_SIZE=96
UPDATE_ROUNDS=4
APP_ROUNDS=4
PROFILE_SAMPLE_CAP=4
MIN_EXTERNAL_SAMPLES_PER_OPERATION="${MIN_EXTERNAL_SAMPLES_PER_OPERATION:-10}"
OPENMLS_TARGETS="$PLATEAU_SIZES"
SIGNAL_TARGETS="$PLATEAU_SIZES"

OPENMLS_RUN="${OPENMLS_REGULAR_RUN:-}"
OPENMLS_REMOVE_JOIN_RUN="${OPENMLS_REMOVE_JOIN_RUN:-}"
SIGNAL_RUN="${SIGNAL_RUN:-}"

stage_selected() {
  case ",$STAGES," in
    *",$1,"*) return 0 ;;
    *) return 1 ;;
  esac
}

validate_stage_list() {
  local stage
  local -a requested
  IFS=',' read -r -a requested <<< "$STAGES"
  [ "${#requested[@]}" -gt 0 ] || {
    echo "ERROR: STAGES must contain at least one stage" >&2
    return 1
  }
  for stage in "${requested[@]}"; do
    case "$stage" in
      openmls|remove-rejoin|signal) ;;
      *)
        echo "ERROR: unknown stage '$stage'; use openmls,remove-rejoin,signal" >&2
        return 1
        ;;
    esac
  done
}

latest_run() {
  local prefix="$1"
  local candidate latest=""
  shopt -s nullglob
  for candidate in "$OUTPUT_ROOT"/"$prefix"*; do
    [ -d "$candidate" ] || continue
    if [ -z "$latest" ] || [ "$candidate" -nt "$latest" ]; then
      latest="$candidate"
    fi
  done
  shopt -u nullglob
  [ -n "$latest" ] || return 1
  printf '%s\n' "$latest"
}

run_command() {
  if [ "$DRY_RUN" = "1" ]; then
    printf 'DRY-RUN:'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

strict_openmls_validation() {
  local run_dir="$1"
  local mode="$2"
  local coverage_json="$run_dir/external_device_coverage.json"
  local -a validator_args

  validator_args=()
  if [ "$mode" = "regular" ]; then
    validator_args=(--require-k-values 1,8)
  fi
  uv run --frozen --project "$REPO_ROOT" python "$OPENMLS_DIR/scripts/validate_benchmark_outputs.py" \
    "${validator_args[@]}" "$run_dir"
  test -s "$coverage_json" || {
    echo "ERROR: missing external-device coverage report: $coverage_json" >&2
    return 1
  }

  uv run --frozen --project "$REPO_ROOT" python - "$coverage_json" <<'PY'
import json
import sys

report = json.load(open(sys.argv[1], encoding="utf-8"))
if not report.get("success"):
    raise SystemExit("external_device_coverage.json reports failure")
for device in report.get("devices", []):
    if device.get("status") == "complete":
        print(device["worker_id"])
    elif not (device.get("worker_id") == "pico-plus-00001" and device.get("status") == "attrited"):
        raise SystemExit(f"unexpected external-device status: {device}")
PY
}

strict_signal_validation() {
  local run_dir="$1"
  uv run --frozen --project "$REPO_ROOT" python "$SIGNAL_DIR/scripts/validate_external_device_coverage.py" \
    "$run_dir/events.csv" \
    --layout "$run_dir/worker_layout.json" \
    --runner-events "$run_dir/runner-events.jsonl" \
    --external-worker-id pico-plus-00001 \
    --external-worker-id raspi5-00001 \
    --external-worker-id raspi3bp-00001 \
    --luckfox-id pico-plus-00001 \
    --allow-luckfox-attrition \
    --min-size "$SIGNAL_MIN_SIZE" \
    --max-size "$SIGNAL_MAX_SIZE" \
    --step-size "$SIGNAL_STEP_SIZE" \
    --switch-at "$SIGNAL_MAX_SIZE" \
    --step-after-switch "$SIGNAL_STEP_SIZE" \
    --plateau-sizes "$SIGNAL_TARGETS" \
    --payload-sizes 512 \
    --expected-profiled-docker 0 \
    --minimum-observations "$MIN_EXTERNAL_SAMPLES_PER_OPERATION"
}

require_reused_run() {
  local stage="$1"
  local run_dir="$2"
  [ -n "$run_dir" ] && [ -d "$run_dir" ] || {
    echo "ERROR: skipped stage '$stage' requires its completed run path" >&2
    return 1
  }
}

write_campaign_manifest() {
  mkdir -p "$CAMPAIGN_DIR/openmls"
  ln -sfn "$OPENMLS_RUN" "$CAMPAIGN_DIR/openmls/regular"
  ln -sfn "$OPENMLS_REMOVE_JOIN_RUN" "$CAMPAIGN_DIR/openmls/remove-rejoin"
  ln -sfn "$SIGNAL_RUN" "$CAMPAIGN_DIR/signal"

  uv run --frozen --project "$REPO_ROOT" python - \
    "$CAMPAIGN_DIR" "$OPENMLS_RUN" "$OPENMLS_REMOVE_JOIN_RUN" "$SIGNAL_RUN" \
    "$OPENMLS_TARGETS" "$MIN_EXTERNAL_SAMPLES_PER_OPERATION" <<'PY'
import json
import pathlib
import sys

campaign_dir = pathlib.Path(sys.argv[1])
openmls_regular = pathlib.Path(sys.argv[2]).resolve()
openmls_remove_rejoin = pathlib.Path(sys.argv[3]).resolve()
signal = pathlib.Path(sys.argv[4]).resolve()
plateaus = [int(value) for value in sys.argv[5].split(",")]
minimum = int(sys.argv[6])

manifest = {
    "schema_version": 1,
    "plateau_order": "ascending",
    "plateau_sizes": plateaus,
    "minimum_observations_per_device_operation_plateau": minimum,
    "update_rounds": 4,
    "application_rounds": 4,
    "runs": {
        "openmls_regular": str(openmls_regular),
        "openmls_remove_rejoin": str(openmls_remove_rejoin),
        "signal": str(signal),
    },
}
(campaign_dir / "campaign_manifest.json").write_text(
    json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
)


PY
  ln -sfn "$CAMPAIGN_DIR" "$OUTPUT_ROOT/latest_external_campaign"
}

validate_stage_list
[ "$PLATEAU_SIZES" = "$EXPECTED_PLATEAU_SIZES" ] || {
  echo "ERROR: production plateaus must be exactly $EXPECTED_PLATEAU_SIZES" >&2
  exit 2
}
[ "$MIN_EXTERNAL_SAMPLES_PER_OPERATION" -ge 10 ] || {
  echo "ERROR: the external observation floor must be at least 10" >&2
  exit 2
}

mkdir -p "$OUTPUT_ROOT"
for required_script in "$OPENMLS_REGULAR" "$OPENMLS_REMOVE_JOIN" "$SIGNAL_REGULAR"; do
  test -x "$required_script" || {
    echo "ERROR: required executable script is missing: $required_script" >&2
    exit 2
  }
done

if ! stage_selected openmls; then
  require_reused_run openmls "$OPENMLS_RUN"
fi
if ! stage_selected remove-rejoin; then
  require_reused_run remove-rejoin "$OPENMLS_REMOVE_JOIN_RUN"
fi
if ! stage_selected signal; then
  require_reused_run signal "$SIGNAL_RUN"
fi

echo "===== External-device 1024 ascending campaign ====="
echo "  stages=$STAGES"
echo "  output_root=$OUTPUT_ROOT"
echo "  free_space=$(df -h "$OUTPUT_ROOT" | awk 'NR==2 {print $4" available on "$1}')"
echo "  exact targets=$PLATEAU_SIZES"
echo "  fallback steps: OpenMLS=$OPENMLS_STEP_SIZE Signal=$SIGNAL_STEP_SIZE"
echo "  rounds: update=$UPDATE_ROUNDS application=$APP_ROUNDS cap=$PROFILE_SAMPLE_CAP"
echo "  required valid samples per external device/operation/plateau=$MIN_EXTERNAL_SAMPLES_PER_OPERATION"
echo "  rebuild images=$BUILD_IMAGES external binaries=$BUILD_EXTERNAL_BINARIES"

if stage_selected openmls; then
  previous="$(latest_run cal05_ext_unconstrained_openmls_i1_ 2>/dev/null || true)"
  run_command env \
    OUTPUT_ROOT="$OUTPUT_ROOT" N=1 WORKERS=1024 \
    MIN_SIZE="$OPENMLS_MIN_SIZE" MAX_SIZE="$OPENMLS_MAX_SIZE" STEP_SIZE="$OPENMLS_STEP_SIZE" \
    PLATEAU_SIZES="$OPENMLS_TARGETS" \
    PLATEAU_ORDER=ascending ROUNDTRIPS=1 \
    UPDATE_ROUNDS="$UPDATE_ROUNDS" APP_ROUNDS="$APP_ROUNDS" \
    MAX_UPDATE_SAMPLES_PER_PLATEAU="$PROFILE_SAMPLE_CAP" \
    MAX_APP_SAMPLES_PER_PAYLOAD="$PROFILE_SAMPLE_CAP" PAYLOAD_SIZES=512 \
    MIN_EXTERNAL_SAMPLES_PER_OPERATION="$MIN_EXTERNAL_SAMPLES_PER_OPERATION" \
    BUILD_IMAGES="$BUILD_IMAGES" BUILD_EXTERNAL_BINARIES="$BUILD_EXTERNAL_BINARIES" \
    SCENARIO=external-device-openmls-1024-ascending-five-plateau \
    bash "$OPENMLS_REGULAR"
  if [ "$DRY_RUN" != "1" ]; then
    OPENMLS_RUN="$(latest_run cal05_ext_unconstrained_openmls_i1_)"
    [ "$OPENMLS_RUN" != "$previous" ] || {
      echo "ERROR: OpenMLS regular stage did not create a new run directory" >&2
      exit 1
    }
    strict_openmls_validation "$OPENMLS_RUN" regular
  fi
else
  strict_openmls_validation "$OPENMLS_RUN" regular
fi

if stage_selected remove-rejoin; then
  previous="$(latest_run cal06_ext_remove_rejoin_openmls_i1_ 2>/dev/null || true)"
  if stage_selected openmls; then
    remove_build_images=0
    remove_build_external=0
  else
    remove_build_images="$BUILD_IMAGES"
    remove_build_external="$BUILD_EXTERNAL_BINARIES"
  fi
  run_command env \
    OUTPUT_ROOT="$OUTPUT_ROOT" N=1 WORKERS=1024 \
    MIN_SIZE="$OPENMLS_MIN_SIZE" MAX_SIZE="$OPENMLS_MAX_SIZE" STEP_SIZE="$OPENMLS_STEP_SIZE" \
    PLATEAU_SIZES="$OPENMLS_TARGETS" \
    PLATEAU_ORDER=ascending ROUNDTRIPS=1 \
    UPDATE_ROUNDS="$UPDATE_ROUNDS" APP_ROUNDS="$APP_ROUNDS" \
    MAX_UPDATE_SAMPLES_PER_PLATEAU="$PROFILE_SAMPLE_CAP" \
    MAX_APP_SAMPLES_PER_PAYLOAD="$PROFILE_SAMPLE_CAP" PAYLOAD_SIZES=512 \
    MIN_EXTERNAL_SAMPLES_PER_OPERATION="$MIN_EXTERNAL_SAMPLES_PER_OPERATION" \
    BUILD_IMAGES="$remove_build_images" BUILD_EXTERNAL_BINARIES="$remove_build_external" \
    SCENARIO=external-device-openmls-remove-join-1024-ascending-five-plateau \
    bash "$OPENMLS_REMOVE_JOIN"
  if [ "$DRY_RUN" != "1" ]; then
    OPENMLS_REMOVE_JOIN_RUN="$(latest_run cal06_ext_remove_rejoin_openmls_i1_)"
    [ "$OPENMLS_REMOVE_JOIN_RUN" != "$previous" ] || {
      echo "ERROR: OpenMLS Remove/Join stage did not create a new run directory" >&2
      exit 1
    }
    strict_openmls_validation "$OPENMLS_REMOVE_JOIN_RUN" remove-rejoin
  fi
else
  strict_openmls_validation "$OPENMLS_REMOVE_JOIN_RUN" remove-rejoin
fi

if stage_selected signal; then
  previous="$(latest_run cal07_ext_signal_pairwise_i1_ 2>/dev/null || true)"
  run_command env \
    OUTPUT_ROOT="$OUTPUT_ROOT" N=1 WORKERS=1024 \
    MIN_SIZE="$SIGNAL_MIN_SIZE" MAX_SIZE="$SIGNAL_MAX_SIZE" STEP_SIZE="$SIGNAL_STEP_SIZE" \
    PLATEAU_SIZES="$SIGNAL_TARGETS" \
    STEP_SIZE_SWITCH_AT="$SIGNAL_MAX_SIZE" STEP_SIZE_AFTER_SWITCH="$SIGNAL_STEP_SIZE" \
    PLATEAU_ORDER=ascending ROUNDTRIPS=1 \
    APP_ROUNDS="$APP_ROUNDS" \
    MAX_APP_SAMPLES_PER_PAYLOAD="$PROFILE_SAMPLE_CAP" PAYLOAD_SIZES=512 \
    MIN_EXTERNAL_SAMPLES_PER_OPERATION="$MIN_EXTERNAL_SAMPLES_PER_OPERATION" \
    BUILD_IMAGES="$BUILD_IMAGES" BUILD_EXTERNAL_BINARIES="$BUILD_EXTERNAL_BINARIES" \
    SCENARIO=external-device-signal-1024-ascending-five-plateau \
    bash "$SIGNAL_REGULAR"
  if [ "$DRY_RUN" != "1" ]; then
    SIGNAL_RUN="$(latest_run cal07_ext_signal_pairwise_i1_)"
    [ "$SIGNAL_RUN" != "$previous" ] || {
      echo "ERROR: Signal stage did not create a new run directory" >&2
      exit 1
    }
    strict_signal_validation "$SIGNAL_RUN"
  fi
else
  strict_signal_validation "$SIGNAL_RUN"
fi

echo ""
if [ "$DRY_RUN" = "1" ]; then
  echo "Dry run complete; no benchmark was started."
  exit 0
fi

write_campaign_manifest

echo "External-device campaign completed with strict coverage checks."
echo "  OpenMLS: $OPENMLS_RUN"
echo "  OpenMLS Remove/Join: $OPENMLS_REMOVE_JOIN_RUN"
echo "  Signal: $SIGNAL_RUN"
