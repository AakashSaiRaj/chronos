#!/usr/bin/env bash
#
# Validates the deployment configuration as far as it can be validated without an
# AWS account or a cluster.
#
# This is the same set of assertions the CI `manifests` job runs, so a failure here
# is a failure there. It checks the requirements that are easy to drop silently in a
# refactor and only noticed during an incident: probes, resource requests, hardened
# security contexts, and — most importantly — that no credential has leaked into a
# rendered manifest.
#
# What it cannot check: whether an IAM policy actually grants enough, whether an
# IRSA trust policy matches, or whether an add-on version conflicts. Only a real
# apply finds those.

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly RENDER_DIR="$(mktemp -d)"
trap 'rm -rf "${RENDER_DIR}"' EXIT

cd "${REPO_ROOT}"

fail=0
note() { echo "  $*"; }
bad() { echo "  FAIL: $*" >&2; fail=1; }

section() { echo; echo "── $* ──"; }

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "error: $1 is required" >&2; exit 1; }
}
require kubectl
require terraform

# ---------------------------------------------------------------------------
section "Terraform"
# ---------------------------------------------------------------------------

cd deploy/terraform
if terraform fmt -check -recursive >/dev/null 2>&1; then
  note "fmt: clean"
else
  bad "terraform fmt -recursive would change files"
fi

terraform init -backend=false -input=false >/dev/null 2>&1
if terraform validate -json | grep -q '"valid": *true'; then
  note "validate: valid"
else
  bad "terraform validate failed"
  terraform validate || true
fi
cd "${REPO_ROOT}"

# ---------------------------------------------------------------------------
section "Kustomize overlays"
# ---------------------------------------------------------------------------

overlays=()
for dir in deploy/k8s/overlays/*/; do
  overlays+=("$(basename "${dir}")")
done

for overlay in "${overlays[@]}"; do
  out="${RENDER_DIR}/${overlay}.yaml"
  if ! kubectl kustomize "deploy/k8s/overlays/${overlay}" >"${out}" 2>"${RENDER_DIR}/${overlay}.err"; then
    bad "${overlay} does not render"
    cat "${RENDER_DIR}/${overlay}.err" >&2
    continue
  fi
  note "${overlay}: renders, $(grep -c '^kind:' "${out}") objects"
done

# ---------------------------------------------------------------------------
section "No credentials in rendered output"
# ---------------------------------------------------------------------------

# A DSN with a password here would mean the Secrets Manager path had been bypassed
# and the credential committed to Git. The single most important check in the file.
if grep -rInE 'postgres://[a-zA-Z0-9_]+:[^$@/]+@' "${RENDER_DIR}"/*.yaml >/dev/null 2>&1; then
  bad "a rendered manifest contains a database credential"
  grep -rInE 'postgres://[a-zA-Z0-9_]+:[^$@/]+@' "${RENDER_DIR}"/*.yaml >&2
else
  note "no embedded DSN found"
fi

if grep -rInE '^\s*(password|secret_key|api_key):' "${RENDER_DIR}"/*.yaml >/dev/null 2>&1; then
  bad "a rendered manifest contains an inline secret value"
else
  note "no inline secret values found"
fi

# ---------------------------------------------------------------------------
section "Required workload settings"
# ---------------------------------------------------------------------------

# Each entry is "description|pattern|minimum count". The counts are per-overlay and
# reflect the three workloads plus the migration Job where relevant.
readonly checks=(
  "liveness probes|livenessProbe|3"
  "readiness probes|readinessProbe|3"
  "startup probes|startupProbe|3"
  "resource requests|requests:|4"
  "memory limits|memory:|8"
  "non-root|runAsNonRoot: true|4"
  "read-only rootfs|readOnlyRootFilesystem: true|4"
  "dropped capabilities|drop:|4"
  "seccomp|seccompProfile|4"
  "zero-downtime rollout|maxUnavailable: 0|3"
  "topology spread|topologySpreadConstraints|3"
  "file-mounted DSN|CHRONOS_DATABASE_URL_FILE|3"
  "CSI secret mount|secrets-store.csi.k8s.io|3"
  "disruption budgets|kind: PodDisruptionBudget|3"
)

for overlay in "${overlays[@]}"; do
  out="${RENDER_DIR}/${overlay}.yaml"
  [[ -f "${out}" ]] || continue
  echo "  ${overlay}:"

  for entry in "${checks[@]}"; do
    IFS='|' read -r label pattern minimum <<<"${entry}"
    actual="$(grep -c "${pattern}" "${out}" || true)"
    if [[ "${actual}" -lt "${minimum}" ]]; then
      bad "${overlay}: ${label} — found ${actual}, want at least ${minimum}"
    else
      printf '    %-26s %s\n' "${label}" "${actual}"
    fi
  done
done

# ---------------------------------------------------------------------------
section "Autoscaling"
# ---------------------------------------------------------------------------

for overlay in "${overlays[@]}"; do
  out="${RENDER_DIR}/${overlay}.yaml"
  [[ -f "${out}" ]] || continue

  if ! grep -q 'kind: HorizontalPodAutoscaler' "${out}"; then
    bad "${overlay}: no HPA"
    continue
  fi

  # The worker Deployment must not set replicas: the HPA owns that field, and
  # setting both makes Kustomize and the autoscaler fight on every apply.
  if awk '/^kind: Deployment$/,/^---$/' "${out}" \
      | awk '/name: chronos-worker$/,/^kind:/' \
      | grep -qE '^\s+replicas:'; then
    bad "${overlay}: the worker Deployment sets replicas, which conflicts with the HPA"
  else
    note "${overlay}: worker replicas left to the HPA"
  fi

  min="$(grep -A2 'kind: HorizontalPodAutoscaler' -m1 "${out}" >/dev/null; grep -m1 'minReplicas:' "${out}" | tr -dc '0-9')"
  max="$(grep -m1 'maxReplicas:' "${out}" | tr -dc '0-9')"
  note "${overlay}: HPA ${min}..${max} replicas"

  if [[ -n "${min}" && -n "${max}" && "${min}" -ge "${max}" ]]; then
    bad "${overlay}: minReplicas (${min}) must be below maxReplicas (${max})"
  fi
done

# ---------------------------------------------------------------------------
section "Workflow YAML"
# ---------------------------------------------------------------------------

if python3 -c 'import yaml' 2>/dev/null; then
  if python3 - <<'PY'
import glob, sys, yaml
ok = True
for path in sorted(glob.glob(".github/workflows/*.yml")):
    try:
        doc = yaml.safe_load(open(path))
    except Exception as exc:
        print(f"  FAIL: {path}: {exc}", file=sys.stderr); ok = False; continue
    if not doc.get("jobs"):
        print(f"  FAIL: {path}: no jobs", file=sys.stderr); ok = False
    else:
        print(f"  {path}: {len(doc['jobs'])} job(s)")
sys.exit(0 if ok else 1)
PY
  then :; else fail=1; fi
else
  note "PyYAML unavailable; skipping workflow parse"
fi

# ---------------------------------------------------------------------------
echo
if [[ "${fail}" -eq 0 ]]; then
  echo "══ deployment configuration valid ══"
  echo
  echo "Not covered by this script: a real terraform apply, a real cluster, IAM"
  echo "permissions actually being sufficient, or IRSA trust policies matching."
else
  echo "══ deployment configuration INVALID ══" >&2
fi
exit "${fail}"
