#!/usr/bin/env bash
# bootstrap.sh — provision the Confluent Cloud resources the dbt-confluent test
# suite needs and write a ready-to-use test.env.
#
# Usage: ./bootstrap.sh [--env-id ENV_ID] [--org-id ORG_ID]
#                       [--kafka-cluster-id lkc-xxxxx] [--output FILE]
#                       [--second-compute-pool] [--force]
#
# Requires: confluent CLI, jq, curl
#
# API keys created (the scopes matter — see "API keys" below):
#   flink      -> CONFLUENT_FLINK_API_KEY   (all Flink SQL statements)
#   global     -> CONFLUENT_GLOBAL_API_KEY  (Tableflow control plane + CMK lookup)
#   <lkc-...>  -> CONFLUENT_KAFKA_API_KEY   (extras: AdminClient/producer, and
#                 CONFLUENT_SASL_JAAS_CONFIG for the streaming_source connector)
#   <lsrc-...> -> CONFLUENT_SR_API_KEY      (extras: Schema Registry)
set -euo pipefail

# ── Prerequisites ─────────────────────────────────────────────────────────────
for cmd in confluent jq curl; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "ERROR: '$cmd' not found on PATH"; exit 1; }
done

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_CLUSTER_NAME="${KAFKA_CLUSTER_NAME:-test_kafka_cluster}"
CLOUD_PROVIDER="${CLOUD_PROVIDER:-aws}"
CLOUD_REGION="${CLOUD_REGION:-us-east-1}"
COMPUTE_POOL_NAME="${COMPUTE_POOL_NAME:-test_compute_pool}"
COMPUTE_POOL_MAX_CFU="${COMPUTE_POOL_MAX_CFU:-10}"
# Tableflow is not available on Basic clusters, and the Tableflow functional
# tests enable it on the test cluster — so a fresh cluster must be Standard+.
KAFKA_CLUSTER_TYPE="${KAFKA_CLUSTER_TYPE:-standard}"
OUTPUT_FILE="test.env"
FORCE=0
SECOND_POOL=0

# ── Parse optional flags ──────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-id)             ENV_ID="$2";               shift 2 ;;
    --org-id)             ORG_ID="$2";               shift 2 ;;
    --kafka-cluster-id)   KAFKA_CLUSTER_ID_ARG="$2"; shift 2 ;;
    --output)             OUTPUT_FILE="$2";          shift 2 ;;
    --second-compute-pool) SECOND_POOL=1;            shift ;;
    --force)              FORCE=1;                   shift ;;
    -h|--help)            sed -n '2,17p' "$0"; exit 0 ;;
    *) echo "Unknown flag: $1"; exit 1 ;;
  esac
done

if [[ -e "$OUTPUT_FILE" && $FORCE -eq 0 ]]; then
  echo "ERROR: $OUTPUT_FILE already exists. Re-run with --force to overwrite" >&2
  echo "       (the current file is backed up to $OUTPUT_FILE.bak)." >&2
  exit 1
fi

# ── Login ─────────────────────────────────────────────────────────────────────
if ! confluent organization list -o json >/dev/null 2>&1; then
  echo "Not logged in — running 'confluent login --save'..." >&2
  confluent login --save
fi

# ── Org ───────────────────────────────────────────────────────────────────────
ORG_ID="${ORG_ID:-$(confluent organization list -o json | jq -r '.[0].id')}"
echo "# Using org: $ORG_ID" >&2

# ── Environment ───────────────────────────────────────────────────────────────
if [[ -z "${ENV_ID:-}" ]]; then
  FIRST_ENV=$(confluent environment list -o json | jq -r 'first(.[] | select(.id)) | .id // empty')
  if [[ -n "$FIRST_ENV" ]]; then
    echo "Reusing existing environment ($FIRST_ENV)" >&2
    ENV_ID="$FIRST_ENV"
  else
    echo "Creating environment 'flink-dbt-eval'..." >&2
    ENV_ID=$(confluent environment create "flink-dbt-eval" -o json | jq -r '.id')
  fi
fi
confluent environment use "$ENV_ID" >/dev/null
ENV_NAME=$(confluent environment list -o json | jq -r --arg id "$ENV_ID" '.[] | select(.id == $id) | .name')
echo "# Using environment: $ENV_ID ($ENV_NAME)" >&2

# ── Kafka cluster ─────────────────────────────────────────────────────────────
if [[ -n "${KAFKA_CLUSTER_ID_ARG:-}" ]]; then
  echo "Using specified Kafka cluster ($KAFKA_CLUSTER_ID_ARG)" >&2
  KAFKA_JSON=$(confluent kafka cluster describe "$KAFKA_CLUSTER_ID_ARG" -o json)
else
  FIRST_KAFKA=$(confluent kafka cluster list -o json | jq -r 'first(.[] | select(.id)) | .id // empty')
  if [[ -n "$FIRST_KAFKA" ]]; then
    echo "Reusing existing Kafka cluster ($FIRST_KAFKA)" >&2
    KAFKA_JSON=$(confluent kafka cluster describe "$FIRST_KAFKA" -o json)
  else
    echo "Creating $KAFKA_CLUSTER_TYPE Kafka cluster '$KAFKA_CLUSTER_NAME'..." >&2
    KAFKA_JSON=$(confluent kafka cluster create "$KAFKA_CLUSTER_NAME" \
      --cloud "$CLOUD_PROVIDER" \
      --region "$CLOUD_REGION" \
      --type "$KAFKA_CLUSTER_TYPE" \
      -o json)
    echo "Waiting for Kafka cluster to be ready..." >&2
    confluent kafka cluster describe "$(echo "$KAFKA_JSON" | jq -r '.id')" --wait >/dev/null
    KAFKA_JSON=$(confluent kafka cluster describe "$(echo "$KAFKA_JSON" | jq -r '.id')" -o json)
  fi
fi

KAFKA_CLUSTER_ID=$(echo "$KAFKA_JSON" | jq -r '.id')
# The Kafka cluster's *display name* is what dbt calls the schema/database, and
# what the `unique_schema` fixture reads out of CONFLUENT_TEST_DBNAME.
KAFKA_CLUSTER_NAME=$(echo "$KAFKA_JSON" | jq -r '.name')
KAFKA_CLUSTER_TYPE_ACTUAL=$(echo "$KAFKA_JSON" | jq -r '.type')
BOOTSTRAP_SERVERS=$(echo "$KAFKA_JSON" | jq -r '.endpoint' | sed 's|SASL_SSL://||')

if [[ "$(echo "$KAFKA_CLUSTER_TYPE_ACTUAL" | tr '[:upper:]' '[:lower:]')" == "basic" ]]; then
  echo "WARNING: cluster $KAFKA_CLUSTER_ID is Basic; Tableflow is not supported there," >&2
  echo "         so tests/functional/adapter/test_tableflow.py will fail." >&2
fi

# ── API keys ──────────────────────────────────────────────────────────────────
# Scopes are NOT interchangeable — each of these routes is served by a different
# backend that only accepts its own key type:
#   * Flink SQL REST      accepts only a Flink *region* key (a cloud key 404s).
#   * /tableflow/v1       accepts only a *tableflow* key (a cloud key 401s).
#   * /cmk/v2 (cluster-id lookup) accepts only a cloud/global key — which is why
#     the control-plane key below is created with --resource global: the adapter
#     derives the lkc- id from the cluster name through that route, and a
#     tableflow-scoped key gets a 401 from it.
# Keys are created for the logged-in user principal, which inherits its roles.
# Pass --service-account to `confluent api-key create` instead if you want a
# dedicated principal, and grant it the matching role bindings yourself.

echo "Creating Flink region API key ($CLOUD_PROVIDER/$CLOUD_REGION)..." >&2
FLINK_KEY_JSON=$(confluent api-key create \
  --resource flink \
  --cloud "$CLOUD_PROVIDER" \
  --region "$CLOUD_REGION" \
  --environment "$ENV_ID" \
  --description "dbt-confluent tests: flink region key" \
  -o json)
CONFLUENT_FLINK_API_KEY=$(echo "$FLINK_KEY_JSON" | jq -r '.api_key')
CONFLUENT_FLINK_API_SECRET=$(echo "$FLINK_KEY_JSON" | jq -r '.api_secret')

echo "Creating Tableflow control-plane API key..." >&2
TABLEFLOW_KEY_JSON=$(confluent api-key create \
  --resource global \
  --description "dbt-confluent tests: tableflow control-plane key" \
  -o json)
CONFLUENT_GLOBAL_API_KEY=$(echo "$TABLEFLOW_KEY_JSON" | jq -r '.api_key')
CONFLUENT_GLOBAL_API_SECRET=$(echo "$TABLEFLOW_KEY_JSON" | jq -r '.api_secret')

echo "Creating Kafka cluster API key..." >&2
KAFKA_KEY_JSON=$(confluent api-key create \
  --resource "$KAFKA_CLUSTER_ID" \
  --description "dbt-confluent tests: kafka cluster key" \
  -o json)
CONFLUENT_KAFKA_API_KEY=$(echo "$KAFKA_KEY_JSON" | jq -r '.api_key')
CONFLUENT_KAFKA_API_SECRET=$(echo "$KAFKA_KEY_JSON" | jq -r '.api_secret')

# The streaming_source connector authenticates to the same cluster with the same
# scope, so it reuses the key above rather than creating a second identical one.
CONFLUENT_SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${CONFLUENT_KAFKA_API_KEY}\" password=\"${CONFLUENT_KAFKA_API_SECRET}\";"

# ── Schema Registry ───────────────────────────────────────────────────────────
echo "Fetching Schema Registry details..." >&2
SR_JSON=$(confluent schema-registry cluster describe -o json)
CONFLUENT_SR_URL=$(echo "$SR_JSON" | jq -r '.endpoint_url')

echo "Creating Schema Registry API key..." >&2
SR_KEY_JSON=$(confluent api-key create \
  --resource "$(echo "$SR_JSON" | jq -r '.cluster')" \
  --description "dbt-confluent tests: schema registry key" \
  -o json)
CONFLUENT_SR_API_KEY=$(echo "$SR_KEY_JSON" | jq -r '.api_key')
CONFLUENT_SR_API_SECRET=$(echo "$SR_KEY_JSON" | jq -r '.api_secret')

# ── Flink compute pool(s) ─────────────────────────────────────────────────────
EXISTING_POOLS=$(confluent flink compute-pool list -o json | jq -r '.[] | select(.id) | .id')
POOL_1=$(echo "$EXISTING_POOLS" | sed -n 1p)
POOL_2=$(echo "$EXISTING_POOLS" | sed -n 2p)

if [[ -n "$POOL_1" ]]; then
  CONFLUENT_COMPUTE_POOL_ID="$POOL_1"
  echo "Reusing existing Flink compute pool ($CONFLUENT_COMPUTE_POOL_ID)" >&2
else
  echo "Creating Flink compute pool '$COMPUTE_POOL_NAME'..." >&2
  CONFLUENT_COMPUTE_POOL_ID=$(confluent flink compute-pool create "$COMPUTE_POOL_NAME" \
    --cloud "$CLOUD_PROVIDER" \
    --region "$CLOUD_REGION" \
    --max-cfu "$COMPUTE_POOL_MAX_CFU" \
    -o json | jq -r '.id')
fi

# The per-model compute pool test needs a second pool in the same env+region.
# It skips when CONFLUENT_COMPUTE_POOL_ID_2 is unset, so this stays opt-in.
CONFLUENT_COMPUTE_POOL_ID_2="$POOL_2"
if [[ -z "$CONFLUENT_COMPUTE_POOL_ID_2" && $SECOND_POOL -eq 1 ]]; then
  echo "Creating second Flink compute pool '${COMPUTE_POOL_NAME}_2'..." >&2
  CONFLUENT_COMPUTE_POOL_ID_2=$(confluent flink compute-pool create "${COMPUTE_POOL_NAME}_2" \
    --cloud "$CLOUD_PROVIDER" \
    --region "$CLOUD_REGION" \
    --max-cfu "$COMPUTE_POOL_MAX_CFU" \
    -o json | jq -r '.id')
fi

# ── Verify the two keys the test suite authenticates with ─────────────────────
# Freshly created keys take a few seconds to propagate, hence the retries.
verify() {
  local label="$1" url="$2" key="$3" secret="$4" code=""
  for _ in 1 2 3 4 5 6; do
    code=$(curl -sS -o /dev/null -w '%{http_code}' -u "$key:$secret" "$url" || echo "000")
    [[ "$code" == "200" ]] && { echo "  OK   $label ($key)" >&2; return 0; }
    sleep 5
  done
  echo "  FAIL $label ($key) -> HTTP $code" >&2
  return 1
}

echo "Verifying API keys..." >&2
VERIFY_OK=1
verify "Flink SQL    " \
  "https://flink.${CLOUD_REGION}.${CLOUD_PROVIDER}.confluent.cloud/sql/v1/organizations/${ORG_ID}/environments/${ENV_ID}/statements?page_size=1" \
  "$CONFLUENT_FLINK_API_KEY" "$CONFLUENT_FLINK_API_SECRET" || VERIFY_OK=0
verify "Tableflow    " \
  "https://api.confluent.cloud/tableflow/v1/tableflow-topics?environment=${ENV_ID}&spec.kafka_cluster=${KAFKA_CLUSTER_ID}" \
  "$CONFLUENT_GLOBAL_API_KEY" "$CONFLUENT_GLOBAL_API_SECRET" || VERIFY_OK=0
# Same key, second route: Tableflow also needs the lkc- id resolved from the
# cluster display name through CMK. A tableflow- or flink-scoped key 401s here,
# so this probe is what distinguishes a correct `global` key from a near miss.
verify "CMK lookup   " \
  "https://api.confluent.cloud/cmk/v2/clusters?environment=${ENV_ID}" \
  "$CONFLUENT_GLOBAL_API_KEY" "$CONFLUENT_GLOBAL_API_SECRET" || VERIFY_OK=0

# ── Write test.env ────────────────────────────────────────────────────────────
[[ -e "$OUTPUT_FILE" ]] && cp "$OUTPUT_FILE" "$OUTPUT_FILE.bak"
umask 077
cat > "$OUTPUT_FILE" <<EOF
# Never commit test.env
# Generated by bootstrap.sh for org $ORG_ID, environment $ENV_ID ($ENV_NAME).

export CONFLUENT_ENV_ID=${ENV_ID}
export CONFLUENT_ORG_ID=${ORG_ID}
export CONFLUENT_COMPUTE_POOL_ID=${CONFLUENT_COMPUTE_POOL_ID}
export CONFLUENT_CLOUD_PROVIDER=${CLOUD_PROVIDER}
export CONFLUENT_CLOUD_REGION=${CLOUD_REGION}
export CONFLUENT_TEST_DBNAME=${KAFKA_CLUSTER_NAME}

# Flink *region* key — used for every Flink SQL statement. Required: a
# cloud-scoped key is rejected by the Flink SQL REST API.
export CONFLUENT_FLINK_API_KEY=${CONFLUENT_FLINK_API_KEY}
export CONFLUENT_FLINK_API_SECRET=${CONFLUENT_FLINK_API_SECRET}

# Optional: a second compute pool (same environment + region, different from
# CONFLUENT_COMPUTE_POOL_ID) used only by the per-model compute pool test.
# The test is skipped when this is unset or equal to CONFLUENT_COMPUTE_POOL_ID.
$(if [[ -n "$CONFLUENT_COMPUTE_POOL_ID_2" ]]; then
    echo "export CONFLUENT_COMPUTE_POOL_ID_2=${CONFLUENT_COMPUTE_POOL_ID_2}"
  else
    echo "# export CONFLUENT_COMPUTE_POOL_ID_2=lfcp-yyyyy  # re-run with --second-compute-pool"
  fi)

# Tableflow control-plane key (resource_type=global), used only by the Tableflow
# functional tests -- Tableflow's control-plane routes reject the Flink-region
# pair above, and the cluster-id lookup they need rejects tableflow- and
# cloud-scoped keys. Those tests are skipped when either of these is unset.
# NOTE: this pair is passed to confluent_sql as tableflow_api_key (NOT as
# global_api_key), so it is only used for Tableflow control-plane routes and
# does NOT override flink_api_key for Flink SQL operations.
export CONFLUENT_GLOBAL_API_KEY=${CONFLUENT_GLOBAL_API_KEY}
export CONFLUENT_GLOBAL_API_SECRET=${CONFLUENT_GLOBAL_API_SECRET}

# ── Not read by the test suite; handy for ad-hoc models and producers ────────
export CONFLUENT_KAFKA_CLUSTER_ID=${KAFKA_CLUSTER_ID}
export CONFLUENT_KAFKA_CLUSTER_NAME=${KAFKA_CLUSTER_NAME}
export CONFLUENT_BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS}
export CONFLUENT_KAFKA_API_KEY=${CONFLUENT_KAFKA_API_KEY}
export CONFLUENT_KAFKA_API_SECRET=${CONFLUENT_KAFKA_API_SECRET}
export CONFLUENT_SR_URL=${CONFLUENT_SR_URL}
export CONFLUENT_SR_API_KEY=${CONFLUENT_SR_API_KEY}
export CONFLUENT_SR_API_SECRET=${CONFLUENT_SR_API_SECRET}

# JAAS config string for the Kafka connector WITH options in a streaming_source
# model. Same credential as CONFLUENT_KAFKA_API_KEY above, just in JAAS form.
export CONFLUENT_SASL_JAAS_CONFIG="${CONFLUENT_SASL_JAAS_CONFIG}"
EOF
chmod 600 "$OUTPUT_FILE"

echo >&2
echo "Wrote $OUTPUT_FILE (mode 600). Run the suite with: uv run pytest" >&2
[[ $VERIFY_OK -eq 1 ]] || {
  echo "WARNING: at least one key failed verification — see FAIL lines above." >&2
  exit 1
}
