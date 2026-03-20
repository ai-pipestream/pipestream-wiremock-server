#!/usr/bin/env bash
# Smoke-test WireMock unary gRPC after: ./gradlew quarkusRun (ports 8080 + 50052).
# Requires grpcurl and a clone of pipestream-protos (adjust PROTO_ROOT).
set -euo pipefail

HOST="${1:-localhost:8080}"
PROTO_ROOT="${PROTO_ROOT:-$(cd "$(dirname "$0")/../../pipestream-protos" 2>/dev/null && pwd || true)}"
if [[ ! -d "$PROTO_ROOT/repo/proto" ]]; then
  echo "Set PROTO_ROOT to your pipestream-protos checkout (parent of repo/proto)." >&2
  exit 1
fi

COMMON="-import-path $PROTO_ROOT/common/proto -import-path $PROTO_ROOT/repo/proto"
PIPEDOC="-proto ai/pipestream/repository/pipedoc/v1/pipedoc_service.proto"

echo "=== PipeDocService.DeletePipeDoc (default idempotent stub) ==="
grpcurl -plaintext $COMMON $PIPEDOC \
  -d '{"logicalDocument":{"docId":"smoke","accountId":"acct","datasourceId":"ds"}}' \
  "$HOST" ai.pipestream.repository.pipedoc.v1.PipeDocService/DeletePipeDoc

echo "=== PipeDocService.GetPipeDocByReference (default test-doc-1) ==="
grpcurl -plaintext $COMMON $PIPEDOC \
  -d '{"documentRef":{"docId":"test-doc-1","accountId":"test-account"}}' \
  "$HOST" ai.pipestream.repository.pipedoc.v1.PipeDocService/GetPipeDocByReference

echo "=== OpenSearchManagerService.ListIndices ==="
grpcurl -plaintext \
  -import-path "$PROTO_ROOT/common/proto" \
  -import-path "$PROTO_ROOT/opensearch/proto" \
  -import-path "$PROTO_ROOT/schemamanager/proto" \
  -proto ai/pipestream/opensearch/v1/opensearch_manager.proto \
  -d '{}' \
  "$HOST" ai.pipestream.opensearch.v1.OpenSearchManagerService/ListIndices

echo "OK"
