#!/usr/bin/env bash
# Run this script once after cloning to initialize Go module dependencies.
# Requires Go 1.22+ installed.
set -euo pipefail

AGENTS=(order_agent kitchen_agent table_agent delivery_agent)

for agent in "${AGENTS[@]}"; do
  echo "→ Initializing $agent..."
  cd "agents/$agent"
  go mod tidy
  cd ../..
done

echo "✅ All Go modules initialized."
