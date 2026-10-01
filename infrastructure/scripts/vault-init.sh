#!/usr/bin/env bash
# vault-init.sh
# Initializes HashiCorp Vault dev mode transit engine for envelope encryption
set -euo pipefail

VAULT_ADDR=${VAULT_ADDR:-"http://127.0.0.1:8200"}
VAULT_TOKEN=${VAULT_TOKEN:-"root"}

export VAULT_ADDR
export VAULT_TOKEN

echo "Connecting to Vault at $VAULT_ADDR..."

# 1. Enable transit secrets engine if not already enabled
if ! vault secrets list | grep -q "^transit/"; then
    echo "Enabling transit secrets engine..."
    vault secrets enable transit
else
    echo "Transit secrets engine already enabled."
fi

# 2. Create master KEK (Key Encryption Key) for workspaces
echo "Configuring workspace master KEK: workspace-kek..."
vault write -f transit/keys/workspace-kek type=aes256-gcm96 derived=false exportable=false

# 3. Test generate DEK
echo "Testing DEK generation..."
DEK_RESP=$(vault write -format=json -f transit/datakey/plaintext/workspace-kek)
CIPHERTEXT=$(echo "$DEK_RESP" | grep -o '"ciphertext": "[^"]*' | cut -d'"' -f4)

echo "Vault Transit KEK setup completed successfully."
echo "Sample Wrapped DEK: $CIPHERTEXT"
