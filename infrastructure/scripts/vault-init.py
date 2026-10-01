#!/usr/bin/env python3
"""
vault-init.py
Cross-platform Python script using Vault's HTTP REST API to configure
the Transit secrets engine and KEK/DEK keys.
"""

import os
import sys
import json
import urllib.request
import urllib.error

VAULT_ADDR = os.getenv("VAULT_ADDR", "http://127.0.0.1:8200").rstrip("/")
VAULT_TOKEN = os.getenv("VAULT_TOKEN", "root")

def vault_request(path: str, method: str = "GET", data: dict = None) -> dict:
    url = f"{VAULT_ADDR}/v1/{path.lstrip('/')}"
    headers = {
        "X-Vault-Token": VAULT_TOKEN,
        "Content-Type": "application/json",
    }
    body = json.dumps(data).encode("utf-8") if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            content = resp.read()
            if content:
                return json.loads(content.decode("utf-8"))
            return {}
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8")
        raise RuntimeError(f"Vault HTTP {e.code} error on {path}: {err_msg}") from e

def main():
    print(f"Connecting to Vault at {VAULT_ADDR}...")

    # 1. Check or enable transit secret engine
    try:
        mounts = vault_request("sys/mounts")
        if "transit/" not in mounts.get("data", {}):
            print("Mounting transit secret engine...")
            vault_request("sys/mounts/transit", method="POST", data={"type": "transit"})
        else:
            print("Transit secret engine is already mounted.")
    except Exception as e:
        print(f"Checking mounts: {e}")

    # 2. Create master KEK
    print("Configuring workspace master KEK: workspace-kek...")
    vault_request("transit/keys/workspace-kek", method="POST", data={
        "type": "aes256-gcm96",
        "derived": False,
        "exportable": False
    })

    # 3. Generate a sample data key (DEK)
    print("Testing DEK generation...")
    res = vault_request("transit/datakey/plaintext/workspace-kek", method="POST", data={})
    data = res.get("data", {})
    ciphertext = data.get("ciphertext", "")
    print(f"Successfully configured Vault Transit KEK. Sample wrapped DEK: {ciphertext[:30]}...")

if __name__ == "__main__":
    main()
