# HashiCorp Vault - KV Engine and AppRole Setup

This guide demonstrates how to:

* Enable the KV Secrets Engine
* Create a test secret
* Enable AppRole Authentication
* Create a Terraform policy
* Create an AppRole and retrieve its credentials

## Prerequisites

Set the Vault address:

```bash
export VAULT_ADDR='http://0.0.0.0:8200'
```

Login to Vault using your root/admin token:

```bash
vault login
```

> Use the token starting with `hva.`

---

## 1. Enable KV Secrets Engine

Enable KV Version 2 secrets engine:

```bash
vault secrets enable -path=kv kv-v2
```

Verify:

```bash
vault secrets list
```

---

## 2. Create a Test Secret

Create a sample secret:

```bash
vault kv put kv/test-secret username=shivram
```

Read the secret:

```bash
vault kv get kv/test-secret
```

Expected Output:

```text
Path: kv/data/test-secret

====== Data ======
Key         Value
---         -----
username    shivram
```

---

## 3. Create Terraform Policy

Create a policy named `terraform`:

```bash
vault policy write terraform - <<EOF
path "*" {
  capabilities = ["list", "read"]
}

path "secrets/data/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "kv/data/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "secret/data/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "auth/token/create" {
  capabilities = ["create", "read", "update", "list"]
}
EOF
```

Verify:

```bash
vault policy read terraform
```

---

## 4. Enable AppRole Authentication

Enable AppRole authentication method:

```bash
vault auth enable approle
```

Verify:

```bash
vault auth list
```

---

## 5. Create AppRole

Create an AppRole named `terraform` and attach the policy:

```bash
vault write auth/approle/role/terraform \
    token_policies="terraform"
```

Verify:

```bash
vault read auth/approle/role/terraform
```

---

## 6. Get Role ID

Retrieve the Role ID:

```bash
vault read auth/approle/role/terraform/role-id
```

Example Output:

```text
Key        Value
---        -----
role_id    xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

---

## 7. Generate Secret ID

Generate a Secret ID:

```bash
vault write -f auth/approle/role/terraform/secret-id
```

Example Output:

```text
Key          Value
---          -----
secret_id    xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

---

## 8. Login Using AppRole

Authenticate using the Role ID and Secret ID:

```bash
vault write auth/approle/login \
    role_id="<ROLE_ID>" \
    secret_id="<SECRET_ID>"
```

The response will contain a Vault token that can be used by Terraform or applications.

---

## 9. Verify Secret Access

Using the AppRole token:

```bash
vault kv get kv/test-secret
```

Expected secret:

```text
username = shivram
```

---

## Useful Commands

List secrets engines:

```bash
vault secrets list
```

List auth methods:

```bash
vault auth list
```

List policies:

```bash
vault policy list
```

Read AppRole configuration:

```bash
vault read auth/approle/role/terraform
```
