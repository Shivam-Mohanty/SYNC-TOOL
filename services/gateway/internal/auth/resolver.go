package auth

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/sync-tool/gateway/internal/crypto"
)

var (
	ErrKeyNotFound     = errors.New("provider key not found for workspace")
	ErrVaultDecryption = errors.New("vault transit decryption failed")
)

// KeyResolver defines the interface for resolving workspace BYOK provider keys.
type KeyResolver interface {
	ResolveWorkspaceKey(ctx context.Context, workspaceID, providerID string) ([]byte, error)
}

// VaultDBKeyResolver resolves keys by querying PostgreSQL for envelope-encrypted keys
// and unwrapping the DEK via HashiCorp Vault Transit Engine.
type VaultDBKeyResolver struct {
	db         *sql.DB
	vaultAddr  string
	vaultToken string
	httpClient *http.Client
}

func NewVaultDBKeyResolver(db *sql.DB, vaultAddr, vaultToken string) *VaultDBKeyResolver {
	return &VaultDBKeyResolver{
		db:         db,
		vaultAddr:  strings.TrimRight(vaultAddr, "/"),
		vaultToken: vaultToken,
		httpClient: &http.Client{},
	}
}

func (r *VaultDBKeyResolver) ResolveWorkspaceKey(ctx context.Context, workspaceID, providerID string) ([]byte, error) {
	if r.db == nil {
		return nil, errors.New("database connection not initialized")
	}

	var encryptedDEK, encryptedKey, keyNonce []byte
	query := `
		SELECT encrypted_dek, encrypted_key, key_nonce
		FROM workspace_provider_keys
		WHERE workspace_id = $1 AND provider = $2
	`
	err := r.db.QueryRowContext(ctx, query, workspaceID, providerID).Scan(&encryptedDEK, &encryptedKey, &keyNonce)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("failed to query provider key: %w", err)
	}

	// 1. Unwrap DEK using Vault Transit API
	dek, err := r.unwrapDEKWithVault(ctx, string(encryptedDEK))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVaultDecryption, err)
	}
	defer crypto.WipeBytes(dek)

	// 2. Decrypt provider API key using unwrapped DEK and nonce
	apiKey, err := crypto.DecryptWithDEK(dek, encryptedKey, keyNonce)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt provider key: %w", err)
	}

	return apiKey, nil
}

// unwrapDEKWithVault calls Vault's Transit /decrypt/workspace-kek endpoint
func (r *VaultDBKeyResolver) unwrapDEKWithVault(ctx context.Context, ciphertext string) ([]byte, error) {
	reqBody, _ := json.Marshal(map[string]string{
		"ciphertext": ciphertext,
	})

	url := fmt.Sprintf("%s/v1/transit/decrypt/workspace-kek", r.vaultAddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", r.vaultToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("vault returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var vaultResp struct {
		Data struct {
			Plaintext string `json:"plaintext"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&vaultResp); err != nil {
		return nil, err
	}

	dek, err := base64.StdEncoding.DecodeString(vaultResp.Data.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 DEK: %w", err)
	}

	return dek, nil
}

// StaticKeyResolver is used for testing and local development fallback.
type StaticKeyResolver struct {
	keys map[string][]byte
}

func NewStaticKeyResolver(initialKeys map[string]string) *StaticKeyResolver {
	km := make(map[string][]byte)
	for k, v := range initialKeys {
		km[k] = []byte(v)
	}
	return &StaticKeyResolver{keys: km}
}

func (r *StaticKeyResolver) SetKey(workspaceID, providerID, key string) {
	lookupKey := fmt.Sprintf("%s:%s", workspaceID, providerID)
	r.keys[lookupKey] = []byte(key)
}

func (r *StaticKeyResolver) ResolveWorkspaceKey(ctx context.Context, workspaceID, providerID string) ([]byte, error) {
	lookupKey := fmt.Sprintf("%s:%s", workspaceID, providerID)
	if key, ok := r.keys[lookupKey]; ok {
		cp := make([]byte, len(key))
		copy(cp, key)
		return cp, nil
	}

	// Fallback to environment variables if present (e.g. OPENAI_API_KEY)
	envVar := fmt.Sprintf("%s_API_KEY", strings.ToUpper(providerID))
	if envKey := os.Getenv(envVar); envKey != "" {
		return []byte(envKey), nil
	}

	return nil, fmt.Errorf("%w: workspace=%s provider=%s", ErrKeyNotFound, workspaceID, providerID)
}
