package crypto_test

import (
	"bytes"
	"testing"

	"github.com/sync-tool/gateway/internal/crypto"
)

func TestWipeBytes(t *testing.T) {
	secret := []byte("super-secret-api-key-12345")
	crypto.WipeBytes(secret)
	for i, b := range secret {
		if b != 0 {
			t.Fatalf("byte at index %d was not zeroed: %v", i, b)
		}
	}
}

func TestEnvelopeEncryptDecrypt(t *testing.T) {
	dek := make([]byte, 32)
	for i := range dek {
		dek[i] = byte(i + 1)
	}

	plaintextKey := []byte("sk-proj-1234567890abcdefghijklmnopqrstuvwxyz")

	// Encrypt
	ciphertext1, nonce1, hash1, err := crypto.EncryptWithDEK(dek, plaintextKey)
	if err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}
	if len(nonce1) != 12 {
		t.Fatalf("expected 12-byte nonce, got %d", len(nonce1))
	}
	if hash1 == "" {
		t.Fatal("expected non-empty key hash")
	}

	// Decrypt
	decrypted, err := crypto.DecryptWithDEK(dek, ciphertext1, nonce1)
	if err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}
	if !bytes.Equal(decrypted, plaintextKey) {
		t.Fatalf("decrypted %s does not match plaintext %s", string(decrypted), string(plaintextKey))
	}

	// FIX #13: Key rotation MUST regenerate both encrypted_key AND key_nonce
	ciphertext2, nonce2, hash2, err := crypto.EncryptWithDEK(dek, plaintextKey)
	if err != nil {
		t.Fatalf("second encrypt failed: %v", err)
	}
	if bytes.Equal(nonce1, nonce2) {
		t.Fatal("nonce collision: new encryption must produce a unique nonce")
	}
	if bytes.Equal(ciphertext1, ciphertext2) {
		t.Fatal("ciphertext collision: distinct nonces must produce distinct ciphertexts")
	}
	if hash1 != hash2 {
		t.Fatal("same plaintext key must produce identical fingerprint hash")
	}
}
