package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

var (
	ErrInvalidNonce      = errors.New("invalid nonce length: must be 12 bytes for AES-GCM")
	ErrDecryptionFailed  = errors.New("decryption failed: ciphertext corrupted or key incorrect")
	ErrInvalidDEKLength  = errors.New("invalid DEK length: must be 32 bytes for AES-256")
)

// DecryptWithDEK decrypts an AES-256-GCM encrypted payload using the plaintext DEK.
func DecryptWithDEK(dek, ciphertext, nonce []byte) ([]byte, error) {
	if len(dek) != 32 {
		return nil, ErrInvalidDEKLength
	}
	if len(nonce) != 12 {
		return nil, ErrInvalidNonce
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	plaintext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	return plaintext, nil
}

// EncryptWithDEK encrypts a plaintext payload using AES-256-GCM.
// It generates a new cryptographically secure 12-byte nonce on every call (FIX #13).
func EncryptWithDEK(dek, plaintext []byte) (ciphertext []byte, nonce []byte, keyHash string, err error) {
	if len(dek) != 32 {
		return nil, nil, "", ErrInvalidDEKLength
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to create cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce = make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, "", fmt.Errorf("failed to generate random nonce: %w", err)
	}

	ciphertext = aesGCM.Seal(nil, nonce, plaintext, nil)

	// Compute SHA-256 fingerprint of the key
	hasher := sha256.New()
	hasher.Write(plaintext)
	keyHash = hex.EncodeToString(hasher.Sum(nil))

	return ciphertext, nonce, keyHash, nil
}
