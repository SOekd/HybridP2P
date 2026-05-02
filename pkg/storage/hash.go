package storage

import (
	"crypto/sha256"
	"io"
	"os"

	"P2P-CDN/pkg/protocol"
)

func HashFile(filePath string) (protocol.FileHash, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return protocol.FileHash{}, err
	}
	defer file.Close()

	return HashReader(file)
}

func HashReader(reader io.Reader) (protocol.FileHash, error) {
	hasher := sha256.New()
	if _, err := io.Copy(hasher, reader); err != nil {
		return protocol.FileHash{}, err
	}

	var hash protocol.FileHash
	copy(hash[:], hasher.Sum(nil))
	return hash, nil
}

func HashBytes(data []byte) protocol.FileHash {
	return sha256.Sum256(data)
}

func VerifyFile(filePath string, expectedHash protocol.FileHash) (bool, error) {
	actualHash, err := HashFile(filePath)
	if err != nil {
		return false, err
	}

	return actualHash == expectedHash, nil
}

func VerifyBytes(data []byte, expectedHash protocol.FileHash) bool {
	actualHash := HashBytes(data)
	return actualHash == expectedHash
}
