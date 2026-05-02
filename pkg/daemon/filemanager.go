package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type FileManager struct {
	downloadDir string
}

func NewFileManager(downloadDir string) (*FileManager, error) {
	expandedDir, err := ExpandPath(downloadDir)
	if err != nil {
		return nil, fmt.Errorf("failed to expand download directory path: %w", err)
	}

	if err := os.MkdirAll(expandedDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create download directory: %w", err)
	}

	return &FileManager{
		downloadDir: expandedDir,
	}, nil
}

func (fm *FileManager) GetDownloadPath(fileHash, userPath string) (string, error) {
	var finalPath string

	if userPath != "" {
		expandedPath, err := ExpandPath(userPath)
		if err != nil {
			return "", fmt.Errorf("failed to expand user path: %w", err)
		}
		finalPath = expandedPath
	} else {
		finalPath = filepath.Join(fm.downloadDir, fileHash)
	}

	finalPath = fm.ResolveConflict(finalPath)

	parentDir := filepath.Dir(finalPath)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create parent directory: %w", err)
	}

	return finalPath, nil
}

func (fm *FileManager) ResolveConflict(path string) string {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return path
	}

	dir := filepath.Dir(path)
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	nameWithoutExt := strings.TrimSuffix(base, ext)

	counter := 1
	for {
		newName := fmt.Sprintf("%s_%d%s", nameWithoutExt, counter, ext)
		newPath := filepath.Join(dir, newName)

		if _, err := os.Stat(newPath); os.IsNotExist(err) {
			return newPath
		}

		counter++

		if counter > 1000 {
			timestamp := fmt.Sprintf("%d", os.Getpid())
			newName = fmt.Sprintf("%s_%s%s", nameWithoutExt, timestamp, ext)
			return filepath.Join(dir, newName)
		}
	}
}

func (fm *FileManager) GetPartialPath(finalPath string) string {
	return finalPath + ".partial"
}

func (fm *FileManager) FinalizeDownload(finalPath string) error {
	partialPath := fm.GetPartialPath(finalPath)

	if _, err := os.Stat(partialPath); os.IsNotExist(err) {
		return fmt.Errorf("partial file does not exist: %s", partialPath)
	}

	if err := os.Rename(partialPath, finalPath); err != nil {
		return fmt.Errorf("failed to rename partial file: %w", err)
	}

	return nil
}

func (fm *FileManager) CleanupPartial(finalPath string) error {
	partialPath := fm.GetPartialPath(finalPath)

	if err := os.Remove(partialPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to cleanup partial file: %w", err)
	}

	return nil
}

func (fm *FileManager) GetDownloadDir() string {
	return fm.downloadDir
}

func (fm *FileManager) ValidatePath(path string) error {
	expandedPath, err := ExpandPath(path)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}

	absPath, err := filepath.Abs(expandedPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	if strings.Contains(absPath, "..") {
		return fmt.Errorf("path contains directory traversal: %s", path)
	}

	return nil
}

func (fm *FileManager) EnsureSpace(path string, requiredBytes uint64) error {
	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		dir = fm.downloadDir
	}

	testFile := filepath.Join(dir, ".space_test")
	f, err := os.Create(testFile)
	if err != nil {
		return fmt.Errorf("insufficient permissions or disk space: %w", err)
	}
	f.Close()
	os.Remove(testFile)

	return nil
}

func (fm *FileManager) GetFileSize(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return uint64(info.Size()), nil
}

func (fm *FileManager) FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
