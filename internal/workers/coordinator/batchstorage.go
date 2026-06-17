package coordinator

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

const (
	splitSeparator  = "|"
	registerColumns = 2
	clientIDIndex   = 0
	batchIDIndex    = 1
	registerFormat  = "%s|%s\n"
)

type BatchStorage struct {
	mu     sync.RWMutex
	seen   map[string]map[string]bool // clientID -> batchID -> bool
	file   *os.File
	writer *bufio.Writer
}

func NewBatchStorage(workerName string, workerID int) (*BatchStorage, error) {
	// TODO: evaluar si es mejor obtener path de env vars
	path := fmt.Sprintf("/storage/%s-%d/seen_batches.log", workerName, workerID)
	if err := os.MkdirAll(fmt.Sprintf("/storage/%s-%d", workerName, workerID), 0755); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	sb := &BatchStorage{
		seen:   make(map[string]map[string]bool),
		file:   f,
		writer: bufio.NewWriter(f),
	}

	// Si me cai y reinicio, cargo informacion de disco
	if err := sb.loadFromDisk(); err != nil {
		return nil, err
	}

	return sb, nil
}

func (sb *BatchStorage) loadFromDisk() error {
	if _, err := sb.file.Seek(0, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(sb.file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, splitSeparator)
		if len(parts) != registerColumns {
			continue
		}
		clientID, batchID := parts[clientIDIndex], parts[batchIDIndex]
		if sb.seen[clientID] == nil {
			sb.seen[clientID] = make(map[string]bool)
		}
		sb.seen[clientID][batchID] = true
	}
	return scanner.Err()
}

func (sb *BatchStorage) HasSeen(clientID, batchID string) bool {
	sb.mu.RLock()
	defer sb.mu.RUnlock()
	return sb.seen[clientID][batchID]
}

func (sb *BatchStorage) MarkSeen(clientID, batchID string) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	if sb.seen[clientID] == nil {
		sb.seen[clientID] = make(map[string]bool)
	}
	sb.seen[clientID][batchID] = true

	// TODO: short write?
	_, err := fmt.Fprintf(sb.writer, registerFormat, clientID, batchID)
	if err != nil {
		return err
	}

	// TODO: evaluar almacen de Batches y flush eventual manual.
	return sb.writer.Flush()
}

func (sb *BatchStorage) Close() error {
	sb.writer.Flush()
	return sb.file.Close()
}
