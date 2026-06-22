// storage.go — solo persistencia, sin mezcla de responsabilidades
package coordinator

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	batchSeparator     = "|"
	batchColumns       = 4
	batchFormat        = "%s|%s|%d|%d\n"
	eofFormat          = "%s|%s|%d\n"
	clientIDIndex      = 0
	batchIDIndex       = 1
	eofIDIndex         = 1
	processedIndex     = 2
	survivorsIndex     = 3
	expectedTotalIndex = 2
)

type BatchRecord struct {
	BatchID   string
	Processed uint64
	Survivors uint64
}

type BatchStorage struct {
	batchFile   *os.File
	batchWriter *bufio.Writer
	eofFile     *os.File
	eofWriter   *bufio.Writer
}

func NewBatchStorage(workerName string, workerID int) (*BatchStorage, error) {
	dir := fmt.Sprintf("/storage/%s-%d", workerName, workerID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	batchFile, err := os.OpenFile(fmt.Sprintf("%s/batches.log", dir), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	eofFile, err := os.OpenFile(fmt.Sprintf("%s/eofs.log", dir), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		batchFile.Close()
		return nil, err
	}

	return &BatchStorage{
		batchFile:   batchFile,
		batchWriter: bufio.NewWriter(batchFile),
		eofFile:     eofFile,
		eofWriter:   bufio.NewWriter(eofFile),
	}, nil
}

// LoadBatches deberia ser llamado solo al arrancar, no hay RC
func (s *BatchStorage) LoadBatches() (map[string]map[string]BatchRecord, error) {
	if _, err := s.batchFile.Seek(0, 0); err != nil {
		return nil, err
	}

	result := make(map[string]map[string]BatchRecord)
	scanner := bufio.NewScanner(s.batchFile)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), batchSeparator)
		if len(parts) != batchColumns {
			continue
		}
		clientID, batchID := parts[clientIDIndex], parts[batchIDIndex]
		processed, err := strconv.ParseUint(parts[processedIndex], 10, 64)
		if err != nil {
			continue
		}
		survivors, err := strconv.ParseUint(parts[survivorsIndex], 10, 64)
		if err != nil {
			continue
		}
		if result[clientID] == nil {
			result[clientID] = make(map[string]BatchRecord)
		}
		result[clientID][batchID] = BatchRecord{
			BatchID:   batchID,
			Processed: processed,
			Survivors: survivors,
		}
	}
	return result, scanner.Err()
}

// LoadEOFs reconstruye los EOFs vistos al arrancar, no hay RC
func (s *BatchStorage) LoadEOFs() (map[string]map[string]uint64, error) {
	if _, err := s.eofFile.Seek(0, 0); err != nil {
		return nil, err
	}

	result := make(map[string]map[string]uint64)
	scanner := bufio.NewScanner(s.eofFile)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), batchSeparator)
		if len(parts) != 3 {
			continue
		}
		clientID, eofID := parts[clientIDIndex], parts[eofIDIndex]
		expectedTotal, err := strconv.ParseUint(parts[expectedTotalIndex], 10, 64)
		if err != nil {
			continue
		}
		if result[clientID] == nil {
			result[clientID] = make(map[string]uint64)
		}
		result[clientID][eofID] = expectedTotal
	}
	return result, scanner.Err()
}

func (s *BatchStorage) WriteBatch(clientID string, record BatchRecord) error {
	_, err := fmt.Fprintf(s.batchWriter, batchFormat,
		clientID, record.BatchID, record.Processed, record.Survivors)
	if err != nil {
		return err
	}
	return s.batchWriter.Flush()
}

func (s *BatchStorage) WriteEOF(clientID, eofID string, expectedTotal uint64) error {
	_, err := fmt.Fprintf(s.eofWriter, eofFormat, clientID, eofID, expectedTotal)
	if err != nil {
		return err
	}
	return s.eofWriter.Flush()
}

func (s *BatchStorage) Close() error {
	s.batchWriter.Flush()
	s.eofWriter.Flush()
	s.batchFile.Close()
	return s.eofFile.Close()
}
