package maxbankjoin

import (
	"sync"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type MaxBankStore struct {
	mu      sync.RWMutex
	results []*protobuf.MaxBankResult
}

func newMaxBankStore() *MaxBankStore {
	return &MaxBankStore{
		results: make([]*protobuf.MaxBankResult, 0),
	}
}

func (s *MaxBankStore) Add(result *protobuf.MaxBankResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.results = append(s.results, result)
}

func (s *MaxBankStore) GetResults() []*protobuf.MaxBankResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make([]*protobuf.MaxBankResult, len(s.results))
	copy(results, s.results)

	return results
}

func (s *MaxBankStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.results = nil
}
