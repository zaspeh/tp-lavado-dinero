package microtransactionjoin

import (
	"sync"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type MicrotransactionStore struct {
	mu      sync.RWMutex
	results []*protobuf.Microtransaction
}

func newMicrotransactionStore() *MicrotransactionStore {
	return &MicrotransactionStore{
		results: make([]*protobuf.Microtransaction, 0),
	}
}

func (s *MicrotransactionStore) Add(result *protobuf.Microtransaction) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.results = append(s.results, result)
}

func (s *MicrotransactionStore) GetResults() []*protobuf.Microtransaction {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make([]*protobuf.Microtransaction, len(s.results))
	copy(results, s.results)

	return results
}

func (s *MicrotransactionStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.results = nil
}
