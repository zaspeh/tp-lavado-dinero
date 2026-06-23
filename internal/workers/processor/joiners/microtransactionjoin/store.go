package microtransactionjoin

import (
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type MicrotransactionStore struct {
	results []*protobuf.Microtransaction
}

func newMicrotransactionStore() *MicrotransactionStore {
	return &MicrotransactionStore{
		results: make([]*protobuf.Microtransaction, 0),
	}
}

func (s *MicrotransactionStore) Add(result *protobuf.Microtransaction) {
	s.results = append(s.results, result)
}

func (s *MicrotransactionStore) GetResults() []*protobuf.Microtransaction {
	results := make([]*protobuf.Microtransaction, len(s.results))
	copy(results, s.results)
	return results
}

func (s *MicrotransactionStore) Clear() {
	s.results = nil
}
