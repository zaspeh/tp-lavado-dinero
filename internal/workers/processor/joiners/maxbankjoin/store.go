package maxbankjoin

import (
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type MaxBankStore struct {
	results []*protobuf.MaxBankResult
}

func newMaxBankStore() *MaxBankStore {
	return &MaxBankStore{
		results: make([]*protobuf.MaxBankResult, 0),
	}
}

func (s *MaxBankStore) Add(result *protobuf.MaxBankResult) {
	s.results = append(s.results, result)
}

func (s *MaxBankStore) GetResults() []*protobuf.MaxBankResult {
	results := make([]*protobuf.MaxBankResult, len(s.results))
	copy(results, s.results)

	return results
}

func (s *MaxBankStore) Clear() {
	s.results = nil
}
