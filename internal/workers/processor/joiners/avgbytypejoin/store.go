package avgbytypejoin

import (
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type AvgByTypeResultStore struct {
	results []*protobuf.AvgByTypeResult
}

func newAvgByTypeResultStore() *AvgByTypeResultStore {
	return &AvgByTypeResultStore{
		results: make([]*protobuf.AvgByTypeResult, 0),
	}
}

func (s *AvgByTypeResultStore) Add(result *protobuf.AvgByTypeResult) {
	s.results = append(s.results, result)
}

func (s *AvgByTypeResultStore) GetResults() []*protobuf.AvgByTypeResult {
	results := make([]*protobuf.AvgByTypeResult, len(s.results))
	copy(results, s.results)
	return results
}

func (s *AvgByTypeResultStore) Clear() {
	s.results = nil
}
