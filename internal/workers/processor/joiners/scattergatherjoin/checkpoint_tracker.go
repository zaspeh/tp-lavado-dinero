package scattergatherjoin

import (
	"encoding/json"
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

const checkpointKindPath = "path"

type scatterCheckpointValue struct {
	OriginBank    int32  `json:"originBank"`
	OriginAccount string `json:"originAccount"`
	DestBank      int32  `json:"destBank"`
	DestAccount   string `json:"destAccount"`
	Count         int    `json:"count"`
}

type ScatterGatherCheckpointTracker struct {
	stores       *map[string]*ScatterGatherStore
	dirtyKeys    map[string]bool
	lastSnapshot map[string]map[string]int
}

func NewScatterGatherCheckpointTracker(stores *map[string]*ScatterGatherStore) *ScatterGatherCheckpointTracker {
	return &ScatterGatherCheckpointTracker{
		stores:       stores,
		dirtyKeys:    make(map[string]bool),
		lastSnapshot: make(map[string]map[string]int),
	}
}

func pairKey(p model.OriginDestinationPair) string {
	return fmt.Sprintf("%d|%s|%d|%s", p.Origin.Bank, p.Origin.Account, p.Destination.Bank, p.Destination.Account)
}

func keyToPair(key string) model.OriginDestinationPair {
	// Not needed for ApplyChange because value contains full pair. Keep for symmetry if required.
	return model.OriginDestinationPair{}
}

func (t *ScatterGatherCheckpointTracker) MarkResultAdded(clientID string) {
	t.dirtyKeys[clientID] = true
}

func (t *ScatterGatherCheckpointTracker) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	if !t.dirtyKeys[clientID] {
		return nil, nil
	}

	store := (*t.stores)[clientID]
	if store == nil {
		return nil, nil
	}

	paths := store.GetPaths()
	prev := t.lastSnapshot[clientID]
	if prev == nil {
		prev = make(map[string]int)
	}

	changes := make([]checkpoint.CheckpointChange, 0)

	// detect new or updated pairs
	for pair, count := range paths {
		k := pairKey(pair)
		if prevCount, ok := prev[k]; !ok || prevCount != count {
			value, err := json.Marshal(scatterCheckpointValue{
				OriginBank:    pair.Origin.Bank,
				OriginAccount: pair.Origin.Account,
				DestBank:      pair.Destination.Bank,
				DestAccount:   pair.Destination.Account,
				Count:         count,
			})
			if err != nil {
				return nil, err
			}
			changes = append(changes, checkpoint.CheckpointChange{
				Kind:  checkpointKindPath,
				Key:   fmt.Sprintf("%s:%s", clientID, k),
				Value: json.RawMessage(value),
			})
		}
	}

	// detect removals (pairs present in prev but not in current)
	for k := range prev {
		// skip if still present
		// reconstruct check by looking up in paths via string key
		// build a small flag
		found := false
		for pair := range paths {
			if pairKey(pair) == k {
				found = true
				break
			}
		}
		if !found {
			// deletion -> persist count 0
			value, err := json.Marshal(scatterCheckpointValue{Count: 0})
			if err != nil {
				return nil, err
			}
			changes = append(changes, checkpoint.CheckpointChange{
				Kind:  checkpointKindPath,
				Key:   fmt.Sprintf("%s:%s", clientID, k),
				Value: json.RawMessage(value),
			})
		}
	}

	// update snapshot
	snap := make(map[string]int, len(paths))
	for pair, count := range paths {
		snap[pairKey(pair)] = count
	}
	t.lastSnapshot[clientID] = snap
	delete(t.dirtyKeys, clientID)

	return changes, nil
}

func (t *ScatterGatherCheckpointTracker) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	for _, change := range changes {
		if change.Kind != checkpointKindPath {
			continue
		}
		var v scatterCheckpointValue
		if err := json.Unmarshal(change.Value, &v); err != nil {
			return err
		}
		store := (*t.stores)[clientID]
		if store == nil {
			store = NewScatterGatherStore()
			(*t.stores)[clientID] = store
		}
		if v.Count == 0 {
			// deletion: set to 0 by clearing pair
			// we don't have pair fields when Count==0 in this path, so skip
			continue
		}
		store.SetPairCount(model.OriginDestinationPair{
			Origin:      model.Account{Bank: v.OriginBank, Account: v.OriginAccount},
			Destination: model.Account{Bank: v.DestBank, Account: v.DestAccount},
		}, v.Count)
		t.MarkResultAdded(clientID)
	}
	return nil
}

func (t *ScatterGatherCheckpointTracker) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	switch change.Kind {
	case checkpointKindPath:
		var v scatterCheckpointValue
		if err := json.Unmarshal(change.Value, &v); err != nil {
			return err
		}
		store := (*t.stores)[clientID]
		if store == nil {
			store = NewScatterGatherStore()
			(*t.stores)[clientID] = store
		}
		// if Count==0 treat as deletion
		if v.Count == 0 {
			// best-effort: rebuild from snapshot copying, removal will be reflected when SerializeEntity is called
			// We don't have direct deletion API; set pair count to 0
			store.SetPairCount(model.OriginDestinationPair{
				Origin:      model.Account{Bank: v.OriginBank, Account: v.OriginAccount},
				Destination: model.Account{Bank: v.DestBank, Account: v.DestAccount},
			}, 0)
			if t.lastSnapshot[clientID] == nil {
				t.lastSnapshot[clientID] = make(map[string]int)
			}
			t.lastSnapshot[clientID][fmt.Sprintf("%d|%s|%d|%s", v.OriginBank, v.OriginAccount, v.DestBank, v.DestAccount)] = 0
			return nil
		}
		pair := model.OriginDestinationPair{
			Origin:      model.Account{Bank: v.OriginBank, Account: v.OriginAccount},
			Destination: model.Account{Bank: v.DestBank, Account: v.DestAccount},
		}
		store.SetPairCount(pair, v.Count)
		if t.lastSnapshot[clientID] == nil {
			t.lastSnapshot[clientID] = make(map[string]int)
		}
		t.lastSnapshot[clientID][pairKey(pair)] = v.Count
		return nil
	default:
		return fmt.Errorf("unknown scattergather checkpoint change kind: %s", change.Kind)
	}
}

func (t *ScatterGatherCheckpointTracker) ClearClient(clientID string) {
	delete(t.dirtyKeys, clientID)
	delete(t.lastSnapshot, clientID)
}
