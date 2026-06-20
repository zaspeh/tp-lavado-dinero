package aggregatebyintermediary

import (
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

func GetIntermediaryPairBatchItems(batch *protobuf.MoneyLaundry, inputID string) []IntermediaryPairEvent {
	isOrigin := inputID == "origin"

	items := batch.GetIntermediarypairBatch().GetItems()

	result := make([]IntermediaryPairEvent, 0, len(items))

	for _, item := range items {
		result = append(result,
			IntermediaryPairEvent{
				Pair:     item,
				IsOrigin: isOrigin,
			},
		)
	}

	return result
}
