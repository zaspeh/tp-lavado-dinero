package aggregatebyintermediary

import protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"

type IntermediaryPairEvent struct {
	Pair     *protobuf.IntermediaryPair
	IsOrigin bool
}
