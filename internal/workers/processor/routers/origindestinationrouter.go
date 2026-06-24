package procesorrouters

import (
	"fmt"
	"hash/fnv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type OriginDestinationRouter struct {
	originRoutes      []string
	destinationRoutes []string
}

func NewOriginDestinationRouter(originRoutes, destinationRoutes []string) *OriginDestinationRouter {
	return &OriginDestinationRouter{
		originRoutes:      originRoutes,
		destinationRoutes: destinationRoutes,
	}
}

func (r *OriginDestinationRouter) Process(_ string, item *protobuf.ScatterGather) ([]sender.RoutedItem[*protobuf.ScatterGather], bool, error) {
	originKey := r.selectOriginRoute(item.GetFromBank(), item.GetAccount())

	destinationKey := r.selectDestinationRoute(item.GetToBank(), item.GetToAccount())
	return []sender.RoutedItem[*protobuf.ScatterGather]{
		{
			Route: originKey,
			Item:  item,
		},
		{
			Route: destinationKey,
			Item:  item,
		},
	}, false, nil
}

func (r *OriginDestinationRouter) selectOriginRoute(bank int32, account string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(fmt.Sprintf("%d-%s", bank, account)))

	return r.originRoutes[h.Sum32()%uint32(len(r.originRoutes))]
}

func (r *OriginDestinationRouter) selectDestinationRoute(bank int32, account string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(fmt.Sprintf("%d-%s", bank, account)))

	return r.destinationRoutes[h.Sum32()%uint32(len(r.destinationRoutes))]
}
