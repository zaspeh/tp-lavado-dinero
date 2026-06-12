package procesorrouters

import (
	"hash/fnv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type AvgByTypeJoinRouter struct {
	routes []string
}

func NewAvgByTypeJoinRouter(routes []string) *AvgByTypeJoinRouter {
	return &AvgByTypeJoinRouter{
		routes: routes,
	}
}

func (r *AvgByTypeJoinRouter) Process(_ string, item *protobuf.AvgByTypeResult) ([]sender.RoutedItem[*protobuf.AvgByTypeResult], error) {
	return []sender.RoutedItem[*protobuf.AvgByTypeResult]{
		{
			Route: r.selectRoute(item.GetAccount()),
			Item:  item,
		},
	}, nil
}

func (r *AvgByTypeJoinRouter) selectRoute(key string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	return r.routes[h.Sum32()%uint32(len(r.routes))]
}
