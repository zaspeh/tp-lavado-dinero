package procesorrouters

import (
	"hash/fnv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type SuspiciousPathToJoinRouter struct {
	routes []string
}

func NewSuspiciousPathToJoinRouter(routes []string) *SuspiciousPathToJoinRouter {
	return &SuspiciousPathToJoinRouter{
		routes: routes,
	}
}

func (r *SuspiciousPathToJoinRouter) Process(_ string, item *protobuf.SuspiciousPath) ([]sender.RoutedItem[*protobuf.SuspiciousPath], error) {
	return []sender.RoutedItem[*protobuf.SuspiciousPath]{
		{
			Route: r.selectRoute(item.GetDestination().GetAccount()),
			Item:  item,
		},
	}, nil
}

func (r *SuspiciousPathToJoinRouter) selectRoute(key string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	return r.routes[h.Sum32()%uint32(len(r.routes))]
}
