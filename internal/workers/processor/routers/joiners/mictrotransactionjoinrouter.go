package procesorrouters

import (
	"hash/fnv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type MicrotransactionJoinRouter struct {
	routes []string
}

func NewMicrotransactionJoinRouter(routes []string) *MicrotransactionJoinRouter {
	return &MicrotransactionJoinRouter{
		routes: routes,
	}
}

func (r *MicrotransactionJoinRouter) Process(_ string, item *protobuf.Microtransaction) ([]sender.RoutedItem[*protobuf.Microtransaction], error) {
	return []sender.RoutedItem[*protobuf.Microtransaction]{
		{
			Route: r.selectRoute(item.GetAccount()),
			Item:  item,
		},
	}, nil
}

func (r *MicrotransactionJoinRouter) selectRoute(key string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	return r.routes[h.Sum32()%uint32(len(r.routes))]
}
