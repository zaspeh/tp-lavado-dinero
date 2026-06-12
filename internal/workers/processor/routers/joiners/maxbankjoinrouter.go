package procesorrouters

import (
	"hash/fnv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type MaxBankJoinRouter struct {
	routes []string
}

func NewMaxBankToJoinRouter(routes []string) *MaxBankJoinRouter {
	return &MaxBankJoinRouter{
		routes: routes,
	}
}

func (r *MaxBankJoinRouter) Process(_ string, item *protobuf.MaxBankResult) ([]sender.RoutedItem[*protobuf.MaxBankResult], error) {
	return []sender.RoutedItem[*protobuf.MaxBankResult]{
		{
			Route: r.selectRoute(item.GetBankName()),
			Item:  item,
		},
	}, nil
}

func (r *MaxBankJoinRouter) selectRoute(key string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	return r.routes[h.Sum32()%uint32(len(r.routes))]
}
