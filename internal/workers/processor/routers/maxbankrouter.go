package procesorrouters

import (
	"hash/fnv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type MaxBankRouter struct {
	routes []string
}

func NewMaxBankRouter(routes []string) *MaxBankRouter {
	return &MaxBankRouter{
		routes: routes,
	}
}

func (r *MaxBankRouter) Process(_ string, item *protobuf.MaxBank) ([]sender.RoutedItem[*protobuf.MaxBank], error) {
	return []sender.RoutedItem[*protobuf.MaxBank]{
		{
			Route: r.selectRoute(item.GetFromBank()),
			Item:  item,
		},
	}, nil
}

func (r *MaxBankRouter) selectRoute(bankID int32) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte{
		byte(bankID),
		byte(bankID >> 8),
		byte(bankID >> 16),
		byte(bankID >> 24),
	})
	return r.routes[h.Sum32()%uint32(len(r.routes))]
}
