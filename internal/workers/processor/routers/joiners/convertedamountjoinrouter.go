package procesorrouters

import (
	"hash/fnv"
	"strconv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type ConvertedAmountJoinRouter struct {
	routes []string
}

func NewConvertedAmountJoinRouter(routes []string) *ConvertedAmountJoinRouter {
	return &ConvertedAmountJoinRouter{
		routes: routes,
	}
}

func (r *ConvertedAmountJoinRouter) Process(_ string, item *protobuf.ConvertedAmount) ([]sender.RoutedItem[*protobuf.ConvertedAmount], error) {
	return []sender.RoutedItem[*protobuf.ConvertedAmount]{
		{
			Route: r.selectRoute(strconv.FormatFloat(item.GetAmount(), 'f', 2, 64)),
			Item:  item,
		},
	}, nil
}

func (r *ConvertedAmountJoinRouter) selectRoute(key string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	return r.routes[h.Sum32()%uint32(len(r.routes))]
}
