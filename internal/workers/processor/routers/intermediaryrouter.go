package procesorrouters

import (
	"fmt"
	"hash/fnv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

/*
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

func (r *OriginDestinationRouter) Process(_ string, item *protobuf.ScatterGather) ([]sender.RoutedItem[*protobuf.ScatterGather], error) {
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
	}, nil
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
}*/

type IntermediaryRouter struct {
	routes []string
}

func NewIntermediaryRouter(routes []string) *IntermediaryRouter {
	return &IntermediaryRouter{
		routes: routes,
	}
}

func (r *IntermediaryRouter) Process(_ string, item *protobuf.GroupedAccounts) ([]sender.RoutedItem[*protobuf.IntermediaryPair], bool, error) {
	group := item.GetRelatedAccounts()
	results := make([]sender.RoutedItem[*protobuf.IntermediaryPair], 0, len(group))
	for _, account := range group {
		results = append(results, sender.RoutedItem[*protobuf.IntermediaryPair]{
			Route: r.selectIntermediaryRoute(account),
			Item: &protobuf.IntermediaryPair{
				Intermediary: account,
				Account:      item.GetBaseAccount(),
			},
		})
	}
	return results, true, nil
}

func (r *IntermediaryRouter) selectIntermediaryRoute(account *protobuf.Account) string {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%d-%s", account.GetBank(), account.GetAccount())))

	return r.routes[h.Sum32()%uint32(len(r.routes))]
}
