package procesorrouters

import (
	"hash/fnv"
	"log/slog"
	"strconv"

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

func (r *SuspiciousPathToJoinRouter) Process(_ string, item *protobuf.SuspiciousPath) ([]sender.RoutedItem[*protobuf.SuspiciousPath], bool, error) {
	slog.Debug("Routing suspicious path", "origin_bank", item.GetOrigin().GetBank(), "origin_account", item.GetOrigin().GetAccount(), "destination_bank", item.GetDestination().GetBank(), "destination_account", item.GetDestination().GetAccount())
	return []sender.RoutedItem[*protobuf.SuspiciousPath]{
		{
			Route: r.selectSouspiciousPathRoute(item.GetOrigin()),
			Item:  item,
		},
	}, false, nil
}

func (r *SuspiciousPathToJoinRouter) selectSouspiciousPathRoute(origin *protobuf.Account) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(strconv.Itoa(int(origin.GetBank())) + "-" + origin.GetAccount()))

	return r.routes[h.Sum32()%uint32(len(r.routes))]
}
