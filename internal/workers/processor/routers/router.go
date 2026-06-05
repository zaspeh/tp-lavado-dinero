package procesorrouters

import (
	"hash/fnv"

	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type RouterProcessor[T any] struct {
	routes      []string
	keySelector func(T) string
}

func NewRouterProcessor[T any](routes []string, keySelector func(T) string) *RouterProcessor[T] {
	return &RouterProcessor[T]{
		routes:      routes,
		keySelector: keySelector,
	}
}

func (r *RouterProcessor[T]) Process(_ string, item T) ([]sender.RoutedItem[T], error) {
	return []sender.RoutedItem[T]{
		{
			Route: r.selectRoute(r.keySelector(item)),
			Item:  item,
		},
	}, nil
}

func (r *RouterProcessor[T]) selectRoute(key string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	return r.routes[h.Sum32()%uint32(len(r.routes))]
}
