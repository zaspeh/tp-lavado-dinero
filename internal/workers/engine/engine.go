package engine

type Engine interface {
	Run() error
	Shutdown()
}
