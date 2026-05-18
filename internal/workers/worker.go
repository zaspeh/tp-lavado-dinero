package workers

type Worker interface {

	// Inicia el trabajo del trabajador.
	// No se detiene el trabajo hasat recibir SIGTERM o SIGINT.
	Run() error
}
