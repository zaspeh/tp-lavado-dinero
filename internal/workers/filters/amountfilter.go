package filters

import (
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
)

type AmountFilter struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware

	AmountToFilter float64
	coordinator    *c.EOFCoordinator

	microtransactionBatchers map[string]*batch.Batcher[*protobuf.Microtransaction, *protobuf.MicrotransactionBatch]
}

type AmountFilterConfig struct {
	InputQueueName  string
	OutputQueueName string

	MomHost string
	MomPort int

	AmountToFilter float64

	WorkerID           int
	WorkerCount        int
	WorkerExchangeName string
}

func NewAmountFilter(config AmountFilterConfig) (*AmountFilter, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	amountFilter := &AmountFilter{
		inputQueue:     inputQueue,
		outputQueue:    outputQueue,
		AmountToFilter: config.AmountToFilter,

		microtransactionBatchers: make(map[string]*batch.Batcher[*protobuf.Microtransaction, *protobuf.MicrotransactionBatch]),
	}

	coordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: config.WorkerExchangeName,
		ConnSettings:      connSettings,
		WorkerID:          config.WorkerID,
		WorkerCount:       config.WorkerCount,
		FlushHandler:      amountFilter.sendEOFMessage,
	}

	coordinator, err := c.NewEOFCoordinator(coordinatorConfig)
	if err != nil {
		inputQueue.Close()
		amountFilter.outputQueue.Close()
		return nil, err
	}

	amountFilter.coordinator = coordinator

	return amountFilter, nil
}

func (af *AmountFilter) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(
		signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	<-signals
	slog.Info("shutdown signal received")
	af.inputQueue.Close()
	af.coordinator.Close()
	af.outputQueue.Close()
}

func (af *AmountFilter) Run() error {
	go af.coordinator.Run()
	go af.handleSignals()
	return af.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		af.handleMessage(msg, ack, nack)
	})
}

func (af *AmountFilter) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundering, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundering.GetType() {
	case protobuf.MessageType_MICROTRANSACTION_BATCH:
		af.handleMicrotransactionMessage(moneyLaundering, msg, ack, nack)
	case protobuf.MessageType_EOF_:
		af.handleEOF(moneyLaundering, msg, ack, nack)
	default:
		nack()
	}
}

func (af *AmountFilter) handleMicrotransactionMessage(moneyLaundering *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	microtransactionBatch := moneyLaundering.GetMicrotransactionsBatch()
	clientID := moneyLaundering.GetClientID()

	batcher := af.getBatcher(clientID)

	for _, microtransaction := range microtransactionBatch.GetItems() {
		amount, err := strconv.ParseFloat(microtransaction.GetAmountPaid(), 64)
		if err != nil {
			nack()
			return
		}

		if amount < af.AmountToFilter {

			if err := batcher.Add(microtransaction); err != nil {
				nack()
				return
			}

			af.coordinator.RecordSurvivor(clientID)
		}

		af.coordinator.RecordProcessed(clientID)
	}

	if err := batcher.Flush(); err != nil {
		nack()
		return
	}

	ack()
}

func (af *AmountFilter) handleEOF(moneyLaundering *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	slog.Info("Received EOF", "clientID", moneyLaundering.GetClientID())

	eofMessage := moneyLaundering.GetEofMessage()
	clientID := moneyLaundering.GetClientID()

	if batcher := af.microtransactionBatchers[clientID]; batcher != nil {
		if err := batcher.Flush(); err != nil {
			nack()
			return
		}
	}

	if err := af.coordinator.HandleLocalEOF(clientID, eofMessage.GetTotalTransactions()); err != nil {
		nack()
		return
	}
	ack()
}

func (af *AmountFilter) sendEOFMessage(clientID string, newEOFCount uint64) error {
	if !af.coordinator.IsLeader() {
		return nil
	}

	slog.Info("coordinator triggered flush handler, sending EOF message", "clientID", clientID)

	eofMessage := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: newEOFCount,
		},
	}

	msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, eofMessage)
	if err != nil {
		return err
	}

	if err := af.outputQueue.Send(msg); err != nil {
		return err
	}

	return nil
}

func (af *AmountFilter) getBatcher(clientID string) *batch.Batcher[*protobuf.Microtransaction, *protobuf.MicrotransactionBatch] {
	if batcher, ok := af.microtransactionBatchers[clientID]; ok {
		return batcher
	}

	microtransactionBatch := batch.New(
		0,
		protowrappers.ProtoSizer[*protobuf.Microtransaction](),
		protowrappers.WrapToMicrotrasactionBatch,
	)

	onFlush := func(batch *protobuf.MicrotransactionBatch) error {
		return af.sendMicrotransactionBatch(clientID, batch)
	}

	batcher := batch.NewBatcher(
		microtransactionBatch,
		onFlush,
	)

	af.microtransactionBatchers[clientID] = batcher

	return batcher
}

func (af *AmountFilter) sendMicrotransactionBatch(clientID string, batch *protobuf.MicrotransactionBatch) error {

	if len(batch.GetItems()) == 0 {
		return nil
	}

	innerMessage :=
		&protobuf.MoneyLaundry_MicrotransactionsBatch{
			MicrotransactionsBatch: batch,
		}

	serializedMsg, err := protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_MICROTRANSACTION_BATCH,
		innerMessage,
	)

	if err != nil {
		return err
	}

	return af.outputQueue.Send(serializedMsg)
}

// crear un getBatcher porque es un batch por cliente.
// en el handlerMicrotransaction(batch)Message, copio lo que hace currencyfilter (
// agarro el id, extraigo el batch e itero el mensaje que me llegó, si pasa el filtro -> lo agrego al batch)
// Cuando salgo del loop hago flush de una vez (en un futuro no se hará flush) y listo.
// cuando creo el batch, le paso una función de envío. (analizar currencyfilter)
// esto cambia el manejo en el join (habría que guardar los batches y una vez que llega el eof mando la lista de batches).
// USAR LA SERIALIZACIÓN DE PROTOBUF
