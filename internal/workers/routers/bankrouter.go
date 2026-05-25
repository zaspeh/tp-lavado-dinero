package routers

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
)

const (
	sourcesAmounts = 2
)

type BackRouterConfig struct {
	ID                  int
	MomHost             string
	MomPort             int
	InputQueueName      string
	MaxBankExchangeName string
	MaxBankWorkerAmount int
	WorkerCount         int
	WorkerExchangeName  string
}

type BankRouter struct {
	inputQueue          middleware.Middleware
	maxBankExchange     *middleware.ExchangeMiddleware
	maxBankExchangeKeys []string
	maxWorkersAmount    int
	coordinator         *c.EOFCoordinator
}

func NewBankRouter(config BackRouterConfig) (*BankRouter, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	maxBankExchangeKeys := make([]string, config.MaxBankWorkerAmount)
	for i := range maxBankExchangeKeys {
		maxBankExchangeKeys[i] = fmt.Sprintf("%s.%d", config.MaxBankExchangeName, i)
	}

	maxBankExchange, err := middleware.CreateExchangeMiddleware(config.MaxBankExchangeName, maxBankExchangeKeys, connSettings)
	if err != nil {
		// TODO: verificar error de cierre?
		inputQueue.Close()
		return nil, err
	}

	bankRouter := &BankRouter{
		inputQueue:          inputQueue,
		maxBankExchange:     maxBankExchange,
		maxBankExchangeKeys: maxBankExchangeKeys,
		maxWorkersAmount:    config.MaxBankWorkerAmount,
	}

	coordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: config.WorkerExchangeName,
		ConnSettings:      connSettings,
		WorkerID:          config.ID,
		WorkerCount:       config.WorkerCount,
		ExpectedEOFs:      sourcesAmounts,
		FlushHandler:      bankRouter.handleFlush,
	}

	coordinator, err := c.NewEOFCoordinator(coordinatorConfig)
	if err != nil {
		inputQueue.Close()
		maxBankExchange.Close()
		return nil, err
	}
	bankRouter.coordinator = coordinator
	return bankRouter, nil
}

func (br *BankRouter) Run() error {
	go br.handleSignals()
	go br.coordinator.Run()
	br.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		br.handleMessage(msg, ack, nack)
	})

	//TODO: REVISAR SI HAY FORMA DE DEVOLVER ERRORES
	return nil
}

func (br *BankRouter) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(
		signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	<-signals
	slog.Info("shutdown signal received")
	br.inputQueue.Close()
	br.maxBankExchange.Close()
}

func (br *BankRouter) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_MAXBANK:
		br.handleMaxBankMessage(moneyLaundry, msg, ack, nack)
	case protobuf.MessageType_MAXBANK_BATCH:
		br.handleMaxBankBatch(moneyLaundry, ack, nack)
	case protobuf.MessageType_EOF_:
		br.handleEOFMessage(moneyLaundry, ack, nack)
	default:
		nack()
	}
}

func (br *BankRouter) handleMaxBankMessage(moneyLaundryMsg *protobuf.MoneyLaundry, serializeMsg middleware.Message, ack, nack func()) {
	maxBankMessage, err := serializer.DeserializeTransaction(moneyLaundryMsg.GetPayload(), &protobuf.MaxBank{})
	if err != nil {
		nack()
		return
	}

	workerKey := br.selectWorkerKey(maxBankMessage.GetFromBank())
	if err := br.maxBankExchange.SendWithKey(workerKey, serializeMsg); err != nil {
		nack()
		return
	}

	clientID := moneyLaundryMsg.GetClientID()
	br.coordinator.RecordProcessed(clientID)
	br.coordinator.RecordSurvivor(clientID)
	ack()
}

func (br *BankRouter) handleMaxBankBatch(moneyLaundryMsg *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundryMsg.GetClientID()
	maxBankBatch := moneyLaundryMsg.GetMaxBankBatch()
	if maxBankBatch == nil || len(maxBankBatch.GetMaxBankMessage()) == 0 {
		ack()
		return
	}

	batchesByKey := make(map[string][]*protobuf.MaxBank)
	for _, maxBankMessage := range maxBankBatch.GetMaxBankMessage() {
		workerKey := br.selectWorkerKey(maxBankMessage.GetFromBank())
		batchesByKey[workerKey] = append(batchesByKey[workerKey], maxBankMessage)
	}

	for workerKey, batchMessages := range batchesByKey {
		innerMessage := &protobuf.MoneyLaundry_MaxBankBatch{
			MaxBankBatch: &protobuf.MaxBankBatch{
				MaxBankMessage: batchMessages,
			},
		}

		msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_MAXBANK_BATCH, innerMessage)
		if err != nil {
			nack()
			return
		}

		if err := br.maxBankExchange.SendWithKey(workerKey, msg); err != nil {
			nack()
			return
		}
	}

	for range maxBankBatch.GetMaxBankMessage() {
		br.coordinator.RecordProcessed(clientID)
		br.coordinator.RecordSurvivor(clientID)
	}

	ack()
}

func (br *BankRouter) handleEOFMessage(moneyLaundryMsg *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundryMsg.GetClientID()
	eofMessage := moneyLaundryMsg.GetEofMessage()
	if err := br.coordinator.HandleLocalEOF(clientID, eofMessage.GetTotalTransactions()); err != nil {
		nack()
		return
	}
	ack()
}

func (br *BankRouter) handleFlush(clientID string, totalSurvivors uint64) error {
	slog.Info("Flushing client", "clientID", clientID, "totalSurvivors", totalSurvivors)
	innerMessage := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: totalSurvivors,
		},
	}

	msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, innerMessage)
	if err != nil {
		return err
	}

	return br.maxBankExchange.Send(msg)
}

func (br *BankRouter) selectWorkerKey(bankID int32) string {
	h := fnv.New32a()
	h.Write([]byte{
		byte(bankID),
		byte(bankID >> 8),
		byte(bankID >> 16),
		byte(bankID >> 24),
	})
	return br.maxBankExchangeKeys[h.Sum32()%uint32(br.maxWorkersAmount)]
}
