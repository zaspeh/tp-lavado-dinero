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
)

type BackRouterConfig struct {
	MomHost             string
	MomPort             int
	InputQueueName      string
	MaxBankExchangeName string
	MaxBankWorkerAmount int
}

type BankRouter struct {
	inputQueue          middleware.Middleware
	maxBankExchange     *middleware.ExchangeMiddleware
	maxBankExchangeKeys []string
	maxWorkersAmount    int
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

	return &BankRouter{
		inputQueue:          inputQueue,
		maxBankExchange:     maxBankExchange,
		maxBankExchangeKeys: maxBankExchangeKeys,
		maxWorkersAmount:    config.MaxBankWorkerAmount,
	}, nil
}

func (br *BankRouter) Run() error {
	go br.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		br.handleMessage(msg, ack, nack)
	})

	go br.handleSignals()

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

	ack()
}

func (br *BankRouter) selectWorkerKey(bankName string) string {
	h := fnv.New32a()
	h.Write([]byte(bankName))
	return br.maxBankExchangeKeys[h.Sum32()%uint32(br.maxWorkersAmount)]
}
