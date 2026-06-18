package joiners

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	coordinator "github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type JoinMicrotransaction struct {
	inputExchange      *middleware.ExchangeMiddleware
	resultExchange     *middleware.ExchangeMiddleware
	clientExchangeName string
	results            map[string][]*protobuf.Microtransaction
	maxBatchBytes      int
	checkpointManager  *checkpoint.CheckpointManager
	workerName         string
	workerID           int
}

type JoinMicrotransactionConfig struct {
	ID                 string
	InputExchangeName  string
	ClientExchangeName string
	MomHost            string
	MomPort            int
	MaxBatchBytes      int
}

type microtransactionCheckpoint struct {
	Account   string  `json:"account"`
	ToAccount string  `json:"to_account"`
	Amount    float64 `json:"amount"`
}

type clientStateCheckpoint struct {
	Results []*microtransactionCheckpoint `json:"results"`
}

func NewJoinMicrotransaction(config JoinMicrotransactionConfig) (*JoinMicrotransaction, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}
	inputExchangeKeys := []string{
		fmt.Sprintf("%s.%s", config.InputExchangeName, config.ID),
	}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchangeName, inputExchangeKeys, connSettings)
	if err != nil {
		return nil, err
	}

	resultExchange, err := middleware.CreateExchangeMiddleware(config.ClientExchangeName, []string{config.ClientExchangeName}, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	j := &JoinMicrotransaction{
		inputExchange:      inputExchange,
		resultExchange:     resultExchange,
		clientExchangeName: config.ClientExchangeName,
		results:            make(map[string][]*protobuf.Microtransaction),
		maxBatchBytes:      config.MaxBatchBytes,
		workerName:         config.InputExchangeName,
	}

	if id, err := strconv.Atoi(config.ID); err == nil {
		j.workerID = id
	} else {
		j.workerID = 0
	}

	storage, err := coordinator.NewBatchStorage(j.workerName, j.workerID)
	if err != nil {
		resultExchange.Close()
		inputExchange.Close()
		return nil, err
	}

	j.checkpointManager = checkpoint.NewCheckpointManager(j, storage)
	if err := j.checkpointManager.LoadState(); err != nil {
		storage.Close()
		resultExchange.Close()
		inputExchange.Close()
		return nil, err
	}

	return j, nil
}

func (j *JoinMicrotransaction) Run() error {
	go j.handleSignals()

	j.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		j.handleMessage(msg, ack, nack)
	})

	//TODO: REVISAR SI HAY FORMA DE DEVOLVER ERRORES
	return nil
}

func (j *JoinMicrotransaction) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_MICROTRANSACTION_BATCH:
		j.handleMicrotransactionMessage(moneyLaundry, ack, nack)

	case protobuf.MessageType_EOF_:
		j.handleEOFMessage(moneyLaundry, ack, nack)

	default:
		nack()
	}
}

func (j *JoinMicrotransaction) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(
		signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	<-signals
	slog.Info("shutdown signal received")
	j.inputExchange.Close()
	j.resultExchange.Close()
}

func (j *JoinMicrotransaction) handleMicrotransactionMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	batchMsg := moneyLaundry.GetMicrotransactionsBatch()
	clientID := moneyLaundry.GetClientID()
	batchID := moneyLaundry.GetBatchID()
	itemCount := len(batchMsg.GetItems())

	if j.checkpointManager != nil {
		shouldProcess, err := j.checkpointManager.BeginBatch(clientID, batchID, ack)
		if err != nil {
			slog.Error("JoinMicrotransaction: BeginBatch failed", "error", err, "clientID", clientID, "batchID", batchID)
			nack()
			return
		}
		if !shouldProcess {
			return
		}
	} else {
		ack()
	}

	slog.Debug("JoinMicrotransaction: processing batch", "clientID", clientID, "batchID", batchID, "itemCount", itemCount, "totalAccumulated", len(j.results[clientID])+itemCount)

	j.results[clientID] = append(j.results[clientID], batchMsg.GetItems()...)

	if j.checkpointManager != nil {
		if err := j.checkpointManager.CommitBatch(clientID, batchID); err != nil {
			slog.Error("JoinMicrotransaction: CommitBatch failed", "error", err, "clientID", clientID, "batchID", batchID)
		}
	}
}

func (j *JoinMicrotransaction) buildBatcher(clientID string) *batch.Batcher[*protobuf.Microtransaction, *protobuf.MicrotransactionResult] {
	sizer := protowrappers.ProtoSizer[*protobuf.Microtransaction]()
	wrapper := protowrappers.WrapMicrotransactions
	joinBatch := batch.New(j.maxBatchBytes, sizer, wrapper)
	onFlush := func(result *protobuf.MicrotransactionResult, batchID string) error {
		return j.sendBatch(clientID, result)
	}
	return batch.NewBatcher(joinBatch, onFlush)
}

func (j *JoinMicrotransaction) handleEOFMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundry.GetClientID()
	results := j.results[clientID]
	resultCount := len(results)

	slog.Info("JoinMicrotransaction: received EOF", "clientID", clientID, "resultCount", resultCount)

	if j.checkpointManager != nil {
		if err := j.checkpointManager.BeforeEOF(clientID); err != nil {
			slog.Error("JoinMicrotransaction: BeforeEOF failed", "error", err, "clientID", clientID)
			nack()
			return
		}
	}

	batcher := j.buildBatcher(clientID)
	for _, tx := range results {
		if err := batcher.Add(tx); err != nil {
			nack()
			return
		}
	}

	if err := batcher.Flush(); err != nil {
		nack()
		return
	}

	if err := j.sendEOF(clientID); err != nil {
		nack()
		return
	}

	delete(j.results, clientID)

	if j.checkpointManager != nil {
		if err := j.checkpointManager.ClearState(clientID); err != nil {
			slog.Warn("JoinMicrotransaction: failed to clear checkpoint", "error", err, "clientID", clientID)
		} else {
			slog.Debug("JoinMicrotransaction: checkpoint cleared", "clientID", clientID)
		}
	}

	ack()
}

func (j *JoinMicrotransaction) sendBatch(clientID string, batch *protobuf.MicrotransactionResult) error {
	slog.Debug("sending microtransaction batch", "client_id", clientID)
	msg, err := serializer.SerializeProtoMessageWithClientID(clientID, batch, protobuf.MessageType_MICROTRANSACTION_RESULT)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s.%s", j.clientExchangeName, clientID)
	return j.resultExchange.SendWithKey(key, *msg)
}

func (j *JoinMicrotransaction) sendEOF(clientID string) error {
	slog.Info("sending EOF for client", "client_id", clientID)
	eof := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{},
	}

	msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, eof, "")
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s.%s", j.clientExchangeName, clientID)
	return j.resultExchange.SendWithKey(key, msg)
}

func (j *JoinMicrotransaction) GetWorkerName() string {
	return j.workerName
}

func (j *JoinMicrotransaction) GetWorkerID() int {
	return j.workerID
}

func (j *JoinMicrotransaction) GetClientState(clientID string) ([]byte, error) {
	txs, ok := j.results[clientID]
	if !ok {
		return json.Marshal(clientStateCheckpoint{Results: nil})
	}

	checkpoint := make([]*microtransactionCheckpoint, 0, len(txs))
	for _, tx := range txs {
		checkpoint = append(checkpoint, &microtransactionCheckpoint{
			Account:   tx.GetAccount(),
			ToAccount: tx.GetToAccount(),
			Amount:    tx.GetAmount(),
		})
	}

	return json.Marshal(clientStateCheckpoint{Results: checkpoint})
}

func (j *JoinMicrotransaction) LoadClientState(clientID string, data []byte) error {
	var state clientStateCheckpoint
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	j.results[clientID] = make([]*protobuf.Microtransaction, 0, len(state.Results))
	for _, c := range state.Results {
		j.results[clientID] = append(j.results[clientID], &protobuf.Microtransaction{
			Account:   c.Account,
			ToAccount: c.ToAccount,
			Amount:    c.Amount,
		})
	}

	slog.Info("JoinMicrotransaction: loaded client state from checkpoint", "clientID", clientID, "resultCount", len(state.Results))
	return nil
}

func (j *JoinMicrotransaction) ClearClientState(clientID string) error {
	delete(j.results, clientID)
	return nil
}
