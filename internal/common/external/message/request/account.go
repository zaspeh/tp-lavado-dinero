package request

type Account struct {
	Record string
}

func NewAccount(record string) Account {
	return Account{Record: record}
}

type AccountBatch []Account

func (ab AccountBatch) Handle(handler MessageHandler) error {
	return handler.HandleAccountBatch(ab)
}

func NewAccountBatch(records []Account) AccountBatch {
	return AccountBatch(records)
}
