package protocol

type MessageType string

const (
	TypeTransaction MessageType = "transaction"
	TypeEOF         MessageType = "eof"
	TypeResult      MessageType = "result"
)
