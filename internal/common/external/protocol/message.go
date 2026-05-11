package protocol

type Message struct {
	Type     MessageType `json:"type"`
	JobID    string      `json:"job_id"`
	SenderID string      `json:"sender_id"`
	Payload  []byte      `json:"payload"`
}
