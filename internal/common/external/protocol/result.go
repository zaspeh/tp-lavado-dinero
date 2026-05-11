package protocol

type Result struct {
	JobID string `json:"job_id"`
	Query string `json:"query"`
	Data  []byte `json:"data"`
}
