package protocol

type EOF struct {
	JobID   string `json:"job_id"`
	Source  string `json:"source"`
	QueryID string `json:"query_id,omitempty"`
}
