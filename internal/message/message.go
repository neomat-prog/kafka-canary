package message

import (
	"encoding/json"
	"time"
)

// Probe = canary payload
type Probe struct {
	ID         string `json:"id"`
	ProducedAt int64  `json:"producedAt"` // unix nanoseconds
}

func New(id string) Probe {
	return Probe{ID: id, ProducedAt: time.Now().UnixNano()}
}

func (p Probe) Encode() ([]byte, error) {
	return json.Marshal(p)
}

func Decode(b []byte) (Probe, error) {
	var p Probe
	err := json.Unmarshal(b, &p)
	return p, err
}

func (p Probe) Latency() time.Duration {
	return time.Since(time.Unix(0, p.ProducedAt))
}
