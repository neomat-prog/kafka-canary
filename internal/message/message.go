package message

import (
	"encoding/json"
	"time"
)

// Probe = canary payload
type Probe struct {
	ID         string `json:"id"`
	Seq        int64  `json:"seq"`
	ProducedAt int64  `json:"producedAt"`
}

func New(id string) Probe {
	return Probe{ID: id, Seq: 0, ProducedAt: time.Now().UnixNano()}
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
