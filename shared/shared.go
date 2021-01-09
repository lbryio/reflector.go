package shared

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
)

type BlobStack struct {
	Timing     time.Duration `json:"timing"`
	OriginName string        `json:"origin_name"`
	HostName   string        `json:"host_name"`
}
type BlobTrace struct {
	Stacks []BlobStack `json:"stacks"`
}

var hostName *string

func getHostName() string {
	if hostName == nil {
		hn, err := os.Hostname()
		if err != nil {
			hn = "unknown"
		}
		hostName = &hn
	}
	return *hostName
}
func (b *BlobTrace) Stack(timing time.Duration, originName string) BlobTrace {
	b.Stacks = append(b.Stacks, BlobStack{
		Timing:     timing,
		OriginName: originName,
		HostName:   getHostName(),
	})
	return *b
}
func (b *BlobTrace) Merge(otherTrance BlobTrace) BlobTrace {
	b.Stacks = append(b.Stacks, otherTrance.Stacks...)
	return *b
}
func NewBlobTrace(timing time.Duration, originName string) BlobTrace {
	b := BlobTrace{}
	b.Stacks = append(b.Stacks, BlobStack{
		Timing:     timing,
		OriginName: originName,
		HostName:   getHostName(),
	})
	return b
}

func (b BlobTrace) String() string {
	var fullTrace string
	for i, stack := range b.Stacks {
		delta := time.Duration(0)
		if i > 0 {
			delta = stack.Timing - b.Stacks[i-1].Timing
		}
		fullTrace += fmt.Sprintf("[%d](%s) origin: %s - timing: %s - delta: %s\n", i, stack.HostName, stack.OriginName, stack.Timing.String(), delta.String())
	}
	return fullTrace
}

func (b BlobTrace) Serialize() (string, error) {
	t, err := json.Marshal(b)
	if err != nil {
		return "", errors.Err(err)
	}
	return string(t), nil
}

func Deserialize(serializedData string) (*BlobTrace, error) {
	var trace BlobTrace
	err := json.Unmarshal([]byte(serializedData), &trace)
	if err != nil {
		return nil, errors.Err(err)
	}
	return &trace, nil
}
