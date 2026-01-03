package shared

import (
	"testing"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/util"

	"github.com/stretchr/testify/assert"
)

func TestBlobTrace_Serialize(t *testing.T) {
	hostName = util.PtrToString("test_machine")
	stack := NewBlobTrace(10*time.Second, "test")
	stack.Stack(20*time.Second, "test2")
	stack.Stack(30*time.Second, "test3")
	serialized, err := stack.Serialize()
	assert.NoError(t, err)

	deserialized, err := Deserialize(serialized)
	assert.NoError(t, err)
	assert.Len(t, deserialized.Stacks, 3)
	assert.Equal(t, 10*time.Second, deserialized.Stacks[0].Timing)
	assert.Equal(t, "test", deserialized.Stacks[0].OriginName)
	assert.Equal(t, "test_machine", deserialized.Stacks[0].HostName)
	assert.Equal(t, 20*time.Second, deserialized.Stacks[1].Timing)
	assert.Equal(t, "test2", deserialized.Stacks[1].OriginName)
	assert.Equal(t, 30*time.Second, deserialized.Stacks[2].Timing)
	assert.Equal(t, "test3", deserialized.Stacks[2].OriginName)
}

func TestBlobTrace_Deserialize(t *testing.T) {
	hostName = util.PtrToString("test_machine")
	serialized := "{\"stacks\":[{\"timing\":10000000000,\"origin_name\":\"test\"},{\"timing\":20000000000,\"origin_name\":\"test2\"},{\"timing\":30000000000,\"origin_name\":\"test3\"}]}"
	stack, err := Deserialize(serialized)
	assert.NoError(t, err)
	assert.Len(t, stack.Stacks, 3)
	assert.Equal(t, stack.Stacks[0].Timing, 10*time.Second)
	assert.Equal(t, stack.Stacks[1].Timing, 20*time.Second)
	assert.Equal(t, stack.Stacks[2].Timing, 30*time.Second)
	assert.Equal(t, stack.Stacks[0].OriginName, "test")
	assert.Equal(t, stack.Stacks[1].OriginName, "test2")
	assert.Equal(t, stack.Stacks[2].OriginName, "test3")
}
