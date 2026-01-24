package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
	protocolpb "go.temporal.io/api/protocol/v1"
	"google.golang.org/protobuf/proto"
)

func TestEventMessageIndex(t *testing.T) {
	newMsg := func(id string, eventID int64) *protocolpb.Message {
		return protocolpb.Message_builder{
			Id:      id,
			EventId: proto.Int64(eventID),
		}.Build()
	}
	messages := []*protocolpb.Message{
		newMsg("00", 0),
		newMsg("01", 2),
		newMsg("02", 2),
		newMsg("03", 101),
		newMsg("04", 100),
		newMsg("05", 50),
		newMsg("06", 3),
		newMsg("07", 0),
		newMsg("08", 100),
	}

	emi := indexMessagesByEventID(messages)

	batch := emi.takeLTE(2)
	require.Len(t, batch, 4)
	for i := 0; i < len(batch)-1; i++ {
		if batch[i].GetEventId() == batch[i+1].GetEventId() {
			require.Less(t, batch[i].GetId(), batch[i+1].GetId())
		}
	}

	batch = emi.takeLTE(2)
	require.Empty(t, batch)

	batch = emi.takeLTE(3)
	require.Len(t, batch, 1)

	batch = emi.takeLTE(100)
	require.Len(t, batch, 3)
	for i := 0; i < len(batch)-1; i++ {
		if batch[i].GetEventId() == batch[i+1].GetEventId() {
			require.Less(t, batch[i].GetId(), batch[i+1].GetId())
		}
	}

	emi = indexMessagesByEventID(messages)
	batch = emi.takeLTE(9000)
	require.Len(t, batch, len(messages))
	require.Empty(t, emi)
}
