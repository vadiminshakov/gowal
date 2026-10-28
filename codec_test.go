package gowal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCodecRejectsEmptyFrame(t *testing.T) {
	t.Parallel()

	_, err := codec{}.marshal(nil)
	require.ErrorIs(t, err, errEmptyFrame)

	for _, payload := range [][]byte{{0x90}, {0xc0}} { // empty array and nil
		_, err := codec{}.unmarshal(bytes.NewReader(frameForPayload(payload)))
		require.ErrorIs(t, err, errEmptyFrame)
	}
}

func TestCodecFrameSizeLimit(t *testing.T) {
	t.Parallel()

	// Start with a 16 MiB value. The encoded payload is larger because MessagePack
	// also stores the array header, record fields, key and value length.
	records := []Record{{Index: 1, Key: "key", Value: make([]byte, maxFrameDataSize)}}
	payload, err := msgpack.Marshal(records)
	require.NoError(t, err)
	overhead := len(payload) - len(records[0].Value)
	// Remove the encoding overhead from the value so the entire payload is
	// exactly 16 MiB. The value still uses the same MessagePack length encoding.
	records[0].Value = records[0].Value[:int(maxFrameDataSize)-overhead]

	// A payload exactly at the limit must encode and decode successfully.
	// The frame header and trailer are outside the payload size limit.
	frame, err := codec{}.marshal(records)
	require.NoError(t, err)
	require.Len(t, frame, frameHeaderSize+int(maxFrameDataSize)+frameTrailerSize)
	decoded, err := codec{}.unmarshal(bytes.NewReader(frame))
	require.NoError(t, err)
	require.Equal(t, records, decoded)

	// One more value byte takes the payload over the limit and must be rejected.
	records[0].Value = append(records[0].Value, 0)
	_, err = codec{}.marshal(records)
	require.ErrorIs(t, err, ErrRecordTooLarge)
}

func TestCodecRejectsImpossibleRecordCount(t *testing.T) {
	t.Parallel()

	// Declare billions of records but write only the array header, without records.
	// Decoding must reject the count before allocating memory for those records.
	const declaredRecordCount = 1<<32 - 1
	var payload bytes.Buffer
	encoder := msgpack.NewEncoder(&payload)
	require.NoError(t, encoder.EncodeArrayLen(declaredRecordCount))

	_, err := codec{}.unmarshal(bytes.NewReader(frameForPayload(payload.Bytes())))
	require.ErrorContains(t, err, "invalid frame record count")
}

func TestCodecRejectsTrailingPayloadData(t *testing.T) {
	t.Parallel()

	payload, err := msgpack.Marshal([]Record{NewRecord(1, "key", []byte("value"))})
	require.NoError(t, err)
	payload = append(payload, 0xc0)
	_, err = codec{}.unmarshal(bytes.NewReader(frameForPayload(payload)))
	require.ErrorContains(t, err, "unexpected trailing frame data")
}

func TestScanFramesRejectsIncompleteBatchPayload(t *testing.T) {
	t.Parallel()

	// Write a valid first frame through the production codec. Recovery must keep
	// this frame and its record even when the following batch cannot be decoded.
	before := NewRecord(1, "before", []byte("value"))
	prefix, err := codec{}.marshal([]Record{before})
	require.NoError(t, err)

	// Build the next payload manually: declare two records, but encode only one.
	// The production codec cannot create this mismatch because it derives the
	// array length from the actual records. The decoder will reach EOF while
	// trying to read the missing second record.
	var payload bytes.Buffer
	encoder := msgpack.NewEncoder(&payload)
	require.NoError(t, encoder.EncodeArrayLen(2))
	require.NoError(t, encoder.Encode(NewRecord(2, "batch", []byte("value"))))

	// Give the malformed payload a complete frame header, correct CRC and trailer.
	// This ensures scanning fails inside MessagePack decoding, rather than on
	// a truncated frame or checksum mismatch. Append it after the valid frame.
	data := append(prefix, frameForPayload(payload.Bytes())...)

	// Track the accepted index and every record published by the scanner.
	var seq indexSequence
	var applied []Record
	validSize, err := scanFrames(bytes.NewReader(data), &seq, func(record Record) {
		applied = append(applied, record)
	})

	// EOF inside the batch is an error, not a clean end of the log. Keep only
	// the first frame: even the present record from the broken batch is rejected.
	require.Error(t, err)
	require.Equal(t, int64(len(prefix)), validSize)
	require.Equal(t, uint64(1), seq.last)
	require.Equal(t, []Record{before}, applied)
}

// frameForPayload builds a valid envelope around arbitrary payloads, including
// legacy or invalid payloads that the current encoder must never produce.
func frameForPayload(payload []byte) []byte {
	frame := make([]byte, frameHeaderSize+len(payload)+frameTrailerSize)
	binary.BigEndian.PutUint32(frame[:4], crc32.ChecksumIEEE(payload))
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(payload)))
	copy(frame[frameHeaderSize:], payload)
	binary.BigEndian.PutUint32(frame[len(frame)-frameTrailerSize:], frameTrailer)
	return frame
}
