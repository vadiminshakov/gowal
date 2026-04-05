package gowal

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// getRandomData returns a random byte slice of the given size.
func getRandomData(sizeBytes int) []byte {
	if sizeBytes <= 0 {
		return nil
	}
	letters := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]byte, sizeBytes)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

func prepareBench(b *testing.B, batchSize int) (*Wal, []Record) {
	dir := b.TempDir()

	log, err := NewWAL(Config{
		Dir:              dir,
		Prefix:           "log_",
		SegmentThreshold: 10000,
		MaxSegments:      10000000,
		IsInSyncDiskMode: false,
	})
	require.NoError(b, err)

	valueData := getRandomData(1024)

	records := make([]Record, b.N*batchSize)
	for i := range records {
		records[i] = Record{
			Index: uint64(i),
			Value: valueData,
			Key:   "key" + strconv.Itoa(i),
		}
	}
	return log, records
}

func withMetrics(b *testing.B, batchSize int, f func()) {
	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()

	f()

	elapsed := time.Since(start)
	b.ReportMetric(float64(b.N*batchSize)/elapsed.Seconds(), "records/s")
	b.ReportMetric(elapsed.Seconds(), "sec")
}

func BenchmarkWal_Write(b *testing.B) {
	log, records := prepareBench(b, 1)
	defer func() {
		require.NoError(b, log.Close())
	}()
	withMetrics(b, 1, func() {
		for i := 0; i < b.N; i++ {
			err := log.Write(records[i].Index, records[i].Key, records[i].Value)
			require.NoError(b, err)
		}
	})

}

func benchmarkWalWriteBatch(b *testing.B, batchSize int) {
	log, records := prepareBench(b, batchSize)
	defer func() {
		require.NoError(b, log.Close())
	}()

	withMetrics(b, batchSize, func() {
		for i := 0; i < len(records); i += batchSize {
			end := min(i+batchSize, len(records))
			require.NoError(b, log.WriteBatch(records[i:end]))
		}
	})
}

func BenchmarkWal_WriteBatch10(b *testing.B)    { benchmarkWalWriteBatch(b, 10) }
func BenchmarkWal_WriteBatch100(b *testing.B)   { benchmarkWalWriteBatch(b, 100) }
func BenchmarkWal_WriteBatch1000(b *testing.B)  { benchmarkWalWriteBatch(b, 1000) }
func BenchmarkWal_WriteBatch10000(b *testing.B) { benchmarkWalWriteBatch(b, 10000) }
