package gowal

type Log interface {
	Set(index uint64, key string, value []byte) error
	Get(index uint64) (string, []byte, bool)
	Close() error
}
