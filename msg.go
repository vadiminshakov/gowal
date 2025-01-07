package gowal

type msg struct {
	Idx   uint64
	Key   string
	Value []byte
}

func (m msg) Index() uint64 {
	return m.Idx
}
