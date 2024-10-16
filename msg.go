package gowal

type msg struct {
	Key   string
	Value []byte
	Idx   uint64
}

func (m msg) Index() uint64 {
	return m.Idx
}
