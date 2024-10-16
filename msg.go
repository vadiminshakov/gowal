package gowal

type Msg struct {
	Key   string
	Value []byte
	Idx   uint64
}

func (m Msg) Index() uint64 {
	return m.Idx
}
