package shared

type Tuple []byte

func NewTuple(data string) Tuple {
	return Tuple(data)
}
