package finder

type Finder interface {
	Execute(query string) error
	List() [][]byte
	Series() [][]byte
	Abs([]byte) []byte
}
