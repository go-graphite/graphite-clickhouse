package finder

type Finder interface {
	Execute(query string) error
	List() [][]byte
	Abs([]byte) []byte
}
