package errs

import "fmt"

type ErrorWithCode struct {
	err  string
	Code int // error code
}

func NewErrorWithCode(err string, code int) error {
	return ErrorWithCode{err, code}
}

func NewErrorfWithCode(code int, f string, args ...interface{}) error {
	return ErrorWithCode{fmt.Sprintf(f, args...), code}
}

func (e ErrorWithCode) Error() string { return e.err }
