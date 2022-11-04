package errs

type ErrorWithCode struct {
	err  string
	Code int // error code
}

func NewErrorWithCode(err string, code int) error {
	return &ErrorWithCode{err, code}
}

func (e *ErrorWithCode) Error() string { return e.err }
