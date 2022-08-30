package client

import "strconv"

type HttpError struct {
	statusCode int
	message    string
}

func NewHttpError(statusCode int, message string) *HttpError {
	return &HttpError{
		statusCode: statusCode,
		message:    message,
	}
}

func (e *HttpError) Error() string {
	return strconv.Itoa(e.statusCode) + ": " + e.message
}
