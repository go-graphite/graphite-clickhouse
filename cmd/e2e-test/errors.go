package main

import "errors"

var (
	ErrTimestampInvalid = errors.New("invalid timestamp")
	ErrNoTest           = errors.New("no test section")
	ErrNoSetDir         = errors.New("dir not set")
)
