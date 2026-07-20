package beeremote

import "errors"

var (
	ErrNilConfiguration = errors.New("cannot apply nil configuration")
	ErrInvalidAddress   = errors.New("address provided for BeeGFS Remote is invalid")
	ErrUnableToConnect  = errors.New("unable to setup connection to BeeGFS Remote")
	ErrUnavailable      = errors.New("BeeGFS Remote is unavailable")
)
