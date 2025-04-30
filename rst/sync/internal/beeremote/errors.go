package beeremote

import "errors"

var (
	ErrNilConfiguration          = errors.New("cannot apply nil configuration")
	ErrInvalidAddress            = errors.New("address provided for BeeRemote is invalid")
	ErrUnableToConnect           = errors.New("unable to setup connection to BeeRemote")
	ErrUnableToUpdateGlobalFlags = errors.New("unable to update ctl global flags")
)
