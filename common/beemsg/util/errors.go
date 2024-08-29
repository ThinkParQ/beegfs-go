package util

import "errors"

var (
	ErrBeeMsgWrite = errors.New("error writing BeeMsg")
	ErrBeeMsgRead  = errors.New("error reading BeeMsg")
)
