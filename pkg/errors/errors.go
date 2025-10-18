package errors

import "errors"

var (
	ErrRetryable  = errors.New("retryable")
	ErrConflict   = errors.New("conflict")
	ErrNotFound   = errors.New("not found")
	ErrNilContext = errors.New("nil context")
)
