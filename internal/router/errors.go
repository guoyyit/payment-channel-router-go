package router

import "errors"

// Routing errors.
var (
	ErrNoRoute       = errors.New("router: no feasible route")
	ErrFeeExceeded   = errors.New("router: fee exceeds maximum allowed fee")
	ErrInvalidAmount = errors.New("router: payment amount must be positive")
	ErrSameNode      = errors.New("router: sender and receiver must differ")
)
