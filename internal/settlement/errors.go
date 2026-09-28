package settlement

import "errors"

// Settlement errors. On any error the channel state is left unchanged:
// either the whole reservation succeeded, or all earlier legs were released.
var (
	ErrPaymentExists   = errors.New("settlement: payment id already exists")
	ErrPaymentNotFound = errors.New("settlement: payment not found")
	ErrWrongPhase      = errors.New("settlement: payment is not in the required phase")
	ErrBadPreimage     = errors.New("settlement: preimage does not match hash")
	ErrAlreadySettled  = errors.New("settlement: payment already completed")
	ErrNotExpired      = errors.New("settlement: payment lock deadline has not passed")
	ErrConcurrentLimit = errors.New("settlement: too many concurrent payments")
	ErrRouteNotFound   = errors.New("settlement: no route satisfies the constraints")
	ErrFeeBudget       = errors.New("settlement: route fee exceeds fee budget")
	ErrReservationGone = errors.New("settlement: reservation vanished during recovery")
)
