package types

import "errors"

var (
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrInvalidMarket     = errors.New("invalid market")
	ErrOrderNotFound     = errors.New("order not found")
)
