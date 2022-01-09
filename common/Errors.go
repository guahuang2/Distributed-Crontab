package common

import "errors"

var (
	ERR_LOCK_ALREADY_REQUIRED = errors.New("Lock has been used")

	ERR_NO_LOCAL_IP_FOUND = errors.New("IP address not found")
)
