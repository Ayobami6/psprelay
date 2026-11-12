// constants.go

// This file defines constants used throughout the application,
//
//	including environment identifiers and default configuration values.
package internal

type PSPProvider string

const (
	Paystack    PSPProvider = "paystack"
	Flutterwave PSPProvider = "flutterwave"
	Monnify     PSPProvider = "monnify"
	Payaza      PSPProvider = "payaza"
	Wema        PSPProvider = "wema"
	Obiex       PSPProvider = "obiex"
)

func IsValidProvider(provider string) bool {
	switch PSPProvider(provider) {
	case Paystack, Flutterwave, Monnify, Payaza, Wema, Obiex:
		return true
	default:
		return false
	}
}
