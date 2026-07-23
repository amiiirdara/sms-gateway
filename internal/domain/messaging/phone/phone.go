// Package phone validates and normalizes recipient numbers.
// Accepted formats: E.164 (+989121234567) and local Iranian mobile (09121234567).
package phone

import (
	"errors"
	"regexp"
	"strings"
)

var (
	e164IR    = regexp.MustCompile(`^\+989\d{9}$`)
	localIR   = regexp.MustCompile(`^09\d{9}$`)
	ErrInvalid = errors.New("invalid phone number: expected +989xxxxxxxxx or 09xxxxxxxxx")
)

// Normalize accepts E.164 or local Iranian mobile and returns canonical E.164.
func Normalize(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "-", "")
	if e164IR.MatchString(s) {
		return s, nil
	}
	if localIR.MatchString(s) {
		return "+98" + s[1:], nil
	}
	return "", ErrInvalid
}
