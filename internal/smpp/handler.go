package smpp

import (
	"regexp"
	"strings"
)

// DLRReceipt holds the parsed fields from an SMPP delivery receipt.
type DLRReceipt struct {
	MessageID string
	Status    string // DELIVRD, UNDELIV, ACCEPTD, EXPIRED, DELETED, REJECTD
	ErrorCode string
}

// Regex patterns for DLR receipt field extraction.
// Format: id:XXXXXX sub:001 dlvrd:001 submit date:YYMMDDhhmm done date:YYMMDDhhmm stat:DELIVRD err:000 text:...
var (
	dlrIDRegex   = regexp.MustCompile(`id:([^\s]+)`)
	dlrStatRegex = regexp.MustCompile(`stat:([A-Z]{5,7})`)
	dlrErrRegex  = regexp.MustCompile(`err:([^\s]+)`)
)

// ParseDLRReceipt parses a delivery receipt from a deliver_sm short_message body.
// Returns nil if the text does not look like a valid DLR receipt.
func ParseDLRReceipt(text string) *DLRReceipt {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	idMatch := dlrIDRegex.FindStringSubmatch(text)
	if idMatch == nil {
		return nil
	}

	receipt := &DLRReceipt{
		MessageID: idMatch[1],
	}

	statMatch := dlrStatRegex.FindStringSubmatch(text)
	if statMatch != nil {
		receipt.Status = statMatch[1]
	}

	errMatch := dlrErrRegex.FindStringSubmatch(text)
	if errMatch != nil {
		receipt.ErrorCode = errMatch[1]
	}

	return receipt
}

// IsDLR checks if a deliver_sm PDU is a delivery receipt based on the ESM class.
// ESM class bit 2 (0x04) indicates a delivery receipt when set.
func IsDLR(esmClass byte) bool {
	return esmClass&0x04 != 0
}
