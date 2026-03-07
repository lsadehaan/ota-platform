package executor

import "ota-platform/pkg/gsm0348"

func parseCertificationMode(mode string) gsm0348.CertificationMode {
	switch mode {
	case "NO_SECURITY", "NONE":
		return gsm0348.CertNone
	case "RC":
		return gsm0348.CertRC
	case "CC":
		return gsm0348.CertCC
	case "DS":
		return gsm0348.CertDS
	default:
		return gsm0348.CertNone
	}
}

func parseCounterMode(mode string) gsm0348.CounterMode {
	switch mode {
	case "NO_COUNTER", "NONE":
		return gsm0348.CounterNone
	case "COUNTER_NO_REPLAY":
		return gsm0348.CounterNoReplay
	case "COUNTER_REPLAY_OR_CHECK":
		return gsm0348.CounterReplayCheck
	default:
		return gsm0348.CounterNone
	}
}

func parsePoRMode(mode string) gsm0348.PoRMode {
	switch mode {
	case "NO_REPLY", "NONE":
		return gsm0348.PoRNone
	case "REPLY_ALWAYS":
		return gsm0348.PoRAlways
	case "REPLY_ON_ERROR":
		return gsm0348.PoROnError
	default:
		return gsm0348.PoRNone
	}
}

func parseCipherMode(mode string) gsm0348.CipherMode {
	switch mode {
	case "DES_CBC":
		return gsm0348.CipherDES_CBC
	case "TRIPLE_DES_CBC_2_KEYS":
		return gsm0348.Cipher3DES_CBC_2Keys
	case "TRIPLE_DES_CBC_3_KEYS":
		return gsm0348.Cipher3DES_CBC_3Keys
	case "AES_CBC":
		return gsm0348.CipherAES_CBC
	default:
		return gsm0348.CipherDES_CBC
	}
}

func parseAlgo(algo string) byte {
	switch algo {
	case "DES":
		return 0x01
	case "AES":
		return 0x02
	default:
		return 0x01
	}
}

func parsePoRProtocol(protocol string) byte {
	switch protocol {
	case "SMS_DELIVER_REPORT":
		return 0x01
	case "SMS_SUBMIT":
		return 0x02
	default:
		return 0x02
	}
}

func porStatusDescription(code byte) string {
	switch code {
	case gsm0348.RespOK:
		return "command executed successfully"
	case gsm0348.RespRCCCDSFailed:
		return "RC/CC/DS verification failed"
	case gsm0348.RespCNTRLow:
		return "counter too low"
	case gsm0348.RespCNTRHighNoUpdate:
		return "counter too high, not updated"
	case gsm0348.RespCNTRHighUpdated:
		return "counter too high, updated"
	case gsm0348.RespCNTRBlocked:
		return "counter blocked"
	case gsm0348.RespCryptError:
		return "cryptographic error"
	case gsm0348.RespInsNotSupported:
		return "instruction not supported"
	case gsm0348.RespCNTRIncreaseNotAllowed:
		return "counter increase not allowed"
	case gsm0348.RespTARUnknown:
		return "TAR unknown"
	case gsm0348.RespInsufficientMemory:
		return "insufficient memory"
	default:
		return "unknown status"
	}
}
