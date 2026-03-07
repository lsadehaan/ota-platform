package globalplatform

import "fmt"

// CLA for GlobalPlatform commands
const (
	CLAGlobalPlatform = 0x80
	CLAISO            = 0x00
)

// INS values
const (
	INSInstall = 0xE6
	INSDelete  = 0xE4
	INSLoad    = 0xE8
)

// BuildDeleteAPDU creates a DELETE APDU for removing an applet.
// P2=0x00 for delete object only, P2=0x80 for delete with related objects.
// Data: 4F <len> <AID>
func BuildDeleteAPDU(aid []byte, deleteRelated bool) []byte {
	if len(aid) == 0 {
		panic(fmt.Sprintf("AID must not be empty"))
	}

	p2 := byte(0x00)
	if deleteRelated {
		p2 = 0x80
	}

	// Data field: TLV with tag 0x4F
	data := make([]byte, 0, 2+len(aid))
	data = append(data, 0x4F, byte(len(aid)))
	data = append(data, aid...)

	apdu := make([]byte, 0, 5+len(data))
	apdu = append(apdu, CLAGlobalPlatform, INSDelete, 0x00, p2, byte(len(data)))
	apdu = append(apdu, data...)
	return apdu
}

// BuildInstallForLoadAPDU creates INSTALL [for LOAD] APDU.
// P1=0x02, P2=0x00
// Data: <loadFileAID_len> <loadFileAID> <securityDomainAID_len> <securityDomainAID>
//
//	<loadFileDataBlockHash_len> <loadParamsField_len> <loadToken_len>
func BuildInstallForLoadAPDU(loadFileAID []byte, securityDomainAID []byte) []byte {
	// Build data field
	data := make([]byte, 0, 2+len(loadFileAID)+len(securityDomainAID)+3)

	// Load file AID
	data = append(data, byte(len(loadFileAID)))
	data = append(data, loadFileAID...)

	// Security domain AID
	data = append(data, byte(len(securityDomainAID)))
	data = append(data, securityDomainAID...)

	// Load file data block hash length (0 = no hash)
	data = append(data, 0x00)

	// Load parameters field length (0 = no params)
	data = append(data, 0x00)

	// Load token length (0 = no token)
	data = append(data, 0x00)

	apdu := make([]byte, 0, 5+len(data))
	apdu = append(apdu, CLAGlobalPlatform, INSInstall, 0x02, 0x00, byte(len(data)))
	apdu = append(apdu, data...)
	return apdu
}

// BuildInstallForInstallAPDU creates INSTALL [for INSTALL and MAKE SELECTABLE] APDU.
// P1=0x0C, P2=0x00
// Data: <loadFileAID_len> <loadFileAID> <moduleAID_len> <moduleAID>
//
//	<appAID_len> <appAID> <privilege_len> <privilege>
//	<installParamsField_len> <installParams> <installToken_len>
//
// privilege is typically 1 byte (0x00 default).
// installParams includes STK parameters if provided.
func BuildInstallForInstallAPDU(loadFileAID, moduleAID, appAID []byte, privilege byte, stkParams []byte, installParams []byte) []byte {
	// Build the install parameters field (C9 tag for system-specific params)
	var paramsField []byte
	if len(stkParams) > 0 || len(installParams) > 0 {
		// Combine STK params and install params into the params field
		// The params field uses TLV: C9 <len> <stkParams + installParams>
		inner := make([]byte, 0, len(stkParams)+len(installParams))
		inner = append(inner, stkParams...)
		inner = append(inner, installParams...)

		paramsField = make([]byte, 0, 2+len(inner))
		paramsField = append(paramsField, 0xC9, byte(len(inner)))
		paramsField = append(paramsField, inner...)
	}

	data := make([]byte, 0, 5+len(loadFileAID)+len(moduleAID)+len(appAID)+1+len(paramsField)+1)

	// Load file AID
	data = append(data, byte(len(loadFileAID)))
	data = append(data, loadFileAID...)

	// Module AID
	data = append(data, byte(len(moduleAID)))
	data = append(data, moduleAID...)

	// Application AID
	data = append(data, byte(len(appAID)))
	data = append(data, appAID...)

	// Privileges
	data = append(data, 0x01, privilege)

	// Install parameters field
	data = append(data, byte(len(paramsField)))
	data = append(data, paramsField...)

	// Install token (0 = no token)
	data = append(data, 0x00)

	apdu := make([]byte, 0, 5+len(data))
	apdu = append(apdu, CLAGlobalPlatform, INSInstall, 0x0C, 0x00, byte(len(data)))
	apdu = append(apdu, data...)
	return apdu
}

// BuildLoadBlockAPDUs splits load file data into block APDUs.
// Each block: CLA=0x80, INS=0xE8, P1=0x00 (more blocks) or 0x80 (last block), P2=block_number
// maxBlockSize is the max data per APDU (typically 0xD8 or 0x5A).
func BuildLoadBlockAPDUs(loadFileData []byte, maxBlockSize int) [][]byte {
	if maxBlockSize <= 0 {
		panic(fmt.Sprintf("maxBlockSize must be positive, got %d", maxBlockSize))
	}

	totalBlocks := (len(loadFileData) + maxBlockSize - 1) / maxBlockSize
	if totalBlocks == 0 {
		totalBlocks = 1
	}

	apdus := make([][]byte, 0, totalBlocks)
	offset := 0
	blockNum := 0

	for offset < len(loadFileData) {
		end := offset + maxBlockSize
		if end > len(loadFileData) {
			end = len(loadFileData)
		}
		chunk := loadFileData[offset:end]

		// P1: 0x00 for intermediate blocks, 0x80 for last block
		p1 := byte(0x00)
		if end >= len(loadFileData) {
			p1 = 0x80
		}

		apdu := make([]byte, 0, 5+len(chunk))
		apdu = append(apdu, CLAGlobalPlatform, INSLoad, p1, byte(blockNum), byte(len(chunk)))
		apdu = append(apdu, chunk...)
		apdus = append(apdus, apdu)

		offset = end
		blockNum++
	}

	return apdus
}

// BuildSTKParameters builds the C9 tag TLV for STK/UICC toolkit parameters.
// Contains menu entries, access domain, priority etc.
func BuildSTKParameters(maxMenuItems int, menuIDPositions []byte, accessDomain []byte, priorityLevel byte, maxTimers byte, maxMenuEntryLength byte, maxChannels byte) []byte {
	// Build the parameter body per GP Amendment C
	// Format: various tags for STK parameters
	body := make([]byte, 0, 32)

	// Toolkit parameters tag (already inside C9)
	// Menu entry related parameters
	body = append(body, byte(maxMenuItems))

	// Menu ID positions
	body = append(body, byte(len(menuIDPositions)))
	body = append(body, menuIDPositions...)

	// Access domain
	body = append(body, byte(len(accessDomain)))
	body = append(body, accessDomain...)

	// Priority level
	body = append(body, priorityLevel)

	// Max timers
	body = append(body, maxTimers)

	// Max menu entry text length
	body = append(body, maxMenuEntryLength)

	// Max channels
	body = append(body, maxChannels)

	// Wrap in C9 TLV
	result := make([]byte, 0, 2+len(body))
	result = append(result, 0xC9, byte(len(body)))
	result = append(result, body...)
	return result
}
