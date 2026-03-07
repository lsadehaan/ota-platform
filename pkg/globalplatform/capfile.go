package globalplatform

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
)

// CAP file section order per GP spec
var sectionOrder = []string{
	"Header", "Directory", "Import", "Applet",
	"Class", "Method", "StaticField", "Export",
	"ConstantPool", "RefLocation",
}

// CAPFile represents a parsed JavaCard CAP file
type CAPFile struct {
	Filename string
	AID      []byte            // extracted from Header component
	Sections map[string][]byte // section name -> raw bytes
	LoadFile []byte            // assembled load file (sections in order)
}

// ParseCAPFile reads a CAP file (ZIP format) and extracts all components.
// Each section is a .cap file inside the ZIP under javacard/ directory.
func ParseCAPFile(data []byte, filename string) (*CAPFile, error) {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to open CAP file as ZIP: %w", err)
	}

	capFile := &CAPFile{
		Filename: filename,
		Sections: make(map[string][]byte),
	}

	for _, file := range reader.File {
		// CAP sections are .cap files, typically under a javacard/ path
		name := file.Name
		if !strings.HasSuffix(strings.ToLower(name), ".cap") {
			continue
		}

		// Extract section name from filename
		// e.g., "com/example/applet/javacard/Header.cap" -> "Header"
		baseName := name
		if idx := strings.LastIndex(baseName, "/"); idx >= 0 {
			baseName = baseName[idx+1:]
		}
		sectionName := strings.TrimSuffix(baseName, ".cap")

		rc, err := file.Open()
		if err != nil {
			return nil, fmt.Errorf("failed to open section %s: %w", sectionName, err)
		}

		sectionData, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read section %s: %w", sectionName, err)
		}

		capFile.Sections[sectionName] = sectionData
	}

	if len(capFile.Sections) == 0 {
		return nil, fmt.Errorf("no CAP sections found in %s", filename)
	}

	// Extract AID from Header component
	capFile.AID = capFile.ExtractAID()

	// Assemble the load file
	capFile.LoadFile = capFile.AssembleLoadFile()

	return capFile, nil
}

// AssembleLoadFile concatenates sections in the correct GlobalPlatform order.
func (c *CAPFile) AssembleLoadFile() []byte {
	// First, collect sections in spec order
	orderedSections := make([]string, 0, len(c.Sections))
	inOrder := make(map[string]bool)

	for _, name := range sectionOrder {
		if _, exists := c.Sections[name]; exists {
			orderedSections = append(orderedSections, name)
			inOrder[name] = true
		}
	}

	// Append any remaining sections not in the standard order (sorted alphabetically)
	var extra []string
	for name := range c.Sections {
		if !inOrder[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(extra)
	orderedSections = append(orderedSections, extra...)

	// Calculate total size
	totalSize := 0
	for _, name := range orderedSections {
		totalSize += len(c.Sections[name])
	}

	// Concatenate
	loadFile := make([]byte, 0, totalSize)
	for _, name := range orderedSections {
		loadFile = append(loadFile, c.Sections[name]...)
	}

	return loadFile
}

// ExtractAID extracts the AID from the Header component.
// Header component format: tag(1) + size(2) + magic(4) + minor(1) + major(1) + flags(1) +
//
//	package info: minor(1) + major(1) + AID_length(1) + AID(variable)
func (c *CAPFile) ExtractAID() []byte {
	header, exists := c.Sections["Header"]
	if !exists || len(header) < 13 {
		return nil
	}

	// Skip: tag(1) + size(2) + magic(4) + minor(1) + major(1) + flags(1)
	// = offset 10
	// Package info: minor(1) + major(1) + AID_length(1)
	// = offset 12 is AID_length
	offset := 10
	if offset+2 >= len(header) {
		return nil
	}

	// Package version: minor + major
	offset += 2 // skip minor(1) + major(1)

	if offset >= len(header) {
		return nil
	}

	aidLen := int(header[offset])
	offset++

	if offset+aidLen > len(header) {
		return nil
	}

	aid := make([]byte, aidLen)
	copy(aid, header[offset:offset+aidLen])
	return aid
}

// SectionSizes returns a map of section name to size for display purposes.
func (c *CAPFile) SectionSizes() map[string]int {
	sizes := make(map[string]int, len(c.Sections))
	for name, data := range c.Sections {
		sizes[name] = len(data)
	}
	return sizes
}
