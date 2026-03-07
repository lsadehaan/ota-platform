package migrations

import (
	"embed"
	"io/fs"
	"sort"
	"strings"
)

// Files contains the embedded SQL migrations.
//
//go:embed *.sql
var Files embed.FS

func UpFiles() ([]string, error) {
	entries, err := fs.ReadDir(Files, ".")
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".up.sql") {
			files = append(files, name)
		}
	}
	sort.Strings(files)
	return files, nil
}
