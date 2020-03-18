package mysqlx

import (
	"strings"
)

var (
	stringReplacer = strings.NewReplacer(
		"'", "''",
	)
)

func escapeValueString(s string) string {
	return s
	// return stringReplacer.Replace(s)
}
