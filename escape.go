package mysqlx

import "strings"

var (
	stringReplacer = strings.NewReplacer(
		// "'", "''",
		"\n", "\\n",
		"\t", "\\t",
		"\r", "\\r",
		"\b", "\\b",
		"\\", "\\\\",
	)
)

func escapeValueString(s string) string {
	// return s
	return stringReplacer.Replace(s)
}
