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
		"\032", "\\Z",
		// "\"", "\\\"",
		// "'", "\\'",
	)
	likeStringReplacer = strings.NewReplacer(
		// "'", "''",
		"\n", "\\n",
		"\t", "\\t",
		"\r", "\\r",
		"\b", "\\b",
		"\\", "\\\\",
		"\032", "\\Z",
		"%", "\\%",
		// "\"", "\\\"",
		// "'", "\\'",
	)
)

func escapeValueString(s string) string {
	// return s
	return stringReplacer.Replace(s)
}

func escapeLikeValueString(s string) string {
	// return s
	return likeStringReplacer.Replace(s)
}
