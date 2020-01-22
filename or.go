package mysqlx

import (
	"strings"
)

// Or packages conditions with OR logic. Only Cond, *Cond, Or, And types are acceptable in the slice.
type Or []interface{}

func (or Or) pack(fieldMap map[string]*Field) string {
	statements := make([]string, 0, len(or))
	for _, v := range or {
		s := ""

		switch v.(type) {
		case Cond:
			c := v.(Cond)
			s = c.pack(fieldMap)
		case *Cond:
			s = v.(*Cond).pack(fieldMap)
		case Or:
			s = v.(Or).pack(fieldMap)
		case And:
			s = v.(And).pack(fieldMap)
		default:
			continue
		}

		if s != "" {
			statements = append(statements, s)
		}
	}

	if 0 == len(statements) {
		return ""
	}

	return "(" + strings.Join(statements, " OR ") + ")"
}
