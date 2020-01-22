package mysqlx

import "strings"

// And packages conditions with AND logic. Only Cond, *Cond, Or, And types are acceptable in Conds slice.
type And []interface{}

func (and And) pack(fieldMap map[string]*Field) string {
	statements := make([]string, 0, len(and))
	for _, v := range and {
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

	return "(" + strings.Join(statements, " AND ") + ")"
}
