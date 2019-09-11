package mysqlx

import "strings"

func SelectStructFields(s interface{}) (string, error) {
	fields, err := ReadStructFields(s)
	if err != nil {
		return "", err
	}

	field_names := make([]string, 0, len(fields))
	for _, f := range fields {
		field_names = append(field_names, "`"+f.Name+"`")
	}

	return strings.Join(field_names, ", "), nil
}

func (_ *DB) SelectStructFields(s interface{}) (string, error) {
	return SelectStructFields(s)
}
