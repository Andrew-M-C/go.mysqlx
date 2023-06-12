package mysqlx

var internal = struct {
	debugf func(string, ...any)
}{
	debugf: func(s string, a ...any) {},
}
