package mysqlx

import "fmt"

// Order is for MySQL ORDER BY statement
type Order struct {
	Param string
	Seq   string
}

func (o *Order) pack() string {
	if o.Param == "" {
		return ""
	}

	return fmt.Sprintf("`%s` %s", o.Param, o.Seq)
}
