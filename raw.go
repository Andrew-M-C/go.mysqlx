package mysqlx

// Raw stores a raw MySQL query statement. In mysqlx operation, Raw type will added to sql query statement directly
// without any escaping.
//
// Currently only update fields supports Raw, like:
//
//     map[string]interface{}{"id": mysqlx.Raw("= id")}
type Raw string
