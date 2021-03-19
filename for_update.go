package mysqlx

// ForUpdateType is returned by ForUpdate()
type ForUpdateType struct{}

// ForUpdate is used in transaction to generate a FOR UPDATE statement section
func ForUpdate() *ForUpdateType {
	return &ForUpdateType{}
}
