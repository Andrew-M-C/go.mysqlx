package mysqlx

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

/* reference:
SELECT ORDINAL_POSITION as No,
    COLUMN_NAME as `Field`,
    COLUMN_TYPE as `Type`,
    IS_NULLABLE as `Nullable`,
    COLUMN_DEFAULT as `Default`,
    COLUMN_KEY as `Key`,
    COLUMN_COMMENT as `Comment`,
    EXTRA as `Extra`
FROM information_schema.columns
WHERE table_schema='db_test' and table_name='t_room_bill_binding' ORDER BY ORDINAL_POSITION;
*/

type _Field struct {
	// No       int            `db:"No"`
	Field    string         `db:"Field"`
	Type     string         `db:"Type"`
	Nullable string         `db:"Null"`
	Default  sql.NullString `db:"Default"`
	Key      string         `db:"Key"`
	Extra    string         `db:"Extra"`
	// Comment  string         `db:"Comment"`
}

// const _ReadTableFields = `
// SELECT ORDINAL_POSITION as ` + "`No`" + `,
// 	COLUMN_NAME as ` + "`Field`" + `,
// 	COLUMN_TYPE as ` + "`Type`" + `,
// 	IS_NULLABLE as ` + "`Null`" + `,
// 	COLUMN_DEFAULT as ` + "`Default`" + `,
// 	COLUMN_KEY as ` + "`Key`" + `,
// 	EXTRA as ` + "`Extra`" + `,
// 	COLUMN_COMMENT as ` + "`Comment`" + `
// FROM ` + "`columns`" + `
// WHERE table_schema='%s' and table_name='%s' ORDER BY ORDINAL_POSITION;
// `

// const _ReadTableFields = "desc `%s`"

const _ReadTableFields = `SELECT
	COLUMN_NAME as ` + "`Field`" + `,
	COLUMN_TYPE as ` + "`Type`" + `,
	IS_NULLABLE as ` + "`Null`" + `,
	COLUMN_DEFAULT as ` + "`Default`" + `,
	COLUMN_KEY as ` + "`Key`" + `,
	EXTRA as ` + "`Extra`" + `
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY ORDINAL_POSITION`

// ReadTableFields returns all fields in given table
func (d *DB) ReadTableFields(table string) (ret []*Field, err error) {
	if nil == d.db {
		return nil, fmt.Errorf("mysqlx not initialized")
	}
	if "" == table {
		return nil, fmt.Errorf("empty table name")
	}

	database := d.param.DBName
	if "" == database {
		var err error
		database, err = d.CurrentDatabase()
		if err != nil {
			return nil, err
		}
	}

	// query := fmt.Sprintf(_ReadTableFields, table)
	query := fmt.Sprintf(_ReadTableFields, database, table)
	var fields []*_Field
	err = d.db.Select(&fields, query)
	if err != nil {
		return nil, err
	}
	if nil == fields || 0 == len(fields) {
		// return make([]*Field, 0), nil
		return nil, fmt.Errorf("Table '%s.%s' doesn't exist", database, table)
	}

	ret = make([]*Field, 0, len(fields))
	for _, f := range fields {
		retF := Field{
			Name: f.Field,
			Type: f.Type,
			// Comment: f.Comment,
		}
		// nullable
		switch strings.ToUpper(f.Nullable) {
		case "YES", "TRUE":
			retF.Nullable = true
		default:
			retF.Nullable = false
		}
		// Default
		if f.Default.Valid {
			if strings.Contains(f.Type, "char") || strings.Contains(f.Type, "text") {
				retF.Default = "'" + strings.Replace(f.Default.String, "'", "\\'", -1) + "'"
			} else {
				retF.Default = f.Default.String
			}
		} else {
			retF.Default = "NULL"
		}
		// auto_increment
		if strings.Contains(f.Extra, "auto_increment") {
			retF.AutoIncrement = true
		}
		// append
		ret = append(ret, &retF)
	}

	return
}

type currDB struct {
	Database string `db:"database()"`
}

// CurrentDatabase gets current operating database
func (d *DB) CurrentDatabase() (string, error) {
	var res []currDB
	err := d.db.Select(&res, "select database()")
	if err != nil {
		return "", err
	}
	if nil == res || 0 == len(res) {
		return "", fmt.Errorf("current database unknown")
	}
	return res[0].Database, nil
}

// ========
type _Index struct {
	Table      string `db:"TABLE_NAME"`
	NonUnique  int    `db:"NON_UNIQUE"`
	KeyName    string `db:"INDEX_NAME"`
	SeqInIndex int    `db:"SEQ_IN_INDEX"`
	ColumnName string `db:"COLUMN_NAME"`
	// Collation    string         `db:"Collation"`
	// Cardinality  string         `db:"Cardinality"`
	// SubPart      sql.NullInt64  `db:"Sub_part"`
	// Packed       sql.NullString `db:"Packed"`
	Null sql.NullString `db:"NULLABLE"`
	// IndexType    string         `db:"Index_type"`
	// Comment      string         `db:"Comment"`
	// IndexComment string         `db:"Index_comment"`
	// Visible      string         `db:"Visible"`
	// Expression   sql.NullString `db:"Expression"`
}

// const _ReadTableIndexes = "show index from `%s`"
const _ReadTableIndexes = "SELECT TABLE_NAME, NON_UNIQUE, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NULLABLE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'"

// ReadTableIndexes returns all indexes and uniques of given table name
func (d *DB) ReadTableIndexes(table string) (map[string]*Index, map[string]*Unique, error) {
	if nil == d.db {
		return nil, nil, fmt.Errorf("mysqlx not initialized")
	}
	if "" == table {
		return nil, nil, fmt.Errorf("empty table name")
	}

	database := d.param.DBName
	if "" == database {
		var err error
		database, err = d.CurrentDatabase()
		if err != nil {
			return nil, nil, err
		}
	}

	var err error
	var indexes []*_Index

	query := fmt.Sprintf(_ReadTableIndexes, database, table)
	err = d.db.Select(&indexes, query)
	if err != nil {
		return nil, nil, err
	}

	indexMap := make(map[string]*Index)
	uniqueMap := make(map[string]*Unique)
	if nil == indexes || 0 == len(indexes) {
		return indexMap, uniqueMap, nil
	}

	for _, idx := range indexes {
		if strings.ToUpper(idx.KeyName) == "PRIMARY" {
			continue
		}
		if idx.NonUnique > 0 {
			// Jut a normal index
			index, exist := indexMap[idx.KeyName]
			if false == exist {
				index = &Index{
					Name:   idx.KeyName,
					Fields: make([]string, 0),
				}
				indexMap[idx.KeyName] = index
			}
			index.Fields = append(index.Fields, idx.ColumnName)
		} else {
			// unique
			unique, exist := uniqueMap[idx.KeyName]
			if false == exist {
				unique = &Unique{
					Name:   idx.KeyName,
					Fields: make([]string, 0),
				}
				uniqueMap[idx.KeyName] = unique
			}
			unique.Fields = append(unique.Fields, idx.ColumnName)
		}
	}

	return indexMap, uniqueMap, nil
}

// ReadStructFields returns all valid SQL fields by given structure and will buffer it
func (d *DB) ReadStructFields(s interface{}) (ret []*Field, err error) {
	// read from buffer
	intfName := reflect.TypeOf(s)
	fieldValue, exist := d.bufferedFields.Load(intfName)
	if exist {
		return fieldValue.([]*Field), nil
	}

	// read now
	ret, err = ReadStructFields(s)
	if err != nil {
		d.bufferedFields.Store(intfName, ret)
	}
	return
}

// StructFields is the same as ReadStructFields
func (*DB) StructFields(s interface{}) (ret []*Field, err error) {
	return ReadStructFields(s)
}

// StructFields returns all valid SQL fields by given structure
func StructFields(s interface{}) (ret []*Field, err error) {
	return ReadStructFields(s)
}

// ReadStructFields is the same as StructFields
func ReadStructFields(s interface{}) (ret []*Field, err error) {
	t := reflect.TypeOf(s)
	v := reflect.ValueOf(s)

	switch t.Kind() {
	case reflect.Ptr:
		return ReadStructFields(t.Elem())
	case reflect.Struct:
		// OK, continue
	default:
		err = fmt.Errorf("invalid type: %v", t.Kind())
		return
	}

	return readStructFields(t, v)
}
