package mysqlx

import (
	"database/sql"
	"fmt"
	"log"
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

// const _READ_TABLE_FIELDS = `
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

const _READ_TABLE_FIELDS = "desc `%s`"

func (d *DB) ReadTableFields(table string) (ret []*Field, err error) {
	if nil == d.db {
		return nil, fmt.Errorf("mysqlx not initialized")
	}
	if "" == table {
		return nil, fmt.Errorf("empty table name")
	}

	query := fmt.Sprintf(_READ_TABLE_FIELDS, table)
	var fields []*_Field
	err = d.db.Select(&fields, query)
	if err != nil {
		return nil, err
	}
	if nil == fields || 0 == len(fields) {
		return make([]*Field, 0), nil
	}

	ret = make([]*Field, 0, len(fields))
	for _, f := range fields {
		ret_f := Field{
			Name: f.Field,
			Type: f.Type,
			// Comment: f.Comment,
		}
		// nullable
		switch strings.ToUpper(f.Nullable) {
		case "YES", "TRUE":
			ret_f.Nullable = true
		default:
			ret_f.Nullable = false
		}
		// Default
		if f.Default.Valid {
			if strings.Contains(f.Type, "char") || strings.Contains(f.Type, "text") {
				ret_f.Default = "'" + strings.Replace(f.Default.String, "'", "\\'", -1) + "'"
			} else {
				ret_f.Default = f.Default.String
			}
		} else {
			ret_f.Default = "NULL"
		}
		// auto_increment
		if strings.Contains(f.Extra, "auto_increment") {
			ret_f.AutoIncrement = true
		}
		// append
		ret = append(ret, &ret_f)
	}

	return
}

// ========
type _Index struct {
	Table        string         `db:"Table"`
	NonUnique    int            `db:"Non_unique"`
	KeyName      string         `db:"Key_name"`
	SqlInIndex   int            `db:"Seq_in_index"`
	ColumnName   string         `db:"Column_name"`
	Collation    string         `db:"Collation"`
	Cardinality  string         `db:"Cardinality"`
	SubPart      sql.NullInt64  `db:"Sub_part"`
	Packed       sql.NullString `db:"Packed"`
	Null         sql.NullString `db:"Null"`
	IndexType    string         `db:"Index_type"`
	Comment      string         `db:"Comment"`
	IndexComment string         `db:"Index_comment"`
}

const _READ_TABLE_INDEXES = "show index from `%s`"

func (d *DB) ReadTableIndexes(table string) (map[string]*Index, map[string]*Unique, error) {
	if nil == d.db {
		return nil, nil, fmt.Errorf("mysqlx not initialized")
	}
	if "" == table {
		return nil, nil, fmt.Errorf("empty table name")
	}

	var err error
	var indexes []*_Index

	query := fmt.Sprintf(_READ_TABLE_INDEXES, table)
	err = d.db.Select(&indexes, query)
	if err != nil {
		return nil, nil, err
	}

	index_map := make(map[string]*Index)
	unique_map := make(map[string]*Unique)
	if nil == indexes || 0 == len(indexes) {
		return index_map, unique_map, nil
	}

	for _, idx := range indexes {
		if strings.ToUpper(idx.KeyName) == "PRIMARY" {
			continue
		}
		if idx.NonUnique > 0 {
			// Jut a normal index
			index, exist := index_map[idx.KeyName]
			if false == exist {
				index = &Index{
					Name:   idx.KeyName,
					Fields: make([]string, 0),
				}
				index_map[idx.KeyName] = index
			}
			index.Fields = append(index.Fields, idx.ColumnName)
		} else {
			// unique
			unique, exist := unique_map[idx.KeyName]
			if false == exist {
				unique = &Unique{
					Name:   idx.KeyName,
					Fields: make([]string, 0),
				}
				unique_map[idx.KeyName] = unique
			}
			unique.Fields = append(unique.Fields, idx.ColumnName)
		}
	}

	return index_map, unique_map, nil
}

func (_ *DB) ReadStructFields(s interface{}) (ret []*Field, err error) {
	return ReadStructFields(s)
}

func (_ *DB) StructFields(s interface{}) (ret []*Field, err error) {
	return ReadStructFields(s)
}

func StructFields(s interface{}) (ret []*Field, err error) {
	return ReadStructFields(s)
}

func ReadStructFields(s interface{}) (ret []*Field, err error) {
	t := reflect.TypeOf(s)
	v := reflect.ValueOf(s)

	log.Println("interface type: ", reflect.TypeOf(s))

	switch t.Kind() {
	case reflect.Ptr:
		return ReadStructFields(t.Elem())
	case reflect.Struct:
		// OK, continue
	default:
		err = fmt.Errorf("invalid type: %v", t.Kind())
		return
	}

	log.Printf("detail: %+v\n", s)
	return readKVFields(t, v)
}
