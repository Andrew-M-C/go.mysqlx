package mysqlx

import (
	"math/rand"
	"strconv"
	"testing"

	atomicbool "github.com/Andrew-M-C/go.atomicbool"
	// _ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
)

type Student struct {
	ID     int32  `gorm:"primary_key"    db:"id"     mysqlx:"increment:true"`
	Name   string `gorm:"column:name"    db:"name"   mysqlx:"type:varchar(255)"`
	Gender int32  `gorm:"column:gender"  db:"gender"`
	Grade  uint32 `gorm:"column:grade"   db:"grade"`
	Class  string `gorm:"column:class"   db:"class"  mysqlx:"type:char(8)"`
}

func (Student) Options() Options {
	return Options{
		TableName: "t_student",
	}
}

func (Student) TableName() string {
	return "t_student"
}

// go test -bench=. -run=none -benchmem

var lcDataInserted atomicbool.B

const cDataLines = 1000

func BenchmarkSelectMysqlx(b *testing.B) {
	// 首先建表，并且插入数据。这里不测试建表耗时
	d, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		b.Errorf("Open failed: %v", err)
		return
	}

	// 建表、建数据
	if lcDataInserted.CompareAndSwap(false, true) {
		d.Sqlx().Exec("DROP TABLE `t_student`")
		d.MustCreateTable(Student{})

		for i := 0; i < cDataLines; i++ {
			d.Insert(Student{
				Name:   strconv.Itoa(i),
				Gender: int32(i & 0x1),
				Grade:  uint32(i & 0x7),
				Class:  string('A' + byte(i&0xF)),
			})
		}
	}

	// 开始测试
	var res []*Student
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := rand.Int31n(cDataLines)
		d.Select(&res, Cond{"id", "=", id + 1})
	}

	return
}

func BenchmarkSelectGorm(b *testing.B) {
	d, err := gorm.Open("mysql", "travis@/db_test?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		b.Errorf("open gorm erro: %v", err)
	}
	defer d.Close()

	// 开始测试
	var res []*Student
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := rand.Int31n(cDataLines)
		d.Where("id = ?", id+1).Find(&res)
	}
}
