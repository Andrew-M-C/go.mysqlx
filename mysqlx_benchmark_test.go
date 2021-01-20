package mysqlx

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	atomicbool "github.com/Andrew-M-C/go.atomicbool"
	// _ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
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
		Indexes: []Index{
			{
				Name:   "i_name",
				Fields: []string{"name"},
			},
		},
	}
}

func (Student) TableName() string {
	return "t_student"
}

// go test -bench=. -run=none -benchmem -benchtime=10s

var (
	lcDataInserted atomicbool.B
	studentNames   = []string{}
	rnd            = rand.New(rand.NewSource(time.Now().UnixNano()))
)

const cDataLines = 100000

func prepareData(d DB) {
	// insert data for testing
	if !lcDataInserted.CompareAndSwap(false, true) {
		return
	}

	d.Sqlx().Exec("DROP TABLE `t_student`")
	d.MustCreateTable(Student{})

	students := make([]*Student, 0, cDataLines)

	for i := 0; i < cDataLines; i++ {
		name := uuid.New().String()
		studentNames = append(studentNames, name)

		students = append(students, &Student{
			Name:   name,
			Gender: int32(i & 0x1),
			Grade:  uint32(i & 0x7),
			Class:  string('A' + byte(i&0xF)),
		})
	}

	d.InsertMany(students)
	return
}

func randName() string {
	i := rnd.Int31n(cDataLines)
	return studentNames[int(i)]
}

func Benchmark_Mysqlx_Select(b *testing.B) {
	// create table firstly.
	d, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		b.Errorf("Open failed: %v", err)
		return
	}

	prepareData(d)

	// start testing
	var res []*Student
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := rand.Int31n(cDataLines)
		d.Select(&res, Condition("id", "=", id+1))
	}

	return
}

func Benchmark_Mysqlx_SelectByVarchar(b *testing.B) {
	// create table firstly.
	d, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		b.Errorf("Open failed: %v", err)
		return
	}

	// start testing
	var res []*Student
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n := randName()
		d.Select(&res, Condition("name", "=", n))
	}

	return
}

func Benchmark_Sqlx_Select(b *testing.B) {
	d, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		b.Errorf("Open failed: %v", err)
		return
	}

	var res []*Student
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := rand.Int31n(cDataLines)
		d.Sqlx().Select(&res, "SELECT * FROM t_student WHERE id = ?", id)
	}

	return
}

func Benchmark_Sqlx_SelectWithoutQ(b *testing.B) {
	d, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		b.Errorf("Open failed: %v", err)
		return
	}

	var res []*Student
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := rand.Int31n(cDataLines)
		idStr := strconv.Itoa(int(id))
		d.Sqlx().Select(&res, "SELECT * FROM t_student WHERE id = "+idStr)
	}

	return
}

func Benchmark_Sqlx_SelectByVarhar(b *testing.B) {
	d, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		b.Errorf("Open failed: %v", err)
		return
	}

	var res []*Student
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n := randName()
		d.Sqlx().Select(&res, "SELECT * FROM t_student WHERE name = ?", n)
	}

	return
}

func Benchmark_Sqlx_SelectByVarharWithoutQ(b *testing.B) {
	d, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		b.Errorf("Open failed: %v", err)
		return
	}

	var res []*Student
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n := randName()
		d.Sqlx().Select(&res, "SELECT * FROM t_student WHERE name = '"+n+"'")
	}

	return
}

func Benchmark_Gorm_SelectWhere(b *testing.B) {
	d, err := gorm.Open("mysql", "travis@/db_test?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		b.Errorf("open gorm erro: %v", err)
	}
	defer d.Close()

	var res []*Student
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := rand.Int31n(cDataLines)
		d.Where("id = ?", id+1).Find(&res)
	}
}

func Benchmark_Gorm_SelectByWarcharWhere(b *testing.B) {
	d, err := gorm.Open("mysql", "travis@/db_test?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		b.Errorf("open gorm erro: %v", err)
	}
	defer d.Close()

	var res []*Student
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n := randName()
		d.Where("name = ?", n).Find(&res)
	}
}

func Benchmark_Gorm_SelectFirst(b *testing.B) {
	d, err := gorm.Open("mysql", "travis@/db_test?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		b.Errorf("open gorm erro: %v", err)
	}
	defer d.Close()

	var res Student
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := rand.Int31n(cDataLines)
		d.First(&res, id)
	}
}

func Benchmark_Gorm_SelectByVarcharFirst(b *testing.B) {
	d, err := gorm.Open("mysql", "travis@/db_test?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		b.Errorf("open gorm erro: %v", err)
	}
	defer d.Close()

	var res Student
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n := randName()
		d.Where("name = ?", n).First(&res)
	}
}
