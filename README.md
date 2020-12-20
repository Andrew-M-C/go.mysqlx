# mysqlx

[![Build status](https://travis-ci.org/Andrew-M-C/go.mysqlx.svg?branch=master)](https://travis-ci.org/Andrew-M-C/go.mysqlx)
[![report](https://goreportcard.com/badge/github.com/Andrew-M-C/go.mysqlx)](https://goreportcard.com/report/github.com/Andrew-M-C/go.mysqlx)
[![Latest](https://img.shields.io/badge/latest-v0.2.0-orange.svg)](https://github.com/Andrew-M-C/go.mysqlx/tree/v0.2.0)
[![License](https://img.shields.io/badge/license-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

<details>
<summary>More Badages</summary>
<a href="https://coveralls.io/github/Andrew-M-C/go.mysqlx?branch=master&kill_cache=1"><img src="https://coveralls.io/repos/github/Andrew-M-C/go.mysqlx/badge.svg?branch=master"></a>
<a href="https://codebeat.co/projects/github-com-andrew-m-c-go-mysqlx-master"><img src="https://codebeat.co/badges/16fb0b95-56f3-42bf-91dc-ddcef8739b13"></a>
</details>

## Supported Go version

- `Go 1.11`
- `Go 1.12`
- `Go 1.13`

## Usage

`// TODO`

## Supported MySQL data types

- Signed Integers:
  - `bigint(n)`, `int(n)`, `smallint(n)`, `tinyint(n)`
- Unsigned Integers:
  - `bigint(n) unsigned`: Should be configured as `ubigint(n)`
  - `int(n) unsigned`: Should be configured as `uint(n)`
  - `smallint(n) unsigned`: Should be configured as `usmallint(n)`
  - `tinyint(n) unsigned`: Should be configured as `utinyint(n)`
- Date / Time Types:
  - `timestamp`
  - `datetime`, `datetime(n)`
  - `time`, `time(n)`
  - `date`
  - `year`

## Notes

### Error `sql: **** unsupported Scan, storing driver.Value type []uint8 into type *time.Time`

reference: [Stackoverflow](https://stackoverflow.com/questions/45040319/unsupported-scan-storing-driver-value-type-uint8-into-type-time-time)

This is because sqlx does not parse *time.Time by default. Add "`parseTime=true`" parameter then opening MySQL with **sqlx**.

## Changelog

[Click here](./CHANGELOG.md)

## Simple Benchmask Test Result

Please refer to [benchmark test file](./mysqlx_benchmark_test.go). Temporally only `SELECT` is tested. Several conditions among [mysqlx](https://github.com/Andrew-M-C/go.mysqlx), [sqlx](https://github.com/jmoiron/sqlx) and [gorm](https://github.com/jinzhu/gorm)(v1) are tested.

Benchmark test statement: `go test -bench=. -run=none -benchmem -benchtime=10s`. Test table sise: `100000`.

### Select by main key

Select a record by auto-increment main key. This is the most basic way to reading record. We use statements for each package conditions: 

- mysqlx: `db.Select(&res, mysqlx.Condition("id", "=", id))`
- sqlx (with "= 1234"): `db.Select(&res, "SELECT * FROM t_student WHERE id=" + idStr)`
- sqlx (with "= ?"): `db.Select(&res, "SELECT * FROM t_student WHERE id=?", id)`
- gorm (with 'Find' function): `d.Where("id = ?", id+1).Find(&res)`
- gorm (with 'First' function): `d.First(&res, id)`

| Package | nanoseconds/op | bytes/op | allocs/op |
|:---|---:|---:|---:|
| mysqlx | `1,038,348` | `1696` | `37` |
| sqlx (with "= 1234") | `1,115,127` | `1039` | `18` |
| sqlx (with "= ?") | `2,112,185` | `1247` | `26` |
| gorm (with 'Find' function) | `2,256,562` | `6641` | `105` |
| gorm (with 'First' function) | `1,114,290` | `4295` | `97` |

### Select By A VARCHAR Field

One of the `t_student` field is generated by uuid. We use statements for each packages:

- mysqlx: `db.Select(&res, Condition("name", "=", name))`
- sqlx (with "= 'Alice'"): `db.Select(&res, "SELECT * FROM t_student WHERE name='" + name + "'")`
- sqlx (with "= ?"): `db.Select(&res, "SELECT * FROM t_student WHERE name=?", name)`
- gorm (with 'Find' function): `d.Where("name = ?", name).Find(&res)`
- gorm (with 'First' function): `d.Where("name = ?", name).First(&res)`

| Package | nanoseconds/op | bytes/op | allocs/op |
|:---|---:|---:|---:|
| mysqlx | `1,247,630` | `1848` | `37` |
| sqlx (with "= 'Alice'") | `1,146,627` | `1064` | `18` |
| sqlx (with "= ?") | `2,023,415` | `1240` | `25` |
| gorm (with 'Find' function) | `2,073,272` | `6625` | `104` |
| gorm (with 'First' function) | `2,207,229` | `5377` | `116` |
