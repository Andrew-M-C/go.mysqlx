# mysqlx

[![Build status](https://travis-ci.org/Andrew-M-C/go.mysqlx.svg?branch=master)](https://travis-ci.org/Andrew-M-C/go.mysqlx)  [![Coverage Status](https://coveralls.io/repos/github/Andrew-M-C/go.mysqlx/badge.svg?branch=master)](https://coveralls.io/github/Andrew-M-C/go.mysqlx?branch=master)  [![Status](https://img.shields.io/badge/status-develop-yellow.svg)](https://github.com/Andrew-M-C/go.mysqlx)  [![Codebeat](https://codebeat.co/badges/16fb0b95-56f3-42bf-91dc-ddcef8739b13)](https://codebeat.co/projects/github-com-andrew-m-c-go-mysqlx-master)  [![License](https://img.shields.io/badge/license-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

## Supported Go version

- `Go 1.11`
- `Go 1.12`
- `Go 1.13`

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
