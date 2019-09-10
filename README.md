# mysqlx

[![Build status](https://travis-ci.org/Andrew-M-C/go.mysqlx.svg?branch=master)](https://travis-ci.org/Andrew-M-C/go.mysqlx)  [![Coverage Status](https://coveralls.io/repos/github/Andrew-M-C/go.mysqlx/badge.svg?branch=master)](https://coveralls.io/github/Andrew-M-C/go.mysqlx?branch=master)

**Note:** This repo is still under development.

## Supported MySQL data types

- Signed Integers:
  - `bigint(n)`, `int(n)`, `smallint(n)`, `tinyint(n)`
- Unsigned Integers:
  - `bigint(n) unsigned`: Should be configured as `ubigint(n)`
  - `int(n) unsigned`: Should be configured as `uint(n)`
  - `smallint(n) unsigned`: Should be configured as `usmallint(n)`
  - `tinyint(n) unsigned`: Should be configured as `utinyint(n)`
