# Changelog

[<-- To README]

## v0.2.0

- Supports MySQL `IN` conditions.
- Supports nested conditions.
- Supports MySQL `OR`.
- Now prototypes parameter could either be a struct or a pointer to struct.
- Cancel go 1.1 in CI test.
- Defaultly use `charset=utfmb4` instead of `utf8`.

## v0.3.0

- Supports customizing options in creating table.
- Add Condition() function to replace the direct using of Cond{} struct.
- Add InsertOnDuplicateKeyUpdate() to support `ON DUPLICATE KEY UPDATE ...` operation.
- Add DoNotExec option. This is quite useful for troubleshoting or debugging `mysqlx`.
- Add InsertMany() to support batch records inserting.
- Support LIKE in Cond{} by Like() function.
- Support `ON UPDATE CURRENT_TIMESTAMP` when creating table.
- Cancel support to go 1.12!
