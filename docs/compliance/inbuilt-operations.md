# Inbuilt Operations Coverage

## Implemented
- Inbuilt operation/function parsing and evaluation pipeline is available and integrated into core execution paths.
- Inbuilt usage is supported in:
  - `SELECT` projection-only mode (no `FROM`)
  - relation and join projections
  - `CASE` projection branches (`THEN`/`ELSE`)
  - `WHERE` expressions where parser/evaluator routes through supported function handling
  - mutation assignments (`UPDATE`/`INSERT` expression evaluation paths)
  - subquery projections used by `IN`/scalar/`EXISTS` style predicates where applicable
- Runtime function argument binding supports column-aware lookup (qualified and unqualified forms when available).
- Inbuilt runtime context includes database/user/session metadata and last-insert-id related context fields.

## Supported Function Table

The table below lists the currently registered inbuilt SQL functions. It reflects the implemented registry surface, including aliases. Registry presence does not by itself claim full MySQL 8 behavioral parity in every query context.

| Function Area | Registry | Query Interface | Test Coverage | MySQL 8 Parity | Registered Functions | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Geo | Supported | Supported | Partial | Partial | `DISTANCE` | DistDB-specific geo support; not a broad MySQL spatial parity claim |
| String | Supported | Supported | Supported | Partial | `ASCII`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, `CONCAT`, `CONCAT_W`, `CONCAT_WS`, `FIELD`, `FIND_IN_SET`, `FORMAT`, `INSERT`, `INSTR`, `LEFT`, `LENGTH`, `LOCATE`, `LOWER`, `LCASE`, `LPAD`, `LTRIM`, `MID`, `POSITION`, `REPEAT`, `REPLACE`, `REVERSE`, `RIGHT`, `RPAD`, `RTRIM`, `SPACE`, `SUBSTR`, `SUBSTRING`, `SUBSTRING_INDEX`, `TRIM`, `UPPER`, `UCASE` | Includes MySQL-style aliases where registered |
| Datetime / Time | Supported | Supported | Supported | Partial | `ADDDATE`, `ADDTIME`, `CURDATE`, `CURRENT_DATE`, `CURTIME`, `CURRENT_TIME`, `DATE`, `DATE_ADD`, `DATEDIFF`, `DATE_FORMAT`, `DATE_SUB`, `DAY`, `DAYNAME`, `DAYOFMONTH`, `DAYOFWEEK`, `DAYOFYEAR`, `EXTRACT`, `FROM_DAYS`, `HOUR`, `LAST_DAY`, `LOCALTIME`, `LOCALTIMESTAMP`, `MAKEDATE`, `MAKETIME`, `MICROSECOND`, `MINUTE`, `MONTH`, `NOW`, `PERIOD_ADD`, `PERIOD_DIFF`, `QUARTER`, `SECOND`, `SEC_TO_TIME`, `STR_TO_DATE`, `SUBDATE`, `SUBTIME`, `SYSDATE`, `TIME`, `TIME_FORMAT`, `TIME_TO_SEC`, `TIMEDIFF`, `TIMESTAMP`, `TO_DAYS`, `UNIXTIMESTAMP`, `UNIX_TIMESTAMP`, `WEEK`, `WEEKDAY`, `WEEKOFYEAR`, `YEAR`, `YEARWEEK` | Registry and tests cover the listed datetime set; parity should still be treated as implementation-defined rather than certified |
| Numeric / Math / Aggregate | Supported | Supported | Supported | Partial | `ABS`, `ACOS`, `ASIN`, `ATAN`, `ATAN2`, `AVG`, `CEIL`, `CEILING`, `COS`, `COUNT`, `COT`, `DEGREES`, `DIV`, `EXP`, `FLOOR`, `GREATEST`, `LEAST`, `LN`, `LOG`, `LOG10`, `LOG2`, `MAX`, `MIN`, `MOD`, `PI`, `POW`, `POWER`, `RADIANS`, `RAND`, `ROUND`, `SIGN`, `SIN`, `SQRT`, `SUM`, `TAN`, `TRUNCATE` | Includes aggregate-like evaluator entries currently routed through the same registry |
| Advanced / Conditional / Context | Supported | Supported | Supported | Partial | `BIN`, `BINARY`, `CASE`, `CAST`, `COALESCE`, `CONNECTION_ID`, `CONV`, `CONVERT`, `CURRENT_USER`, `DATABASE`, `IF`, `IFNULL`, `ISNULL`, `LAST_INSERT_ID`, `NULLIF`, `SESSION_USER`, `SYSTEM_USER`, `USER`, `VERSION` | Mix of conditional, conversion, and session/context functions |
| Custom non-MySQL | Supported | Supported | Partial | Not Applicable | `LOOKUP` | DistDB extension, not part of MySQL 8 built-in function surface |

## Integration Status Table

This table describes how the inbuilt function subsystem is integrated into the platform, separately from whether it has full MySQL 8 parity.

| Integration Area | Status | Current State | Notes |
| --- | --- | --- | --- |
| Registry implementation | Supported | Inbuilt function registry/evaluator is implemented in serverlib | Backed by resolver/indexer code |
| Registry access | Supported | Registered inbuilt names are now exposed through a public accessor | Enables code-driven documentation and tooling |
| Query interface access | Supported | Registered inbuilt functions are callable through supported SQL query/expression paths | Applies where execution routes through current evaluator |
| Projection integration | Supported | Works in projection-only, relation, and join projection paths | Includes normal `SELECT` projection usage |
| Filter/condition integration | Partial | Supported where `WHERE`/expression evaluation routes through implemented function handling | Not a blanket claim for every MySQL expression surface |
| Mutation-expression integration | Supported | Supported in current `UPDATE` and relevant `INSERT` expression paths | Limited by implemented resolver paths |
| CASE integration | Supported | Inbuilt functions can be evaluated in `CASE` branches | Applies to current expression engine |
| Runtime context integration | Supported | Current database/user/session/connection/last-insert-id context is available to supported functions | Only functions that explicitly use runtime context benefit |
| Test coverage | Supported | Registry and evaluator behavior are covered by targeted tests | Coverage exists, but is not yet a full conformance suite |
| MySQL 8 parity | Partial | Implemented subset, not full built-in function parity | Should not be presented as complete MySQL 8 coverage |

## Practical Notes

- Inbuilt functions can be used in the currently supported execution paths for `SELECT`, `CASE`, `WHERE`, mutation expressions, and relevant subquery projections.
- Support should be treated as function-by-function rather than category-complete.
- If a MySQL built-in is not listed above, assume it is not yet supported unless separately documented or covered by tests.

## Gaps
- Only the implemented inbuilt function set is supported; this is not full MySQL built-in parity.
- Some advanced expression combinations are still limited by current parser/execution constraints.
- User-defined SQL functions are supported through the current scalar lifecycle (`CREATE FUNCTION`, `DROP FUNCTION`, and query-time execution), but full MySQL UDF parity is not implemented.
- UDF storage currently reuses the shared routine catalog model used by stored procedures instead of a distinct function catalog object type.
