---
title: "Functions"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

## Spark SQL Functions for Iceberg Transforms

Iceberg provides Spark SQL functions that expose internal metadata and transformation capabilities useful for working with Iceberg tables.

### `system.iceberg_version()`

Returns the Iceberg library version at runtime.

#### Description
Returns the current version of Iceberg used in the Spark runtime. This is useful for debugging, validation, or auditing purposes where it's necessary to track the version of Iceberg being used.

#### Usage

```sql
SELECT system.iceberg_version();
```

#### Returns
A `STRING` value representing the Iceberg version, e.g., `"1.5.0"`.

#### Example

```sql
SELECT system.iceberg_version();
-- Result: 1.5.0
```

---

### `system.bucket(num_buckets, value)`

Returns the bucket number for a given value, using the same hash logic as Iceberg's `bucket` partition transform.

---

#### Description
Computes a deterministic bucket number for the given value, compatible with Iceberg table partitioning. This is useful when validating partition logic or computing bucket values externally (e.g., in ETL pipelines).

#### Arguments

- `num_buckets`: Positive integer indicating the number of buckets.
- `value`: A primitive type (e.g., `INT`, `STRING`, `BIGINT`).

#### Usage

```sql
SELECT system.bucket(num_buckets, column_value);
```

#### Parameters

* `num_buckets`: The total number of buckets (positive integer).
* `column_value`: Any supported primitive type (e.g., `INT`, `STRING`) to be hashed.

#### Returns
An `INT` between `0` and `num_buckets - 1`.

#### Example

```sql
SELECT system.bucket(8, 'user_id_123');
-- Result: 3
```

### `system.years(value)`

Returns the number of years since the Unix epoch (1970) for a given date or timestamp.

#### Description

Calculates `year(value) - 1970`. This is consistent with the Iceberg `years` partition transform, and can be used to compute partition keys manually.

#### Arguments

- `value`: A `DATE`, `TIMESTAMP`, or `TIMESTAMP_NTZ`. Other types will result in an error.

#### Returns

An `INT` representing the number of years from 1970. If the input is `NULL`, the result is also `NULL`.

#### Examples

```sql
SELECT system.years(DATE '2017-12-01');
-- Result: 47

SELECT system.years(TIMESTAMP '1969-12-31 23:59:58');
-- Result: -1

SELECT system.years(TIMESTAMP '1970-01-01 00:00:01');
-- Result: 0

SELECT system.years(NULL);
-- Result: NULL
```

### `system.months(value)`

Returns the number of months since the Unix epoch (1970-01-01) for a given date or timestamp.

#### Description

Calculates `12 * (year(value) - 1970) + month(value) - 1`. This is consistent with the Iceberg `months` partition transform and can be used to compute partition keys manually.

#### Arguments

- `value`: A `DATE`, `TIMESTAMP`, or `TIMESTAMP_NTZ`. Other types will result in an error.

#### Returns

An `INT` representing the number of months from January 1970. If the input is `NULL`, the result is also `NULL`.

#### Examples

```sql
SELECT system.months(DATE '2017-12-01');
-- Result: 575

SELECT system.months(TIMESTAMP '1969-12-31 23:59:58');
-- Result: -1

SELECT system.months(TIMESTAMP '1970-01-01 00:00:01');
-- Result: 0

SELECT system.months(NULL);
-- Result: NULL
```

### `system.days(value)`

Returns the number of days since the Unix epoch (1970-01-01) for a given date or timestamp.

#### Description

Calculates the number of days between `value` and 1970-01-01. This is consistent with the Iceberg `days` partition transform, and can be used to compute partition keys manually.

#### Arguments

- `value`: A `DATE`, `TIMESTAMP`, or `TIMESTAMP_NTZ`. Other types will result in an error.

#### Returns

An `INT` representing the number of days from 1970-01-01. If the input is `NULL`, the result is also `NULL`.

#### Examples

```sql
SELECT system.days(DATE '2017-12-01');
-- Result: 17596

SELECT system.days(TIMESTAMP '1969-12-31 23:59:59');
-- Result: -1

SELECT system.days(TIMESTAMP '1970-01-01 00:00:01');
-- Result: 0

SELECT system.days(NULL);
-- Result: NULL
```

### `system.hours(value)`

Returns the number of hours since the Unix epoch (1970-01-01 00:00:00 UTC) for a given timestamp.

#### Description

Computes the number of whole hours between `value` and 1970-01-01 00:00:00 UTC. Follows the semantics of the Iceberg `hours` partition transform and is intended for computing partition keys.

#### Arguments

- `value`: A `TIMESTAMP` or `TIMESTAMP_NTZ`. Other types are not supported and will result in an error.

#### Returns

An `INT` representing the number of hours since the epoch. Returns `NULL` if the input is `NULL`.

#### Examples

```sql
SELECT system.hours(TIMESTAMP '2017-12-01 10:12:55.038194 UTC+00:00');
-- Result: 420034

SELECT system.hours(TIMESTAMP '1970-01-01 00:00:01.000001 UTC+00:00');
-- Result: 0

SELECT system.hours(TIMESTAMP '1969-12-31 23:59:58.999999 UTC+00:00');
-- Result: -1

SELECT system.hours(NULL::TIMESTAMP);
-- Result: NULL

SELECT system.hours(TIMESTAMP_NTZ '2017-12-01 10:12:55.038194 UTC');
-- Result: 420034
```

### `system.truncate(width, value)`

Truncates a given value to a multiple of the specified width.

#### Description

Returns the result of truncating the input `value` to a multiple of `width`. The behavior depends on the input type:

- For numeric types, it returns the nearest multiple of `width` less than or equal to `value`.
- For strings, it returns the first `width` characters based on Unicode code points.
- For binary values, it returns the first `width` bytes.
- **For decimal values, the effective truncation unit is determined by applying the decimal scale to the width.**  
  **For example, with scale 2 and width 10, the truncation unit is 0.10.**  
  **The result preserves the original decimal scale.**

If `value` is `NULL`, the result is `NULL`.

#### Arguments

- `width`: An `INT` specifying the width for truncation. Must be positive.
- `value`: A supported value of type `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `DECIMAL`, `STRING`, `VARCHAR`, `CHAR`, or `BINARY`.

#### Returns

A value of the same type as the input `value`, truncated according to `width`.

#### Examples

```sql
-- Numeric values
SELECT system.truncate(10, 34);
-- Result: 30

SELECT system.truncate(10, -11);
-- Result: -20

-- Decimal values
-- width=10, scale=2 → unit=0.10, result is 12.30
SELECT system.truncate(10, DECIMAL '12.34');
-- Result: 12.30

-- width=3, scale=2 → unit=0.03, result is 0.03
SELECT system.truncate(3, DECIMAL '0.05');
-- Result: 0.03

-- String values
SELECT system.truncate(5, 'abcdefg');
-- Result: 'abcde'

SELECT system.truncate(2, 'イロハニホヘト');
-- Result: 'イロ'

SELECT system.truncate(4, '测试raul试测');
-- Result: '测试ra'

-- Binary values
SELECT system.truncate(3, X'1234567890');
-- Result: X'123456'

-- NULL values
SELECT system.truncate(10, NULL);
-- Result: NULL
```