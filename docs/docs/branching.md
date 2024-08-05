---
title: "Branching and Tagging"
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

# Branching and Tagging

## Overview

Iceberg table metadata maintains a snapshot log, which represents the changes applied to a table.
Snapshots are fundamental in Iceberg as they are the basis for reader isolation and time travel queries.
For controlling metadata size and storage costs, Iceberg provides snapshot lifecycle management procedures such as [`expire_snapshots`](spark-procedures.md#expire-snapshots) for removing unused snapshots and no longer necessary data files based on table snapshot retention properties.

**For more sophisticated snapshot lifecycle management, Iceberg supports branches and tags which are named references to snapshots with their own independent lifecycles. This lifecycle is controlled by branch and tag level retention policies.** 
Branches are independent lineages of snapshots and point to the head of the lineage. 
Branches and tags have a maximum reference age property which control when the reference to the snapshot itself should be expired.
Branches have retention properties which define the minimum number of snapshots to retain on a branch as well as the maximum age of individual snapshots to retain on the branch. 
These properties are used when the expireSnapshots procedure is run. 
For details on the algorithm for expireSnapshots, refer to the [spec](../../spec.md#snapshot-retention-policy).

## Use Cases

Branching and tagging can be used for handling GDPR requirements and retaining important historical snapshots for auditing.
Branches can also be used as part of data engineering workflows, for enabling experimental branches for testing and validating new jobs.
See below for some examples of how branching and tagging can facilitate these use cases.

### Historical Tags

Tags can be used for retaining important historical snapshots for auditing purposes.

![Historical Tags](assets/images/historical-snapshot-tag.png)

The above diagram demonstrates retaining important historical snapshot with the following retention policy, defined 
via Spark SQL.

1. Retain 1 snapshot per week for 1 month. This can be achieved by tagging the weekly snapshot and setting the tag retention to be a month.
snapshots will be kept, and the branch reference itself will be retained for 1 week. 
```sql
-- Create a tag for the first end of week snapshot. Retain the snapshot for a week
ALTER TABLE prod.db.table CREATE TAG `EOW-01` AS OF VERSION 7 RETAIN 7 DAYS;
```

2. Retain 1 snapshot per month for 6 months. This can be achieved by tagging the monthly snapshot and setting the tag retention to be 6 months.
```sql
-- Create a tag for the first end of month snapshot. Retain the snapshot for 6 months
ALTER TABLE prod.db.table CREATE TAG `EOM-01` AS OF VERSION 30 RETAIN 180 DAYS;
```

3. Retain 1 snapshot per year forever. This can be achieved by tagging the annual snapshot. The default retention for branches and tags is forever.
```sql
-- Create a tag for the end of the year and retain it forever.
ALTER TABLE prod.db.table CREATE TAG `EOY-2023` AS OF VERSION 365;
```

4. Create a temporary "test-branch" which is retained for 7 days and the latest 2 snapshots on the branch are retained.
```sql
-- Create a branch "test-branch" which will be retained for 7 days along with the  latest 2 snapshots
ALTER TABLE prod.db.table CREATE BRANCH `test-branch` RETAIN 7 DAYS WITH SNAPSHOT RETENTION 2 SNAPSHOTS;
```

### Audit Branch

![Audit Branch](assets/images/audit-branch.png)

The above diagram shows an example of using an audit branch for validating a write workflow. 

1. First ensure `write.wap.enabled` is set.
```sql
ALTER TABLE db.table SET TBLPROPERTIES (
    'write.wap.enabled'='true'
);
```
2. Create `audit-branch` starting from snapshot 3, which will be written to and retained for 1 week.
```sql
ALTER TABLE db.table CREATE BRANCH `audit-branch` AS OF VERSION 3 RETAIN 7 DAYS;
```
3. Writes are performed on a separate `audit-branch` independent from the main table history.
```sql
-- WAP Branch write
SET spark.wap.branch = audit-branch
INSERT INTO prod.db.table VALUES (3, 'c');
```
4. A validation workflow can validate (e.g. data quality) the state of `audit-branch`.
5. After validation, the main branch can be `fastForward` to the head of `audit-branch` to update the main table state.
```sql
CALL catalog_name.system.fast_forward('prod.db.table', 'main', 'audit-branch');
```
6. The branch reference will be removed when `expireSnapshots` is run 1 week later.

## Usage 

Creating, querying and writing to branches and tags are supported in the Iceberg Java library, and in Spark and Flink engine integrations.

- [Iceberg Java Library](java-api-quickstart.md#branching-and-tagging)
- [Spark DDLs](spark-ddl.md#branching-and-tagging-ddl)
- [Spark Reads](spark-queries.md#time-travel)
- [Spark Branch Writes](spark-writes.md#writing-to-branches)
- [Flink Reads](flink-queries.md#reading-branches-and-tags-with-SQL)
- [Flink Branch Writes](flink-writes.md#branch-writes)


## Schema selection with branches and tags

It is important to understand that the schema tracked for a table is valid across all branches.
When working with branches, the table's schema is used as that's the schema being validated when writing data to a branch.
On the other hands, querying a tag uses the snapshot's schema, which is the schema id that snapshot pointed to when the snapshot was created.

The below examples show which schema is being used when working with branches.

Create a table and insert some data:

```sql
CREATE TABLE db.table (id bigint, data string, col float);
INSERT INTO db.table values (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0);
SELECT * FROM db.table;
1	a	1.0
2	b	2.0
3	c	3.0
```

Create a branch `test_branch` that points to the current snapshot and read data from the branch:

```sql
ALTER TABLE db.table CREATE BRANCH test_branch;

SELECT * FROM db.table.branch_test_branch;
1	a	1.0
2	b	2.0
3	c	3.0
```

Modify the table's schema by dropping the `col` column and adding a new column named `new_col`:

```sql
ALTER TABLE db.table drop column col;

ALTER TABLE db.table add column new_col date;

INSERT INTO db.table values (4, 'd', date('2024-04-04')), (5, 'e', date('2024-05-05'));

SELECT * FROM db.table;
1	a	NULL
2	b	NULL
3	c	NULL
4	d	2024-04-04
5	e	2024-05-05
```

Querying the head of the branch using one of the below statements will return data using the **table's schema**:

```sql
SELECT * FROM db.table.branch_test_branch;
1	a	NULL
2	b	NULL
3	c	NULL

SELECT * FROM db.table VERSION AS OF 'test_branch';
1	a	NULL
2	b	NULL
3	c	NULL
```

Performing a time travel query using the snapshot id uses the **snapshot's schema**:

```sql

SELECT * FROM db.table.refs;
test_branch	BRANCH	8109744798576441359	NULL	NULL	NULL
main		BRANCH	6910357365743665710	NULL	NULL	NULL


SELECT * FROM db.table VERSION AS OF 8109744798576441359;
1	a	1.0
2	b	2.0
3	c	3.0
```

When writing to the branch, the **table's schema** is used for validation:

```sql

INSERT INTO db.table.branch_test_branch values (6, 'e', date('2024-06-06')), (7, 'g', date('2024-07-07'));

SELECT * FROM db.table.branch_test_branch;
6	e	2024-06-06
7	g	2024-07-07
1	a	NULL
2	b	NULL
3	c	NULL
```
