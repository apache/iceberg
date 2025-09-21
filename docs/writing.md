<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->




# Writing Data

PyIceberg lets you write Arrow tables to Iceberg with a few clear operations: **append**, **overwrite**, **delete**, **partial overwrite** (via `overwrite_filter`), **dynamic partition overwrite**, and **upsert**. Examples below use `pyarrow.Table` and the `Table` API. See the API page for more. 

## Prerequisites
```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog

catalog = load_catalog("default")  # configure this for your environment
```

---

## 1) Table API
Use when you want a straightforward, single-operation write.

### Append
```python
from pyiceberg.table import Table
tbl = Table.load("...")

with tbl.transaction() as tx:
    tx.append(df)  # or tbl.append(df) for direct append
# Commit is implicit for Table.append; explicit within transactions
```

### Overwrite
```python
tbl.overwrite(df)
```

### Delete
```python
tbl.delete(delete_filter="city == 'Paris'")
```

### Partial Overwrites (`overwrite_filter`)
```python
from pyiceberg.expressions import EqualTo

new_df = pa.Table.from_pylist([{"city": "New York", "lat": 40.7128, "long": 74.0060}])
tbl.overwrite(new_df, overwrite_filter=EqualTo("city", "Paris"))
```

### Dynamic Partition Overwrite
```python
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DoubleType, NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField, IdentityTransform

schema = Schema(
    NestedField(1, "city", StringType()),
    NestedField(2, "lat", DoubleType()),
    NestedField(3, "long", DoubleType()),
)

tbl = catalog.create_table(
    "default.cities_v2",
    schema=schema,
    partition_spec=PartitionSpec(
        PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="city_identity")
    ),
)

# seed data
seed = pa.Table.from_pylist([
    {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
    {"city": "Paris", "lat": 48.864716, "long": 2.349014},
])
tbl.append(seed)

# correct the Paris partition only
fix = pa.Table.from_pylist([{"city": "Paris", "lat": 48.864716, "long": 2.349014}])
tbl.dynamic_partition_overwrite(fix)
```

### Upsert
```python
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType, NestedField

schema = Schema(
    NestedField(1, "city", StringType(), required=True),
    NestedField(2, "inhabitants", IntegerType(), required=True),
    identifier_field_ids=[1],   # city is the identifier
)

tbl = catalog.create_table("default.cities_upsert", schema=schema)

initial = pa.Table.from_pylist([
    {"city": "Amsterdam", "inhabitants": 921402},
    {"city": "Paris", "inhabitants": 2103000},
])
tbl.append(initial)

changes = pa.Table.from_pylist([
    {"city": "Paris", "inhabitants": 2103000},   # unchanged, ignored
    {"city": "Drachten", "inhabitants": 45505},  # new, inserted
])
result = tbl.upsert(changes)
print(result.rows_updated, result.rows_inserted)
```

---

## 2) Transaction API
Use when you want to group multiple changes into a single atomic commit.

```python
with tbl.transaction() as tx:
    tx.append(df_new)
    tx.delete(where="status = 'old'")
    tx.overwrite(df_fix, overwrite_filter="id in (1,2,3)")
    tx.commit()
```

---

## Tips & Pitfalls
- Arrow first: create writes from `pyarrow.Table` to avoid schema mismatches.  
- Filters: `overwrite_filter` scopes the delete portion of an overwrite; use precise predicates.  
- Partition awareness: prefer `dynamic_partition_overwrite` to surgically replace partitions.  
- Identifier fields: define them to use upsert correctly.  

---

## See also
- API: write / delete / partial overwrite / dynamic partition overwrite / upsert.
