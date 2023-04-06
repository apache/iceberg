---
hide:
  - navigation
---

<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Python CLI

Pyiceberg comes with a CLI that's available after installing the `pyiceberg` package.

You can pass the path to the Catalog using the `--uri` and `--credential` argument, but it is recommended to setup a `~/.pyiceberg.yaml` config as described in the [Catalog](configuration.md) section.

```sh
➜  pyiceberg --help
Usage: pyiceberg [OPTIONS] COMMAND [ARGS]...

Options:
--catalog TEXT
--verbose BOOLEAN
--output [text|json]
--uri TEXT
--credential TEXT
--help                Show this message and exit.

Commands:
describe    Describes a namespace xor table
drop        Operations to drop a namespace or table
list        Lists tables or namespaces
location    Returns the location of the table
properties  Properties on tables/namespaces
rename      Renames a table
schema      Gets the schema of the table
spec        Returns the partition spec of the table
uuid        Returns the UUID of the table
```

This example assumes that you have a default catalog set. If you want to load another catalog, for example, the rest example above. Then you need to set `--catalog rest`.

```sh
➜  pyiceberg list
default
nyc
```

```sh
➜  pyiceberg list nyc
nyc.taxis
```

```sh
➜  pyiceberg describe nyc.taxis
Table format version  1
Metadata location     file:/.../nyc.db/taxis/metadata/00000-aa3a3eac-ea08-4255-b890-383a64a94e42.metadata.json
Table UUID            6cdfda33-bfa3-48a7-a09e-7abb462e3460
Last Updated          1661783158061
Partition spec        []
Sort order            []
Current schema        Schema, id=0
├── 1: VendorID: optional long
├── 2: tpep_pickup_datetime: optional timestamptz
├── 3: tpep_dropoff_datetime: optional timestamptz
├── 4: passenger_count: optional double
├── 5: trip_distance: optional double
├── 6: RatecodeID: optional double
├── 7: store_and_fwd_flag: optional string
├── 8: PULocationID: optional long
├── 9: DOLocationID: optional long
├── 10: payment_type: optional long
├── 11: fare_amount: optional double
├── 12: extra: optional double
├── 13: mta_tax: optional double
├── 14: tip_amount: optional double
├── 15: tolls_amount: optional double
├── 16: improvement_surcharge: optional double
├── 17: total_amount: optional double
├── 18: congestion_surcharge: optional double
└── 19: airport_fee: optional double
Current snapshot      Operation.APPEND: id=5937117119577207079, schema_id=0
Snapshots             Snapshots
└── Snapshot 5937117119577207079, schema 0: file:/.../nyc.db/taxis/metadata/snap-5937117119577207079-1-94656c4f-4c66-4600-a4ca-f30377300527.avro
Properties            owner                 root
write.format.default  parquet
```

Or output in JSON for automation:

```sh
➜  pyiceberg --output json describe nyc.taxis | jq
{
  "identifier": [
    "nyc",
    "taxis"
  ],
  "metadata_location": "file:/.../nyc.db/taxis/metadata/00000-aa3a3eac-ea08-4255-b890-383a64a94e42.metadata.json",
  "metadata": {
    "location": "file:/.../nyc.db/taxis",
    "table-uuid": "6cdfda33-bfa3-48a7-a09e-7abb462e3460",
    "last-updated-ms": 1661783158061,
    "last-column-id": 19,
    "schemas": [
      {
        "type": "struct",
        "fields": [
          {
            "id": 1,
            "name": "VendorID",
            "type": "long",
            "required": false
          },
...
          {
            "id": 19,
            "name": "airport_fee",
            "type": "double",
            "required": false
          }
        ],
        "schema-id": 0,
        "identifier-field-ids": []
      }
    ],
    "current-schema-id": 0,
    "partition-specs": [
      {
        "spec-id": 0,
        "fields": []
      }
    ],
    "default-spec-id": 0,
    "last-partition-id": 999,
    "properties": {
      "owner": "root",
      "write.format.default": "parquet"
    },
    "current-snapshot-id": 5937117119577207000,
    "snapshots": [
      {
        "snapshot-id": 5937117119577207000,
        "timestamp-ms": 1661783158061,
        "manifest-list": "file:/.../nyc.db/taxis/metadata/snap-5937117119577207079-1-94656c4f-4c66-4600-a4ca-f30377300527.avro",
        "summary": {
          "operation": "append",
          "spark.app.id": "local-1661783139151",
          "added-data-files": "1",
          "added-records": "2979431",
          "added-files-size": "46600777",
          "changed-partition-count": "1",
          "total-records": "2979431",
          "total-files-size": "46600777",
          "total-data-files": "1",
          "total-delete-files": "0",
          "total-position-deletes": "0",
          "total-equality-deletes": "0"
        },
        "schema-id": 0
      }
    ],
    "snapshot-log": [
      {
        "snapshot-id": "5937117119577207079",
        "timestamp-ms": 1661783158061
      }
    ],
    "metadata-log": [],
    "sort-orders": [
      {
        "order-id": 0,
        "fields": []
      }
    ],
    "default-sort-order-id": 0,
    "refs": {
      "main": {
        "snapshot-id": 5937117119577207000,
        "type": "branch"
      }
    },
    "format-version": 1,
    "schema": {
      "type": "struct",
      "fields": [
        {
          "id": 1,
          "name": "VendorID",
          "type": "long",
          "required": false
        },
...
        {
          "id": 19,
          "name": "airport_fee",
          "type": "double",
          "required": false
        }
      ],
      "schema-id": 0,
      "identifier-field-ids": []
    },
    "partition-spec": []
  }
}
```
