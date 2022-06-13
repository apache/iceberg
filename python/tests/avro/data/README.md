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

# Avro tests

This directory contains a manifest file for running tests against.

## Data

```bash
avro-tools tojson tests/avro/manifest.avro  | head -n
```

```json
{
	"status": 1,
	"snapshot_id": {
		"long": 8744736658442914487
	},
	"data_file": {
		"file_path": "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet",
		"file_format": "PARQUET",
		"partition": {
			"VendorID": null
		},
		"record_count": 19513,
		"file_size_in_bytes": 388872,
		"block_size_in_bytes": 67108864,
		"column_sizes": {
			"array": [{
				"key": 1,
				"value": 53
			}, {
				"key": 2,
				"value": 98153
			}, {
				"key": 3,
				"value": 98693
			}, {
				"key": 4,
				"value": 53
			}, {
				"key": 5,
				"value": 53
			}, {
				"key": 6,
				"value": 53
			}, {
				"key": 7,
				"value": 17425
			}, {
				"key": 8,
				"value": 18528
			}, {
				"key": 9,
				"value": 53
			}, {
				"key": 10,
				"value": 44788
			}, {
				"key": 11,
				"value": 35571
			}, {
				"key": 12,
				"value": 53
			}, {
				"key": 13,
				"value": 1243
			}, {
				"key": 14,
				"value": 2355
			}, {
				"key": 15,
				"value": 12750
			}, {
				"key": 16,
				"value": 4029
			}, {
				"key": 17,
				"value": 110
			}, {
				"key": 18,
				"value": 47194
			}, {
				"key": 19,
				"value": 2948
			}]
		},
		"value_counts": {
			"array": [{
				"key": 1,
				"value": 19513
			}, {
				"key": 2,
				"value": 19513
			}, {
				"key": 3,
				"value": 19513
			}, {
				"key": 4,
				"value": 19513
			}, {
				"key": 5,
				"value": 19513
			}, {
				"key": 6,
				"value": 19513
			}, {
				"key": 7,
				"value": 19513
			}, {
				"key": 8,
				"value": 19513
			}, {
				"key": 9,
				"value": 19513
			}, {
				"key": 10,
				"value": 19513
			}, {
				"key": 11,
				"value": 19513
			}, {
				"key": 12,
				"value": 19513
			}, {
				"key": 13,
				"value": 19513
			}, {
				"key": 14,
				"value": 19513
			}, {
				"key": 15,
				"value": 19513
			}, {
				"key": 16,
				"value": 19513
			}, {
				"key": 17,
				"value": 19513
			}, {
				"key": 18,
				"value": 19513
			}, {
				"key": 19,
				"value": 19513
			}]
		},
		"null_value_counts": {
			"array": [{
				"key": 1,
				"value": 19513
			}, {
				"key": 2,
				"value": 0
			}, {
				"key": 3,
				"value": 0
			}, {
				"key": 4,
				"value": 19513
			}, {
				"key": 5,
				"value": 19513
			}, {
				"key": 6,
				"value": 19513
			}, {
				"key": 7,
				"value": 0
			}, {
				"key": 8,
				"value": 0
			}, {
				"key": 9,
				"value": 19513
			}, {
				"key": 10,
				"value": 0
			}, {
				"key": 11,
				"value": 0
			}, {
				"key": 12,
				"value": 19513
			}, {
				"key": 13,
				"value": 0
			}, {
				"key": 14,
				"value": 0
			}, {
				"key": 15,
				"value": 0
			}, {
				"key": 16,
				"value": 0
			}, {
				"key": 17,
				"value": 0
			}, {
				"key": 18,
				"value": 0
			}, {
				"key": 19,
				"value": 0
			}]
		},
		"nan_value_counts": {
			"array": [{
				"key": 16,
				"value": 0
			}, {
				"key": 17,
				"value": 0
			}, {
				"key": 18,
				"value": 0
			}, {
				"key": 19,
				"value": 0
			}, {
				"key": 10,
				"value": 0
			}, {
				"key": 11,
				"value": 0
			}, {
				"key": 12,
				"value": 0
			}, {
				"key": 13,
				"value": 0
			}, {
				"key": 14,
				"value": 0
			}, {
				"key": 15,
				"value": 0
			}]
		},
		"lower_bounds": {
			"array": [{
				"key": 2,
				"value": "2020-04-01 00:00"
			}, {
				"key": 3,
				"value": "2020-04-01 00:12"
			}, {
				"key": 7,
				"value": "\u0003\u0000\u0000\u0000"
			}, {
				"key": 8,
				"value": "\u0001\u0000\u0000\u0000"
			}, {
				"key": 10,
				"value": "ö(\\Â\u0005SÀ"
			}, {
				"key": 11,
				"value": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000"
			}, {
				"key": 13,
				"value": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000"
			}, {
				"key": 14,
				"value": "\u0000\u0000\u0000\u0000\u0000\u0000à¿"
			}, {
				"key": 15,
				"value": ")\\Âõ(\bÀ"
			}, {
				"key": 16,
				"value": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000"
			}, {
				"key": 17,
				"value": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000"
			}, {
				"key": 18,
				"value": "ö(\\ÂÅSÀ"
			}, {
				"key": 19,
				"value": "\u0000\u0000\u0000\u0000\u0000\u0000\u0004À"
			}]
		},
		"upper_bounds": {
			"array": [{
				"key": 2,
				"value": "2020-04-30 23:5:"
			}, {
				"key": 3,
				"value": "2020-05-01 00:41"
			}, {
				"key": 7,
				"value": "\t\u0001\u0000\u0000"
			}, {
				"key": 8,
				"value": "\t\u0001\u0000\u0000"
			}, {
				"key": 10,
				"value": "ÍÌÌÌÌ,_@"
			}, {
				"key": 11,
				"value": "\u001FëQ\\âþ@"
			}, {
				"key": 13,
				"value": "\u0000\u0000\u0000\u0000\u0000\u0000\u0012@"
			}, {
				"key": 14,
				"value": "\u0000\u0000\u0000\u0000\u0000\u0000à?"
			}, {
				"key": 15,
				"value": "q=\n×£ð1@"
			}, {
				"key": 16,
				"value": "\u0000\u0000\u0000\u0000\u0000`B@"
			}, {
				"key": 17,
				"value": "333333Ó?"
			}, {
				"key": 18,
				"value": "\u0000\u0000\u0000\u0000\u0000\u0018b@"
			}, {
				"key": 19,
				"value": "\u0000\u0000\u0000\u0000\u0000\u0000\u0004@"
			}]
		},
		"key_metadata": null,
		"split_offsets": {
			"array": [4]
		},
		"sort_order_id": {
			"int": 0
		}
	}
}
```

## Schema

```bash
avro-tools getschema tests/avro/manifest.avro
```

```json
{
  "type" : "record",
  "name" : "manifest_entry",
  "fields" : [ {
    "name" : "status",
    "type" : "int",
    "field-id" : 0
  }, {
    "name" : "snapshot_id",
    "type" : [ "null", "long" ],
    "default" : null,
    "field-id" : 1
  }, {
    "name" : "data_file",
    "type" : {
      "type" : "record",
      "name" : "r2",
      "fields" : [ {
        "name" : "file_path",
        "type" : "string",
        "doc" : "Location URI with FS scheme",
        "field-id" : 100
      }, {
        "name" : "file_format",
        "type" : "string",
        "doc" : "File format name: avro, orc, or parquet",
        "field-id" : 101
      }, {
        "name" : "partition",
        "type" : {
          "type" : "record",
          "name" : "r102",
          "fields" : [ {
            "name" : "VendorID",
            "type" : [ "null", "int" ],
            "default" : null,
            "field-id" : 1000
          } ]
        },
        "field-id" : 102
      }, {
        "name" : "record_count",
        "type" : "long",
        "doc" : "Number of records in the file",
        "field-id" : 103
      }, {
        "name" : "file_size_in_bytes",
        "type" : "long",
        "doc" : "Total file size in bytes",
        "field-id" : 104
      }, {
        "name" : "block_size_in_bytes",
        "type" : "long",
        "field-id" : 105
      }, {
        "name" : "column_sizes",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k117_v118",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 117
            }, {
              "name" : "value",
              "type" : "long",
              "field-id" : 118
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to total size on disk",
        "default" : null,
        "field-id" : 108
      }, {
        "name" : "value_counts",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k119_v120",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 119
            }, {
              "name" : "value",
              "type" : "long",
              "field-id" : 120
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to total count, including null and NaN",
        "default" : null,
        "field-id" : 109
      }, {
        "name" : "null_value_counts",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k121_v122",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 121
            }, {
              "name" : "value",
              "type" : "long",
              "field-id" : 122
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to null value count",
        "default" : null,
        "field-id" : 110
      }, {
        "name" : "nan_value_counts",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k138_v139",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 138
            }, {
              "name" : "value",
              "type" : "long",
              "field-id" : 139
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to number of NaN values in the column",
        "default" : null,
        "field-id" : 137
      }, {
        "name" : "lower_bounds",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k126_v127",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 126
            }, {
              "name" : "value",
              "type" : "bytes",
              "field-id" : 127
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to lower bound",
        "default" : null,
        "field-id" : 125
      }, {
        "name" : "upper_bounds",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "k129_v130",
            "fields" : [ {
              "name" : "key",
              "type" : "int",
              "field-id" : 129
            }, {
              "name" : "value",
              "type" : "bytes",
              "field-id" : 130
            } ]
          },
          "logicalType" : "map"
        } ],
        "doc" : "Map of column id to upper bound",
        "default" : null,
        "field-id" : 128
      }, {
        "name" : "key_metadata",
        "type" : [ "null", "bytes" ],
        "doc" : "Encryption key metadata blob",
        "default" : null,
        "field-id" : 131
      }, {
        "name" : "split_offsets",
        "type" : [ "null", {
          "type" : "array",
          "items" : "long",
          "element-id" : 133
        } ],
        "doc" : "Splittable offsets",
        "default" : null,
        "field-id" : 132
      }, {
        "name" : "sort_order_id",
        "type" : [ "null", "int" ],
        "doc" : "Sort order ID",
        "default" : null,
        "field-id" : 140
      } ]
    },
    "field-id" : 2
  } ]
}
```
