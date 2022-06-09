# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# pylint: disable=W0212
import pathlib

from iceberg.avro.file import AvroFile
from iceberg.avro.reader import AvroStruct, BooleanReader, FixedReader
from iceberg.schema import Schema
from iceberg.types import (
    BinaryType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)
from tests.io.test_io_base import LocalInputFile

MANIFEST_SCHEMA = Schema(
    NestedField(field_id=0, name="status", field_type=IntegerType(), is_optional=False),
    NestedField(field_id=1, name="snapshot_id", field_type=LongType(), is_optional=True),
    NestedField(
        field_id=2,
        name="data_file",
        field_type=StructType(
            NestedField(
                field_id=100,
                name="file_path",
                field_type=StringType(),
                doc="Location URI with FS scheme",
                is_optional=False,
            ),
            NestedField(
                field_id=101,
                name="file_format",
                field_type=StringType(),
                doc="File format name: avro, orc, or parquet",
                is_optional=False,
            ),
            NestedField(
                field_id=102,
                name="partition",
                field_type=StructType(
                    NestedField(
                        field_id=1000,
                        name="VendorID",
                        field_type=IntegerType(),
                        is_optional=True,
                    ),
                ),
                is_optional=False,
            ),
            NestedField(
                field_id=103,
                name="record_count",
                field_type=LongType(),
                doc="Number of records in the file",
                is_optional=False,
            ),
            NestedField(
                field_id=104,
                name="file_size_in_bytes",
                field_type=LongType(),
                doc="Total file size in bytes",
                is_optional=False,
            ),
            NestedField(
                field_id=105,
                name="block_size_in_bytes",
                field_type=LongType(),
                is_optional=False,
            ),
            NestedField(
                field_id=108,
                name="column_sizes",
                field_type=MapType(
                    key_id=117,
                    key_type=IntegerType(),
                    value_id=118,
                    value_type=LongType(),
                    value_is_optional=False,
                ),
                doc="Map of column id to total size on disk",
                is_optional=True,
            ),
            NestedField(
                field_id=109,
                name="value_counts",
                field_type=MapType(
                    key_id=119,
                    key_type=IntegerType(),
                    value_id=120,
                    value_type=LongType(),
                    value_is_optional=False,
                ),
                doc="Map of column id to total count, including null and NaN",
                is_optional=True,
            ),
            NestedField(
                field_id=110,
                name="null_value_counts",
                field_type=MapType(
                    key_id=121,
                    key_type=IntegerType(),
                    value_id=122,
                    value_type=LongType(),
                    value_is_optional=False,
                ),
                doc="Map of column id to null value count",
                is_optional=True,
            ),
            NestedField(
                field_id=137,
                name="nan_value_counts",
                field_type=MapType(
                    key_id=138,
                    key_type=IntegerType(),
                    value_id=139,
                    value_type=LongType(),
                    value_is_optional=False,
                ),
                doc="Map of column id to number of NaN values in the column",
                is_optional=True,
            ),
            NestedField(
                field_id=125,
                name="lower_bounds",
                field_type=MapType(
                    key_id=126,
                    key_type=IntegerType(),
                    value_id=127,
                    value_type=BinaryType(),
                    value_is_optional=False,
                ),
                doc="Map of column id to lower bound",
                is_optional=True,
            ),
            NestedField(
                field_id=128,
                name="upper_bounds",
                field_type=MapType(
                    key_id=129,
                    key_type=IntegerType(),
                    value_id=130,
                    value_type=BinaryType(),
                    value_is_optional=False,
                ),
                doc="Map of column id to upper bound",
                is_optional=True,
            ),
            NestedField(
                field_id=131,
                name="key_metadata",
                field_type=BinaryType(),
                doc="Encryption key metadata blob",
                is_optional=True,
            ),
            NestedField(
                field_id=132,
                name="split_offsets",
                field_type=ListType(
                    element_id=133,
                    element_type=LongType(),
                    element_is_optional=False,
                ),
                doc="Splittable offsets",
                is_optional=True,
            ),
            NestedField(
                field_id=140,
                name="sort_order_id",
                field_type=IntegerType(),
                doc="Sort order ID",
                is_optional=True,
            ),
        ),
        is_optional=False,
    ),
    schema_id=1,
    identifier_field_ids=[],
)

test_file_path = str(pathlib.Path(__file__).parent.resolve()) + "/data/manifest.avro"


def test_read_header(manifest_schema):
    test_file = LocalInputFile(test_file_path)
    with AvroFile(test_file) as reader:
        header = reader._read_header()

    assert header.magic == b"Obj\x01"
    assert header.meta == {
        "schema": '{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"VendorID","required":false,"type":"int"},{"id":2,"name":"tpep_pickup_datetime","required":false,"type":"string"},{"id":3,"name":"tpep_dropoff_datetime","required":false,"type":"string"},{"id":4,"name":"passenger_count","required":false,"type":"int"},{"id":5,"name":"RatecodeID","required":false,"type":"int"},{"id":6,"name":"store_and_fwd_flag","required":false,"type":"string"},{"id":7,"name":"PULocationID","required":false,"type":"int"},{"id":8,"name":"DOLocationID","required":false,"type":"int"},{"id":9,"name":"payment_type","required":false,"type":"int"},{"id":10,"name":"fare","required":false,"type":"double"},{"id":11,"name":"distance","required":false,"type":"double","doc":"The elapsed trip distance in miles reported by the taximeter."},{"id":12,"name":"fare_per_distance_unit","required":false,"type":"float"},{"id":13,"name":"extra","required":false,"type":"double"},{"id":14,"name":"mta_tax","required":false,"type":"double"},{"id":15,"name":"tip_amount","required":false,"type":"double"},{"id":16,"name":"tolls_amount","required":false,"type":"double"},{"id":17,"name":"improvement_surcharge","required":false,"type":"double"},{"id":18,"name":"total_amount","required":false,"type":"double"},{"id":19,"name":"congestion_surcharge","required":false,"type":"double"}]}',
        "avro.schema": '{"type":"record","name":"manifest_entry","fields":[{"name":"status","type":"int","field-id":0},{"name":"snapshot_id","type":["null","long"],"default":null,"field-id":1},{"name":"data_file","type":{"type":"record","name":"r2","fields":[{"name":"file_path","type":"string","doc":"Location URI with FS scheme","field-id":100},{"name":"file_format","type":"string","doc":"File format name: avro, orc, or parquet","field-id":101},{"name":"partition","type":{"type":"record","name":"r102","fields":[{"name":"VendorID","type":["null","int"],"default":null,"field-id":1000}]},"field-id":102},{"name":"record_count","type":"long","doc":"Number of records in the file","field-id":103},{"name":"file_size_in_bytes","type":"long","doc":"Total file size in bytes","field-id":104},{"name":"block_size_in_bytes","type":"long","field-id":105},{"name":"column_sizes","type":["null",{"type":"array","items":{"type":"record","name":"k117_v118","fields":[{"name":"key","type":"int","field-id":117},{"name":"value","type":"long","field-id":118}]},"logicalType":"map"}],"doc":"Map of column id to total size on disk","default":null,"field-id":108},{"name":"value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k119_v120","fields":[{"name":"key","type":"int","field-id":119},{"name":"value","type":"long","field-id":120}]},"logicalType":"map"}],"doc":"Map of column id to total count, including null and NaN","default":null,"field-id":109},{"name":"null_value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k121_v122","fields":[{"name":"key","type":"int","field-id":121},{"name":"value","type":"long","field-id":122}]},"logicalType":"map"}],"doc":"Map of column id to null value count","default":null,"field-id":110},{"name":"nan_value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k138_v139","fields":[{"name":"key","type":"int","field-id":138},{"name":"value","type":"long","field-id":139}]},"logicalType":"map"}],"doc":"Map of column id to number of NaN values in the column","default":null,"field-id":137},{"name":"lower_bounds","type":["null",{"type":"array","items":{"type":"record","name":"k126_v127","fields":[{"name":"key","type":"int","field-id":126},{"name":"value","type":"bytes","field-id":127}]},"logicalType":"map"}],"doc":"Map of column id to lower bound","default":null,"field-id":125},{"name":"upper_bounds","type":["null",{"type":"array","items":{"type":"record","name":"k129_v130","fields":[{"name":"key","type":"int","field-id":129},{"name":"value","type":"bytes","field-id":130}]},"logicalType":"map"}],"doc":"Map of column id to upper bound","default":null,"field-id":128},{"name":"key_metadata","type":["null","bytes"],"doc":"Encryption key metadata blob","default":null,"field-id":131},{"name":"split_offsets","type":["null",{"type":"array","items":"long","element-id":133}],"doc":"Splittable offsets","default":null,"field-id":132},{"name":"sort_order_id","type":["null","int"],"doc":"Sort order ID","default":null,"field-id":140}]},"field-id":2}]}',
        "avro.codec": "deflate",
        "format-version": "1",
        "partition-spec-id": "0",
        "iceberg.schema": '{"type":"struct","schema-id":0,"fields":[{"id":0,"name":"status","required":true,"type":"int"},{"id":1,"name":"snapshot_id","required":false,"type":"long"},{"id":2,"name":"data_file","required":true,"type":{"type":"struct","fields":[{"id":100,"name":"file_path","required":true,"type":"string","doc":"Location URI with FS scheme"},{"id":101,"name":"file_format","required":true,"type":"string","doc":"File format name: avro, orc, or parquet"},{"id":102,"name":"partition","required":true,"type":{"type":"struct","fields":[{"id":1000,"name":"VendorID","required":false,"type":"int"}]}},{"id":103,"name":"record_count","required":true,"type":"long","doc":"Number of records in the file"},{"id":104,"name":"file_size_in_bytes","required":true,"type":"long","doc":"Total file size in bytes"},{"id":105,"name":"block_size_in_bytes","required":true,"type":"long"},{"id":108,"name":"column_sizes","required":false,"type":{"type":"map","key-id":117,"key":"int","value-id":118,"value":"long","value-required":true},"doc":"Map of column id to total size on disk"},{"id":109,"name":"value_counts","required":false,"type":{"type":"map","key-id":119,"key":"int","value-id":120,"value":"long","value-required":true},"doc":"Map of column id to total count, including null and NaN"},{"id":110,"name":"null_value_counts","required":false,"type":{"type":"map","key-id":121,"key":"int","value-id":122,"value":"long","value-required":true},"doc":"Map of column id to null value count"},{"id":137,"name":"nan_value_counts","required":false,"type":{"type":"map","key-id":138,"key":"int","value-id":139,"value":"long","value-required":true},"doc":"Map of column id to number of NaN values in the column"},{"id":125,"name":"lower_bounds","required":false,"type":{"type":"map","key-id":126,"key":"int","value-id":127,"value":"binary","value-required":true},"doc":"Map of column id to lower bound"},{"id":128,"name":"upper_bounds","required":false,"type":{"type":"map","key-id":129,"key":"int","value-id":130,"value":"binary","value-required":true},"doc":"Map of column id to upper bound"},{"id":131,"name":"key_metadata","required":false,"type":"binary","doc":"Encryption key metadata blob"},{"id":132,"name":"split_offsets","required":false,"type":{"type":"list","element-id":133,"element":"long","element-required":true},"doc":"Splittable offsets"},{"id":140,"name":"sort_order_id","required":false,"type":"int","doc":"Sort order ID"}]}}]}',
        "partition-spec": '[{"name":"VendorID","transform":"identity","source-id":1,"field-id":1000}]',
    }
    assert header.get_schema() == MANIFEST_SCHEMA
    assert header.sync == b"B\x9e\xb0\xc3\x04,vG\xd1O\x8e\xf5\x9c\xdd\x17\x8d"


def test_reader_data():
    test_file = LocalInputFile(test_file_path)
    with AvroFile(test_file) as reader:
        # Consume the generator
        files = list(reader)

    assert len(files) == 3, f"Expected 3 records, got {len(files)}"
    assert files[0] == AvroStruct(
        _data=[
            1,
            8744736658442914487,
            AvroStruct(
                _data=[
                    "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet",
                    "PARQUET",
                    AvroStruct(_data=[None]),
                    19513,
                    388872,
                    67108864,
                    {
                        1: 53,
                        2: 98153,
                        3: 98693,
                        4: 53,
                        5: 53,
                        6: 53,
                        7: 17425,
                        8: 18528,
                        9: 53,
                        10: 44788,
                        11: 35571,
                        12: 53,
                        13: 1243,
                        14: 2355,
                        15: 12750,
                        16: 4029,
                        17: 110,
                        18: 47194,
                        19: 2948,
                    },
                    {
                        1: 19513,
                        2: 19513,
                        3: 19513,
                        4: 19513,
                        5: 19513,
                        6: 19513,
                        7: 19513,
                        8: 19513,
                        9: 19513,
                        10: 19513,
                        11: 19513,
                        12: 19513,
                        13: 19513,
                        14: 19513,
                        15: 19513,
                        16: 19513,
                        17: 19513,
                        18: 19513,
                        19: 19513,
                    },
                    {
                        1: 19513,
                        2: 0,
                        3: 0,
                        4: 19513,
                        5: 19513,
                        6: 19513,
                        7: 0,
                        8: 0,
                        9: 19513,
                        10: 0,
                        11: 0,
                        12: 19513,
                        13: 0,
                        14: 0,
                        15: 0,
                        16: 0,
                        17: 0,
                        18: 0,
                        19: 0,
                    },
                    {16: 0, 17: 0, 18: 0, 19: 0, 10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0},
                    {
                        2: b"2020-04-01 00:00",
                        3: b"2020-04-01 00:12",
                        7: b"\x03\x00\x00\x00",
                        8: b"\x01\x00\x00\x00",
                        10: b"\xf6(\\\x8f\xc2\x05S\xc0",
                        11: b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        13: b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        14: b"\x00\x00\x00\x00\x00\x00\xe0\xbf",
                        15: b")\\\x8f\xc2\xf5(\x08\xc0",
                        16: b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        17: b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        18: b"\xf6(\\\x8f\xc2\xc5S\xc0",
                        19: b"\x00\x00\x00\x00\x00\x00\x04\xc0",
                    },
                    {
                        2: b"2020-04-30 23:5:",
                        3: b"2020-05-01 00:41",
                        7: b"\t\x01\x00\x00",
                        8: b"\t\x01\x00\x00",
                        10: b"\xcd\xcc\xcc\xcc\xcc,_@",
                        11: b"\x1f\x85\xebQ\\\xe2\xfe@",
                        13: b"\x00\x00\x00\x00\x00\x00\x12@",
                        14: b"\x00\x00\x00\x00\x00\x00\xe0?",
                        15: b"q=\n\xd7\xa3\xf01@",
                        16: b"\x00\x00\x00\x00\x00`B@",
                        17: b"333333\xd3?",
                        18: b"\x00\x00\x00\x00\x00\x18b@",
                        19: b"\x00\x00\x00\x00\x00\x00\x04@",
                    },
                    None,
                    [4],
                    0,
                ]
            ),
        ]
    )


def test_singleton():
    """We want to reuse the readers to avoid creating a gazillion of them"""
    assert id(BooleanReader()) == id(BooleanReader())
    assert id(FixedReader(22)) == id(FixedReader(22))
    assert id(FixedReader(19)) != id(FixedReader(25))
