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
# pylint:disable=redefined-outer-name
from typing import Dict

import pytest
from sortedcontainers import SortedList

from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    EqualTo,
    In,
)
from pyiceberg.io import PY_IO_IMPL
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestEntry,
    ManifestEntryStatus,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import (
    SetPropertiesUpdate,
    StaticTable,
    Table,
    UpdateSchema,
    _match_deletes_to_datafile,
)
from pyiceberg.table.metadata import INITIAL_SEQUENCE_NUMBER
from pyiceberg.table.snapshots import (
    Operation,
    Snapshot,
    SnapshotLogEntry,
    Summary,
)
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


def test_schema(table: Table) -> None:
    assert table.schema() == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        schema_id=1,
        identifier_field_ids=[1, 2],
    )


def test_schemas(table: Table) -> None:
    assert table.schemas() == {
        0: Schema(
            NestedField(field_id=1, name="x", field_type=LongType(), required=True),
            schema_id=0,
            identifier_field_ids=[],
        ),
        1: Schema(
            NestedField(field_id=1, name="x", field_type=LongType(), required=True),
            NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
            NestedField(field_id=3, name="z", field_type=LongType(), required=True),
            schema_id=1,
            identifier_field_ids=[1, 2],
        ),
    }


def test_spec(table: Table) -> None:
    assert table.spec() == PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"), spec_id=0
    )


def test_specs(table: Table) -> None:
    assert table.specs() == {
        0: PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"), spec_id=0)
    }


def test_sort_order(table: Table) -> None:
    assert table.sort_order() == SortOrder(
        SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
        SortField(
            source_id=3,
            transform=BucketTransform(num_buckets=4),
            direction=SortDirection.DESC,
            null_order=NullOrder.NULLS_LAST,
        ),
        order_id=3,
    )


def test_sort_orders(table: Table) -> None:
    assert table.sort_orders() == {
        3: SortOrder(
            SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
            SortField(
                source_id=3,
                transform=BucketTransform(num_buckets=4),
                direction=SortDirection.DESC,
                null_order=NullOrder.NULLS_LAST,
            ),
            order_id=3,
        )
    }


def test_location(table: Table) -> None:
    assert table.location() == "s3://bucket/test/location"


def test_current_snapshot(table: Table) -> None:
    assert table.current_snapshot() == Snapshot(
        snapshot_id=3055729675574597004,
        parent_snapshot_id=3051729675574597004,
        sequence_number=1,
        timestamp_ms=1555100955770,
        manifest_list="s3://a/b/2.avro",
        summary=Summary(operation=Operation.APPEND),
        schema_id=1,
    )


def test_snapshot_by_id(table: Table) -> None:
    assert table.snapshot_by_id(3055729675574597004) == Snapshot(
        snapshot_id=3055729675574597004,
        parent_snapshot_id=3051729675574597004,
        sequence_number=1,
        timestamp_ms=1555100955770,
        manifest_list="s3://a/b/2.avro",
        summary=Summary(operation=Operation.APPEND),
        schema_id=1,
    )


def test_snapshot_by_id_does_not_exist(table: Table) -> None:
    assert table.snapshot_by_id(-1) is None


def test_snapshot_by_name(table: Table) -> None:
    assert table.snapshot_by_name("test") == Snapshot(
        snapshot_id=3051729675574597004,
        parent_snapshot_id=None,
        sequence_number=0,
        timestamp_ms=1515100955770,
        manifest_list="s3://a/b/1.avro",
        summary=Summary(operation=Operation.APPEND),
        schema_id=None,
    )


def test_snapshot_by_name_does_not_exist(table: Table) -> None:
    assert table.snapshot_by_name("doesnotexist") is None


def test_repr(table: Table) -> None:
    expected = """table(
  1: x: required long,
  2: y: required long (comment),
  3: z: required long
),
partition by: [x],
sort order: [2 ASC NULLS FIRST, bucket[4](3) DESC NULLS LAST],
snapshot: Operation.APPEND: id=3055729675574597004, parent_id=3051729675574597004, schema_id=1"""
    assert repr(table) == expected


def test_history(table: Table) -> None:
    assert table.history() == [
        SnapshotLogEntry(snapshot_id=3051729675574597004, timestamp_ms=1515100955770),
        SnapshotLogEntry(snapshot_id=3055729675574597004, timestamp_ms=1555100955770),
    ]


def test_table_scan_select(table: Table) -> None:
    scan = table.scan()
    assert scan.selected_fields == ("*",)
    assert scan.select("a", "b").selected_fields == ("a", "b")
    assert scan.select("a", "c").select("a").selected_fields == ("a",)


def test_table_scan_row_filter(table: Table) -> None:
    scan = table.scan()
    assert scan.row_filter == AlwaysTrue()
    assert scan.filter(EqualTo("x", 10)).row_filter == EqualTo("x", 10)
    assert scan.filter(EqualTo("x", 10)).filter(In("y", (10, 11))).row_filter == And(EqualTo("x", 10), In("y", (10, 11)))


def test_table_scan_ref(table: Table) -> None:
    scan = table.scan()
    assert scan.use_ref("test").snapshot_id == 3051729675574597004


def test_table_scan_ref_does_not_exists(table: Table) -> None:
    scan = table.scan()

    with pytest.raises(ValueError) as exc_info:
        _ = scan.use_ref("boom")

    assert "Cannot scan unknown ref=boom" in str(exc_info.value)


def test_table_scan_projection_full_schema(table: Table) -> None:
    scan = table.scan()
    assert scan.select("x", "y", "z").projection() == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        schema_id=1,
        identifier_field_ids=[1, 2],
    )


def test_table_scan_projection_single_column(table: Table) -> None:
    scan = table.scan()
    assert scan.select("y").projection() == Schema(
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        schema_id=1,
        identifier_field_ids=[2],
    )


def test_table_scan_projection_single_column_case_sensitive(table: Table) -> None:
    scan = table.scan()
    assert scan.with_case_sensitive(False).select("Y").projection() == Schema(
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        schema_id=1,
        identifier_field_ids=[2],
    )


def test_table_scan_projection_unknown_column(table: Table) -> None:
    scan = table.scan()

    with pytest.raises(ValueError) as exc_info:
        _ = scan.select("a").projection()

    assert "Could not find column: 'a'" in str(exc_info.value)


def test_static_table_same_as_table(table: Table, metadata_location: str) -> None:
    static_table = StaticTable.from_metadata(metadata_location)
    assert isinstance(static_table, Table)
    assert static_table.metadata == table.metadata


def test_static_table_gz_same_as_table(table: Table, metadata_location_gz: str) -> None:
    static_table = StaticTable.from_metadata(metadata_location_gz)
    assert isinstance(static_table, Table)
    assert static_table.metadata == table.metadata


def test_static_table_io_does_not_exist(metadata_location: str) -> None:
    with pytest.raises(ValueError):
        StaticTable.from_metadata(metadata_location, {PY_IO_IMPL: "pyiceberg.does.not.exist.FileIO"})


def test_match_deletes_to_datafile() -> None:
    data_entry = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=1,
        data_file=DataFile(
            content=DataFileContent.DATA,
            file_path="s3://bucket/0000.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
        ),
    )
    delete_entry_1 = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=0,  # Older than the data
        data_file=DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/0001-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
        ),
    )
    delete_entry_2 = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=3,
        data_file=DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/0002-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
            # We don't really care about the tests here
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
        ),
    )
    assert _match_deletes_to_datafile(
        data_entry,
        SortedList(iterable=[delete_entry_1, delete_entry_2], key=lambda entry: entry.sequence_number or INITIAL_SEQUENCE_NUMBER),
    ) == {
        delete_entry_2.data_file,
    }


def test_match_deletes_to_datafile_duplicate_number() -> None:
    data_entry = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=1,
        data_file=DataFile(
            content=DataFileContent.DATA,
            file_path="s3://bucket/0000.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
        ),
    )
    delete_entry_1 = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=3,
        data_file=DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/0001-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
            # We don't really care about the tests here
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
        ),
    )
    delete_entry_2 = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=3,
        data_file=DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/0002-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
            # We don't really care about the tests here
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
        ),
    )
    assert _match_deletes_to_datafile(
        data_entry,
        SortedList(iterable=[delete_entry_1, delete_entry_2], key=lambda entry: entry.sequence_number or INITIAL_SEQUENCE_NUMBER),
    ) == {
        delete_entry_1.data_file,
        delete_entry_2.data_file,
    }


def test_serialize_set_properties_updates() -> None:
    assert SetPropertiesUpdate(updates={"abc": "🤪"}).model_dump_json() == """{"action":"set-properties","updates":{"abc":"🤪"}}"""


def test_add_column(table: Table) -> None:
    update = UpdateSchema(table)
    update.add_column(path="b", field_type=IntegerType())
    apply_schema: Schema = update._apply()  # pylint: disable=W0212
    assert len(apply_schema.fields) == 4

    assert apply_schema == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        NestedField(field_id=4, name="b", field_type=IntegerType(), required=False),
        identifier_field_ids=[1, 2],
    )
    assert apply_schema.schema_id == 2
    assert apply_schema.highest_field_id == 4


def test_add_primitive_type_column(table: Table) -> None:
    primitive_type: Dict[str, PrimitiveType] = {
        "boolean": BooleanType(),
        "int": IntegerType(),
        "long": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "date": DateType(),
        "time": TimeType(),
        "timestamp": TimestampType(),
        "timestamptz": TimestamptzType(),
        "string": StringType(),
        "uuid": UUIDType(),
        "binary": BinaryType(),
    }

    for name, type_ in primitive_type.items():
        field_name = f"new_column_{name}"
        update = UpdateSchema(table)
        update.add_column(path=field_name, field_type=type_, doc=f"new_column_{name}")
        new_schema = update._apply()  # pylint: disable=W0212

        field: NestedField = new_schema.find_field(field_name)
        assert field.field_type == type_
        assert field.doc == f"new_column_{name}"


def test_add_nested_type_column(table: Table) -> None:
    # add struct type column
    field_name = "new_column_struct"
    update = UpdateSchema(table)
    struct_ = StructType(
        NestedField(1, "lat", DoubleType()),
        NestedField(2, "long", DoubleType()),
    )
    update.add_column(path=field_name, field_type=struct_)
    schema_ = update._apply()  # pylint: disable=W0212
    field: NestedField = schema_.find_field(field_name)
    assert field.field_type == StructType(
        NestedField(5, "lat", DoubleType()),
        NestedField(6, "long", DoubleType()),
    )
    assert schema_.highest_field_id == 6


def test_add_nested_map_type_column(table: Table) -> None:
    # add map type column
    field_name = "new_column_map"
    update = UpdateSchema(table)
    map_ = MapType(1, StringType(), 2, IntegerType(), False)
    update.add_column(path=field_name, field_type=map_)
    new_schema = update._apply()  # pylint: disable=W0212
    field: NestedField = new_schema.find_field(field_name)
    assert field.field_type == MapType(5, StringType(), 6, IntegerType(), False)
    assert new_schema.highest_field_id == 6


def test_add_nested_list_type_column(table: Table) -> None:
    # add list type column
    field_name = "new_column_list"
    update = UpdateSchema(table)
    list_ = ListType(
        element_id=101,
        element_type=StructType(
            NestedField(102, "lat", DoubleType()),
            NestedField(103, "long", DoubleType()),
        ),
        element_required=False,
    )
    update.add_column(path=field_name, field_type=list_)
    new_schema = update._apply()  # pylint: disable=W0212
    field: NestedField = new_schema.find_field(field_name)
    assert field.field_type == ListType(
        element_id=5,
        element_type=StructType(
            NestedField(6, "lat", DoubleType()),
            NestedField(7, "long", DoubleType()),
        ),
        element_required=False,
    )
    assert new_schema.highest_field_id == 7
