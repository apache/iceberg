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

import inspect
from enum import Enum
from tempfile import TemporaryDirectory
from typing import Any

import pytest
from fastavro import reader

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    ManifestEntry,
    write_manifest,
)
from pyiceberg.table import Table
from pyiceberg.utils.lazydict import LazyDict


# helper function to serialize our objects to dicts to enable
# direct comparison with the dicts returned by fastavro
def todict(obj: Any) -> Any:
    if isinstance(obj, dict) or isinstance(obj, LazyDict):
        data = []
        for k, v in obj.items():
            data.append({"key": k, "value": v})
        return data
    elif isinstance(obj, Enum):
        return obj.value
    elif hasattr(obj, "__iter__") and not isinstance(obj, str) and not isinstance(obj, bytes):
        return [todict(v) for v in obj]
    elif hasattr(obj, "__dict__"):
        return {key: todict(value) for key, value in inspect.getmembers(obj) if not callable(value) and not key.startswith("_")}
    else:
        return obj


@pytest.fixture()
def catalog() -> Catalog:
    return load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )


@pytest.fixture()
def table_test_all_types(catalog: Catalog) -> Table:
    return catalog.load_table("default.test_all_types")


@pytest.mark.integration
def test_write_sample_manifest(table_test_all_types: Table) -> None:
    test_snapshot = table_test_all_types.current_snapshot()
    if test_snapshot is None:
        raise ValueError("Table has no current snapshot, check the docker environment")
    io = table_test_all_types.io
    test_manifest_file = test_snapshot.manifests(io)[0]
    test_manifest_entries = test_manifest_file.fetch_manifest_entry(io)
    entry = test_manifest_entries[0]
    test_schema = table_test_all_types.schema()
    test_spec = table_test_all_types.spec()
    wrapped_data_file_v2_debug = DataFile(
        format_version=2,
        content=entry.data_file.content,
        file_path=entry.data_file.file_path,
        file_format=entry.data_file.file_format,
        partition=entry.data_file.partition,
        record_count=entry.data_file.record_count,
        file_size_in_bytes=entry.data_file.file_size_in_bytes,
        column_sizes=entry.data_file.column_sizes,
        value_counts=entry.data_file.value_counts,
        null_value_counts=entry.data_file.null_value_counts,
        nan_value_counts=entry.data_file.nan_value_counts,
        lower_bounds=entry.data_file.lower_bounds,
        upper_bounds=entry.data_file.upper_bounds,
        key_metadata=entry.data_file.key_metadata,
        split_offsets=entry.data_file.split_offsets,
        equality_ids=entry.data_file.equality_ids,
        sort_order_id=entry.data_file.sort_order_id,
        spec_id=entry.data_file.spec_id,
    )
    wrapped_entry_v2 = ManifestEntry(*entry.record_fields())
    wrapped_entry_v2.data_file = wrapped_data_file_v2_debug
    with TemporaryDirectory() as tmpdir:
        tmp_avro_file = tmpdir + "/test_write_manifest.avro"
        output = PyArrowFileIO().new_output(tmp_avro_file)
        with write_manifest(
            format_version=2,
            spec=test_spec,
            schema=test_schema,
            output_file=output,
            snapshot_id=test_snapshot.snapshot_id,
        ) as manifest_writer:
            # For simplicity, try one entry first
            manifest_writer.add_entry(test_manifest_entries[0])

        with open(tmp_avro_file, "rb") as fo:
            r = reader(fo=fo)
            it = iter(r)
            fa_entry = next(it)

            assert fa_entry == todict(wrapped_entry_v2)
