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
import glob
import json
import os
import tempfile
from typing import Tuple

import pytest
from fastavro import reader, writer

from pyiceberg.expressions import (
    BooleanExpression,
    EqualTo,
    IsNull,
    Reference,
)
from pyiceberg.expressions.literals import literal
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table
from tests.conftest import LocalFileIO
from tests.io.test_io import LocalInputFile


@pytest.fixture
def temperatures_table() -> Table:
    package_dir = os.path.dirname(os.path.abspath(__file__))
    base_location = (
        "s3://nflx-secure-dataeng-prod-us-east-1/iceberg/warehouse/dpratap.db/84cf6afa-e639-49a3-a294-bd124249ff74/temperatures"
    )
    return _copy_and_rewrite_table(
        identifier=("test", "temperatures"),
        current_absolute_base_path=base_location,
        metadata_location=("metadata", "00001-14139f1e-0653-489c-8d9a-d27e78c13fb9.metadata.json"),
        walk_dir=os.path.abspath(os.path.join(package_dir, "..", "warehouse", "temperatures")),
        new_location=tempfile.mkdtemp(),
    )


def _copy_and_rewrite_table(
    identifier: Tuple[str, ...],
    current_absolute_base_path: str,
    metadata_location: Tuple[str, ...],
    walk_dir: str,
    new_location: str,
) -> Table:
    """API to copy a table from a location to another, re-writing all its absolute paths within files to
    the new location. This is useful for staging Iceberg test tables in testing directory for testing PyIceberg
    read data APIs until we have the write-data APIs available in PyIceberg to do this on the fly as part of the
    test set up.

    Args:
        identifier: desired table identifier
        current_absolute_base_path: Current base location of the table in its metadata. This is usually the value of 'location' in
            the metadata file.
        metadata_location: Tuple of strings representing the metadata location relative to the base path. This is
            intentionally made tuple to avoid OS specific location separators.
        walk_dir: Absolute path of the directory where the table metadata and data resides.
        new_location: The new base location where the table should be copied and re-written to.

    Returns:
        the table instance of the copied and re-written table

    """

    for filename in glob.iglob(walk_dir + "/**/*", recursive=True):
        if not os.path.isdir(filename):
            with open(filename, "rb") as f:
                tmp_file = filename.replace(walk_dir, new_location)
                os.makedirs(os.path.dirname(tmp_file), exist_ok=True)

                if filename.endswith(".metadata.json"):
                    # Process Table Metadata files
                    _copy_and_rewrite_table_metadata_file(current_absolute_base_path, f, tmp_file, new_location)

                elif "snap-" in filename and filename.endswith(".avro"):
                    # Process Manifest List File For a Snapshot
                    _copy_and_rewrite_manifest_list_file(current_absolute_base_path, f, tmp_file, new_location)

                elif filename.endswith(".avro"):
                    # Process Manifest Path Files Under a Manifest List File
                    _copy_and_rewrite_manifest_file(current_absolute_base_path, f, tmp_file, new_location)

                else:
                    # Data Files (Parquet Format)
                    _copy_data_file(f, tmp_file)

    rewritten_metadata_location = os.path.join(new_location, *metadata_location)
    table_metadata = FromInputFile.table_metadata(LocalInputFile(rewritten_metadata_location))
    return Table(identifier=identifier, metadata=table_metadata, metadata_location=rewritten_metadata_location)


def _copy_data_file(f, tmp_file):
    parquet_content = f.read()
    with open(tmp_file, "wb") as out:
        out.write(parquet_content)


def _copy_and_rewrite_manifest_file(base_location, f, tmp_file, tmpdir):
    avro_reader = reader(f)
    records = []
    for record in avro_reader:
        record["data_file"]["file_path"] = record["data_file"]["file_path"].replace(base_location, tmpdir)
        records.append(record)
    with open(tmp_file, "wb") as out:
        writer(out, avro_reader.writer_schema, records)


def _copy_and_rewrite_manifest_list_file(base_location, f, tmp_file, tmpdir):
    avro_reader = reader(f)
    records = []
    for record in avro_reader:
        record["manifest_path"] = record["manifest_path"].replace(base_location, tmpdir)
        records.append(record)
    with open(tmp_file, "wb") as out:
        writer(out, avro_reader.writer_schema, records)


def _copy_and_rewrite_table_metadata_file(base_location, f, tmp_file, tmpdir):
    table_metadata = json.load(f)
    table_metadata["location"] = table_metadata["location"].replace(base_location, tmpdir)
    for snapshot in table_metadata["snapshots"]:
        snapshot["manifest-list"] = snapshot["manifest-list"].replace(base_location, tmpdir)
    for log in table_metadata["metadata-log"]:
        log["metadata-file"] = log["metadata-file"].replace(base_location, tmpdir)
    with open(tmp_file, "w", encoding="utf-8") as out:
        json.dump(table_metadata, out)


def test_plan_files_for_full_table_scan_of_temperatures(temperatures_table: Table, local_file_io: LocalFileIO):
    file_scan_tasks = list(temperatures_table.new_scan(local_file_io).plan_files())
    assert len(file_scan_tasks) == 13


def test_plan_files_of_temperatures_for_country_id_null(temperatures_table: Table, local_file_io: LocalFileIO):
    partition_filter: BooleanExpression = IsNull(term=Reference("country_id"))
    file_scan_tasks = list(temperatures_table.new_scan(local_file_io, expression=partition_filter).plan_files())
    assert len(file_scan_tasks) == 0


def test_plan_files_of_temperatures_for_country_id_is_blank(temperatures_table: Table, local_file_io: LocalFileIO):
    partition_filter: BooleanExpression = EqualTo(term=Reference("country_id"), literal=literal(""))
    file_scan_tasks = list(temperatures_table.new_scan(local_file_io, expression=partition_filter).plan_files())

    # This would eventually prune down to 6 once residual evaluation is implemented.
    assert len(file_scan_tasks) == 13


def test_plan_files_of_temperatures_for_country_id_bra(temperatures_table: Table, local_file_io: LocalFileIO):
    partition_filter: BooleanExpression = EqualTo(term=Reference("country_id"), literal=literal("BRA"))
    file_scan_tasks = list(temperatures_table.new_scan(local_file_io, expression=partition_filter).plan_files())

    # This would eventually prune down to 5 once residual evaluation is implemented.
    assert len(file_scan_tasks) == 13


def test_plan_files_of_temperatures_for_country_id_new(temperatures_table: Table, local_file_io: LocalFileIO):
    partition_filter: BooleanExpression = EqualTo(term=Reference("country_id"), literal=literal("NEW"))
    file_scan_tasks = list(temperatures_table.new_scan(local_file_io, expression=partition_filter).plan_files())

    # This would eventually prune down to 2 once residual evaluation is implemented.
    assert len(file_scan_tasks) == 13
