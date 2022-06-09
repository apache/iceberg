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

from typing import List, Literal, Union

from pydantic import Field

from iceberg.schema import Schema
from iceberg.utils.iceberg_base_model import IcebergBaseModel


class TableMetadataCommonFields(IcebergBaseModel):
    """Metadata for an Iceberg table as specified in the Apache Iceberg
    spec (https://iceberg.apache.org/spec/#iceberg-table-spec)"""

    table_uuid: str = Field(alias="table-uuid")
    """A UUID that identifies the table, generated when the table is created.
    Implementations must throw an exception if a table’s UUID does not match
    the expected UUID after refreshing metadata."""

    location: str
    """The table’s base location. This is used by writers to determine where
    to store data files, manifest files, and table metadata files."""

    last_updated_ms: int = Field(alias="last-updated-ms")
    """Timestamp in milliseconds from the unix epoch when the table
    was last updated. Each table metadata file should update this
    field just before writing."""

    last_column_id: int = Field(alias="last-column-id")
    """An integer; the highest assigned column ID for the table.
    This is used to ensure columns are always assigned an unused ID
    when evolving schemas."""

    schemas: List[Schema] = Field()
    """A list of schemas, stored as objects with schema-id."""

    current_schema_id: int = Field(alias="current-schema-id")
    """ID of the table’s current schema."""

    partition_specs: list = Field(alias="partition-specs")
    """A list of partition specs, stored as full partition spec objects."""

    default_spec_id: int = Field(alias="default-spec-id")
    """ID of the “current” spec that writers should use by default."""

    last_partition_id: int = Field(alias="last-partition-id")
    """An integer; the highest assigned partition field ID across all
    partition specs for the table. This is used to ensure partition fields
    are always assigned an unused ID when evolving specs."""

    properties: dict
    """	A string to string map of table properties. This is used to
    control settings that affect reading and writing and is not intended
    to be used for arbitrary metadata. For example, commit.retry.num-retries
    is used to control the number of commit retries."""

    current_snapshot_id: int = Field(alias="current-snapshot-id")
    """ID of the current table snapshot."""

    snapshots: list
    """A list of valid snapshots. Valid snapshots are snapshots for which
    all data files exist in the file system. A data file must not be
    deleted from the file system until the last snapshot in which it was
    listed is garbage collected."""

    snapshot_log: list = Field(alias="snapshot-log")
    """A list (optional) of timestamp and snapshot ID pairs that encodes
    changes to the current snapshot for the table. Each time the
    current-snapshot-id is changed, a new entry should be added with the
    last-updated-ms and the new current-snapshot-id. When snapshots are
    expired from the list of valid snapshots, all entries before a snapshot
    that has expired should be removed."""

    metadata_log: list = Field(alias="metadata-log")
    """A list (optional) of timestamp and metadata file location pairs that
    encodes changes to the previous metadata files for the table. Each time
    a new metadata file is created, a new entry of the previous metadata
    file location should be added to the list. Tables can be configured to
    remove oldest metadata log entries and keep a fixed-size log of the most
    recent entries after a commit."""

    sort_orders: list = Field(alias="sort-orders")
    """A list of sort orders, stored as full sort order objects."""

    default_sort_order_id: int = Field(alias="default-sort-order-id")
    """Default sort order id of the table. Note that this could be used by
    writers, but is not used when reading because reads use the specs stored
     in manifest files."""


class TableMetadataV1(TableMetadataCommonFields, IcebergBaseModel):
    format_version: Literal[1] = Field(alias="format-version")
    """An integer version number for the format. Currently, this can be 1 or 2
    based on the spec. Implementations must throw an exception if a table’s
    version is higher than the supported version."""

    schema_: Schema = Field(alias="schema")
    """The table’s current schema. (Deprecated: use schemas and
    current-schema-id instead)"""

    partition_spec: dict = Field(alias="partition-spec")
    """The table’s current partition spec, stored as only fields.
    Note that this is used by writers to partition data, but is
    not used when reading because reads use the specs stored in
    manifest files. (Deprecated: use partition-specs and default-spec-id
    instead)"""


class TableMetadataV2(TableMetadataCommonFields, IcebergBaseModel):
    format_version: Literal[2] = Field(alias="format-version")
    """An integer version number for the format. Currently, this can be 1 or 2
    based on the spec. Implementations must throw an exception if a table’s
    version is higher than the supported version."""

    last_sequence_number: int = Field(alias="last-sequence-number")
    """The table’s highest assigned sequence number, a monotonically
    increasing long that tracks the order of snapshots in a table."""


class TableMetadata:
    # Once this has been resolved, we can simplify this: https://github.com/samuelcolvin/pydantic/issues/3846
    # TableMetadata = Annotated[Union[TableMetadataV1, TableMetadataV2], Field(alias="format-version", discriminator="format-version")]

    @staticmethod
    def parse_obj(data: dict) -> Union[TableMetadataV1, TableMetadataV2]:
        if "format-version" not in data:
            raise ValueError(f"Missing format-version in TableMetadata: {data}")

        format_version = data["format-version"]

        if format_version == 1:
            return TableMetadataV1(**data)
        elif format_version == 2:
            return TableMetadataV2(**data)
        else:
            raise ValueError(f"Unknown format version: {format_version}")
