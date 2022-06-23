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
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Union,
)
from uuid import UUID

from pydantic import Field, root_validator

from iceberg.schema import Schema
from iceberg.utils.iceberg_base_model import IcebergBaseModel

_INITIAL_SEQUENCE_NUMBER = 0
INITIAL_SPEC_ID = 0
DEFAULT_SCHEMA_ID = 0


class SnapshotRefType(str, Enum):
    branch = "branch"
    tag = "tag"


class SnapshotRef(IcebergBaseModel):
    snapshot_id: int = Field(alias="snapshot-id")
    snapshot_ref_type: SnapshotRefType = Field(alias="type")
    min_snapshots_to_keep: int = Field(alias="min-snapshots-to-keep")
    max_snapshot_age_ms: int = Field(alias="max-snapshot-age-ms")
    max_ref_age_ms: int = Field(alias="max-ref-age-ms")


class TableMetadataCommonFields(IcebergBaseModel):
    """Metadata for an Iceberg table as specified in the Apache Iceberg
    spec (https://iceberg.apache.org/spec/#iceberg-table-spec)"""

    location: str = Field()
    """The table’s base location. This is used by writers to determine where
    to store data files, manifest files, and table metadata files."""

    last_updated_ms: int = Field(alias="last-updated-ms")
    """Timestamp in milliseconds from the unix epoch when the table
    was last updated. Each table metadata file should update this
    field just before writing."""

    last_column_id: int = Field(alias="last-column-id")
    """An integer; the highest assigned column ID for the table.
    This is used to ensure fields are always assigned an unused ID
    when evolving schemas."""

    schemas: List[Schema] = Field(default_factory=list)
    """A list of schemas, stored as objects with schema-id."""

    current_schema_id: int = Field(alias="current-schema-id", default=0)
    """ID of the table’s current schema."""

    partition_specs: list = Field(alias="partition-specs", default_factory=list)
    """A list of partition specs, stored as full partition spec objects."""

    default_spec_id: int = Field(alias="default-spec-id")
    """ID of the “current” spec that writers should use by default."""

    last_partition_id: int = Field(alias="last-partition-id")
    """An integer; the highest assigned partition field ID across all
    partition specs for the table. This is used to ensure partition fields
    are always assigned an unused ID when evolving specs."""

    properties: Dict[str, str] = Field(default_factory=dict)
    """	A string to string map of table properties. This is used to
    control settings that affect reading and writing and is not intended
    to be used for arbitrary metadata. For example, commit.retry.num-retries
    is used to control the number of commit retries."""

    current_snapshot_id: Optional[int] = Field(alias="current-snapshot-id")
    """ID of the current table snapshot."""

    snapshots: list = Field(default_factory=list)
    """A list of valid snapshots. Valid snapshots are snapshots for which
    all data files exist in the file system. A data file must not be
    deleted from the file system until the last snapshot in which it was
    listed is garbage collected."""

    snapshot_log: List[Dict[str, Any]] = Field(alias="snapshot-log", default_factory=list)
    """A list (optional) of timestamp and snapshot ID pairs that encodes
    changes to the current snapshot for the table. Each time the
    current-snapshot-id is changed, a new entry should be added with the
    last-updated-ms and the new current-snapshot-id. When snapshots are
    expired from the list of valid snapshots, all entries before a snapshot
    that has expired should be removed."""

    metadata_log: List[Dict[str, Any]] = Field(alias="metadata-log", default_factory=list)
    """A list (optional) of timestamp and metadata file location pairs that
    encodes changes to the previous metadata files for the table. Each time
    a new metadata file is created, a new entry of the previous metadata
    file location should be added to the list. Tables can be configured to
    remove oldest metadata log entries and keep a fixed-size log of the most
    recent entries after a commit."""

    sort_orders: List[Dict[str, Any]] = Field(alias="sort-orders", default_factory=list)
    """A list of sort orders, stored as full sort order objects."""

    default_sort_order_id: int = Field(alias="default-sort-order-id")
    """Default sort order id of the table. Note that this could be used by
    writers, but is not used when reading because reads use the specs stored
     in manifest files."""


class TableMetadataV1(TableMetadataCommonFields, IcebergBaseModel):

    # When we read a V1 format-version, we'll make sure to populate the fields
    # for V2 as well. This makes it easier downstream because we can just
    # assume that everything is a TableMetadataV2.
    # When writing, we should stick to the same version that it was,
    # because bumping the version should be an explicit operation that is up
    # to the owner of the table.

    @root_validator(pre=True)
    def set_schema_id(cls, data: Dict[str, Any]):
        # Set some sensible defaults for V1, so we comply with the schema
        # this is in pre=True, meaning that this will be done before validation
        # we don't want to make them optional, since we do require them for V2
        data["schema"]["schema-id"] = DEFAULT_SCHEMA_ID
        data["default-spec-id"] = INITIAL_SPEC_ID
        data["last-partition-id"] = max(spec["field-id"] for spec in data["partition-spec"])
        data["default-sort-order-id"] = 0
        return data

    @root_validator()
    def migrate_schema(cls, data: Dict[str, Any]):
        # Migrate schemas
        schema = data["schema_"]
        schemas = data["schemas"]
        if all([schema != other_schema for other_schema in schemas]):
            data["schemas"].append(schema)
        data["current_schema_id"] = schema.schema_id
        return data

    @root_validator()
    def migrate_partition_spec(cls, data: Dict[str, Any]):
        # This is going to be much nicer as soon as partition-spec is also migrated to pydantic
        if partition_spec := data.get("partition_spec"):
            data["partition_specs"] = [{**spec, "spec-id": INITIAL_SPEC_ID + idx} for idx, spec in enumerate(partition_spec)]
            data["default_spec_id"] = INITIAL_SPEC_ID
            data["last_partition_id"] = max(spec["spec-id"] for spec in data["partition_specs"])
        return data

    table_uuid: Optional[UUID] = Field(alias="table-uuid")
    """A UUID that identifies the table, generated when the table is created.
    Implementations must throw an exception if a table’s UUID does not match
    the expected UUID after refreshing metadata."""

    format_version: Literal[1] = Field(alias="format-version")
    """An integer version number for the format. Currently, this can be 1 or 2
    based on the spec. Implementations must throw an exception if a table’s
    version is higher than the supported version."""

    schema_: Schema = Field(alias="schema")
    """The table’s current schema. (Deprecated: use schemas and
    current-schema-id instead)"""

    partition_spec: List[Dict[str, Any]] = Field(alias="partition-spec")
    """The table’s current partition spec, stored as only fields.
    Note that this is used by writers to partition data, but is
    not used when reading because reads use the specs stored in
    manifest files. (Deprecated: use partition-specs and default-spec-id
    instead)"""


class TableMetadataV2(TableMetadataCommonFields, IcebergBaseModel):
    @root_validator(skip_on_failure=True)
    def check_if_schema_is_found(cls, data: Dict[str, Any]):
        current_schema_id = data["current_schema_id"]

        for schema in data["schemas"]:
            if schema.schema_id == current_schema_id:
                return data

        raise ValueError(f"current-schema-id {current_schema_id} can't be found in the schemas")

    @root_validator
    def check_partition_spec(cls, data: Dict[str, Any]):
        default_spec_id = data["default_spec_id"]

        for spec in data["partition_specs"]:
            if spec["spec-id"] == default_spec_id:
                return data

        raise ValueError(f"default-spec-id {default_spec_id} can't be found")

    @root_validator(skip_on_failure=True)
    def check_sort_order(cls, data: Dict[str, Any]):
        default_sort_order_id = data["default_sort_order_id"]

        # 0 == unsorted
        if default_sort_order_id != 0:
            for sort in data["sort_orders"]:
                if sort["order-id"] == default_sort_order_id:
                    return data

            raise ValueError(f"default-sort-order-id {default_sort_order_id} can't be found")
        return data

    format_version: Literal[2] = Field(alias="format-version")
    """An integer version number for the format. Currently, this can be 1 or 2
    based on the spec. Implementations must throw an exception if a table’s
    version is higher than the supported version."""

    table_uuid: UUID = Field(alias="table-uuid")
    """A UUID that identifies the table, generated when the table is created.
    Implementations must throw an exception if a table’s UUID does not match
    the expected UUID after refreshing metadata."""

    last_sequence_number: int = Field(alias="last-sequence-number", default=_INITIAL_SEQUENCE_NUMBER)
    """The table’s highest assigned sequence number, a monotonically
    increasing long that tracks the order of snapshots in a table."""

    refs: Dict[str, SnapshotRef] = Field(default_factory=dict)
    """A map of snapshot references.
    The map keys are the unique snapshot reference names in the table,
    and the map values are snapshot reference objects.
    There is always a main branch reference pointing to the
    current-snapshot-id even if the refs map is null."""


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
