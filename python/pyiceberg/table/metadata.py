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
from __future__ import annotations

import datetime
import uuid
from copy import copy
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Union,
)

from pydantic import Field, model_validator
from pydantic import ValidationError as PydanticValidationError
from typing_extensions import Annotated

from pyiceberg.exceptions import ValidationError
from pyiceberg.partitioning import PARTITION_FIELD_ID_START, PartitionSpec, assign_fresh_partition_spec_ids
from pyiceberg.schema import Schema, assign_fresh_schema_ids
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRef, SnapshotRefType
from pyiceberg.table.snapshots import MetadataLogEntry, Snapshot, SnapshotLogEntry
from pyiceberg.table.sorting import (
    UNSORTED_SORT_ORDER,
    UNSORTED_SORT_ORDER_ID,
    SortOrder,
    assign_fresh_sort_order_ids,
)
from pyiceberg.typedef import (
    EMPTY_DICT,
    IcebergBaseModel,
    IcebergRootModel,
    Properties,
)
from pyiceberg.utils.datetime import datetime_to_millis

CURRENT_SNAPSHOT_ID = "current-snapshot-id"
CURRENT_SCHEMA_ID = "current-schema-id"
SCHEMAS = "schemas"
DEFAULT_SPEC_ID = "default-spec-id"
PARTITION_SPEC = "partition-spec"
PARTITION_SPECS = "partition-specs"
SORT_ORDERS = "sort-orders"
LAST_PARTITION_ID = "last-partition-id"
LAST_ASSIGNED_FIELD_ID = "last-assigned-field-id"
REFS = "refs"
SPEC_ID = "spec-id"
FIELD_ID = "field-id"
FIELDS = "fields"

INITIAL_SEQUENCE_NUMBER = 0
INITIAL_SPEC_ID = 0
DEFAULT_SCHEMA_ID = 0


def cleanup_snapshot_id(data: Dict[str, Any]) -> Dict[str, Any]:
    """Run before validation."""
    if CURRENT_SNAPSHOT_ID in data and data[CURRENT_SNAPSHOT_ID] == -1:
        # We treat -1 and None the same, by cleaning this up
        # in a pre-validator, we can simplify the logic later on
        data[CURRENT_SNAPSHOT_ID] = None
    return data


def check_schemas(table_metadata: TableMetadata) -> TableMetadata:
    """Check if the current-schema-id is actually present in schemas."""
    current_schema_id = table_metadata.current_schema_id

    for schema in table_metadata.schemas:
        if schema.schema_id == current_schema_id:
            return table_metadata

    raise ValidationError(f"current-schema-id {current_schema_id} can't be found in the schemas")


def check_partition_specs(table_metadata: TableMetadata) -> TableMetadata:
    """Check if the default-spec-id is present in partition-specs."""
    default_spec_id = table_metadata.default_spec_id

    partition_specs: List[PartitionSpec] = table_metadata.partition_specs
    for spec in partition_specs:
        if spec.spec_id == default_spec_id:
            return table_metadata

    raise ValidationError(f"default-spec-id {default_spec_id} can't be found")


def check_sort_orders(table_metadata: TableMetadata) -> TableMetadata:
    """Check if the default_sort_order_id is present in sort-orders."""
    default_sort_order_id: int = table_metadata.default_sort_order_id

    if default_sort_order_id != UNSORTED_SORT_ORDER_ID:
        sort_orders: List[SortOrder] = table_metadata.sort_orders
        for sort_order in sort_orders:
            if sort_order.order_id == default_sort_order_id:
                return table_metadata

        raise ValidationError(f"default-sort-order-id {default_sort_order_id} can't be found in {sort_orders}")
    return table_metadata


def construct_refs(table_metadata: TableMetadata) -> TableMetadata:
    """Set the main branch if missing."""
    if table_metadata.current_snapshot_id is not None:
        if MAIN_BRANCH not in table_metadata.refs:
            table_metadata.refs[MAIN_BRANCH] = SnapshotRef(
                snapshot_id=table_metadata.current_snapshot_id, snapshot_ref_type=SnapshotRefType.BRANCH
            )
    return table_metadata


class TableMetadataCommonFields(IcebergBaseModel):
    """Metadata for an Iceberg table as specified in the Apache Iceberg spec.

    https://iceberg.apache.org/spec/#iceberg-table-spec
    """

    location: str = Field()
    """The table’s base location. This is used by writers to determine where
    to store data files, manifest files, and table metadata files."""

    table_uuid: uuid.UUID = Field(alias="table-uuid", default_factory=uuid.uuid4)
    """A UUID that identifies the table, generated when the table is created.
    Implementations must throw an exception if a table’s UUID does not match
    the expected UUID after refreshing metadata."""

    last_updated_ms: int = Field(
        alias="last-updated-ms", default_factory=lambda: datetime_to_millis(datetime.datetime.now().astimezone())
    )
    """Timestamp in milliseconds from the unix epoch when the table
    was last updated. Each table metadata file should update this
    field just before writing."""

    last_column_id: int = Field(alias="last-column-id")
    """An integer; the highest assigned column ID for the table.
    This is used to ensure fields are always assigned an unused ID
    when evolving schemas."""

    schemas: List[Schema] = Field(default_factory=list)
    """A list of schemas, stored as objects with schema-id."""

    current_schema_id: int = Field(alias="current-schema-id", default=DEFAULT_SCHEMA_ID)
    """ID of the table’s current schema."""

    partition_specs: List[PartitionSpec] = Field(alias="partition-specs", default_factory=list)
    """A list of partition specs, stored as full partition spec objects."""

    default_spec_id: int = Field(alias="default-spec-id", default=INITIAL_SPEC_ID)
    """ID of the “current” spec that writers should use by default."""

    last_partition_id: Optional[int] = Field(alias="last-partition-id", default=None)
    """An integer; the highest assigned partition field ID across all
    partition specs for the table. This is used to ensure partition fields
    are always assigned an unused ID when evolving specs."""

    properties: Dict[str, str] = Field(default_factory=dict)
    """A string to string map of table properties. This is used to
    control settings that affect reading and writing and is not intended
    to be used for arbitrary metadata. For example, commit.retry.num-retries
    is used to control the number of commit retries."""

    current_snapshot_id: Optional[int] = Field(alias="current-snapshot-id", default=None)
    """ID of the current table snapshot."""

    snapshots: List[Snapshot] = Field(default_factory=list)
    """A list of valid snapshots. Valid snapshots are snapshots for which
    all data files exist in the file system. A data file must not be
    deleted from the file system until the last snapshot in which it was
    listed is garbage collected."""

    snapshot_log: List[SnapshotLogEntry] = Field(alias="snapshot-log", default_factory=list)
    """A list (optional) of timestamp and snapshot ID pairs that encodes
    changes to the current snapshot for the table. Each time the
    current-snapshot-id is changed, a new entry should be added with the
    last-updated-ms and the new current-snapshot-id. When snapshots are
    expired from the list of valid snapshots, all entries before a snapshot
    that has expired should be removed."""

    metadata_log: List[MetadataLogEntry] = Field(alias="metadata-log", default_factory=list)
    """A list (optional) of timestamp and metadata file location pairs that
    encodes changes to the previous metadata files for the table. Each time
    a new metadata file is created, a new entry of the previous metadata
    file location should be added to the list. Tables can be configured to
    remove oldest metadata log entries and keep a fixed-size log of the most
    recent entries after a commit."""

    sort_orders: List[SortOrder] = Field(alias="sort-orders", default_factory=list)
    """A list of sort orders, stored as full sort order objects."""

    default_sort_order_id: int = Field(alias="default-sort-order-id", default=UNSORTED_SORT_ORDER_ID)
    """Default sort order id of the table. Note that this could be used by
    writers, but is not used when reading because reads use the specs stored
     in manifest files."""

    refs: Dict[str, SnapshotRef] = Field(default_factory=dict)
    """A map of snapshot references.
    The map keys are the unique snapshot reference names in the table,
    and the map values are snapshot reference objects.
    There is always a main branch reference pointing to the
    current-snapshot-id even if the refs map is null."""


class TableMetadataV1(TableMetadataCommonFields, IcebergBaseModel):
    """Represents version 1 of the Table Metadata.

    More information about the specification:
    https://iceberg.apache.org/spec/#version-1-analytic-data-tables
    """

    # When we read a V1 format-version, we'll make sure to populate the fields
    # for V2 as well. This makes it easier downstream because we can just
    # assume that everything is a TableMetadataV2.
    # When writing, we should stick to the same version that it was,
    # because bumping the version should be an explicit operation that is up
    # to the owner of the table.

    @model_validator(mode="before")
    def cleanup_snapshot_id(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        return cleanup_snapshot_id(data)

    @model_validator(mode="after")
    def construct_refs(cls, data: TableMetadataV1) -> TableMetadataV1:
        return construct_refs(data)

    @model_validator(mode="before")
    def set_v2_compatible_defaults(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Set default values to be compatible with the format v2.

        Args:
            data: The raw arguments when initializing a V1 TableMetadata.

        Returns:
            The TableMetadata with the defaults applied.
        """
        # When the schema doesn't have an ID
        if data.get("schema") and "schema_id" not in data["schema"]:
            data["schema"]["schema_id"] = DEFAULT_SCHEMA_ID

        return data

    @model_validator(mode="before")
    def construct_schemas(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert the schema into schemas.

        For V1 schemas is optional, and if they aren't set, we'll set them
        in this validator. This was we can always use the schemas when reading
        table metadata, and we don't have to worry if it is a v1 or v2 format.

        Args:
            data: The raw data after validation, meaning that the aliases are applied.

        Returns:
            The TableMetadata with the schemas set, if not provided.
        """
        if not data.get("schemas"):
            schema = data["schema"]
            data["schemas"] = [schema]
        return data

    @model_validator(mode="before")
    def construct_partition_specs(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert the partition_spec into partition_specs.

        For V1 partition_specs is optional, and if they aren't set, we'll set them
        in this validator. This was we can always use the partition_specs when reading
        table metadata, and we don't have to worry if it is a v1 or v2 format.

        Args:
            data: The raw data after validation, meaning that the aliases are applied.

        Returns:
            The TableMetadata with the partition_specs set, if not provided.
        """
        if not data.get(PARTITION_SPECS):
            if data.get(PARTITION_SPEC) is not None:
                # Promote the spec from partition-spec to partition-specs
                fields = data[PARTITION_SPEC]
                data[PARTITION_SPECS] = [{SPEC_ID: INITIAL_SPEC_ID, FIELDS: fields}]
                data[DEFAULT_SPEC_ID] = INITIAL_SPEC_ID
            else:
                data[PARTITION_SPECS] = [{"field-id": 0, "fields": ()}]

        data[LAST_PARTITION_ID] = max(
            [field.get(FIELD_ID) for spec in data[PARTITION_SPECS] for field in spec[FIELDS]], default=PARTITION_FIELD_ID_START
        )

        return data

    @model_validator(mode="before")
    def set_sort_orders(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Set the sort_orders if not provided.

        For V1 sort_orders is optional, and if they aren't set, we'll set them
        in this validator.

        Args:
            data: The raw data after validation, meaning that the aliases are applied.

        Returns:
            The TableMetadata with the sort_orders set, if not provided.
        """
        if not data.get(SORT_ORDERS):
            data[SORT_ORDERS] = [UNSORTED_SORT_ORDER]
        return data

    def to_v2(self) -> TableMetadataV2:
        metadata = copy(self.model_dump())
        metadata["format-version"] = 2
        return TableMetadataV2.model_validate(metadata)

    format_version: Literal[1] = Field(alias="format-version")
    """An integer version number for the format. Currently, this can be 1 or 2
    based on the spec. Implementations must throw an exception if a table’s
    version is higher than the supported version."""

    schema_: Schema = Field(alias="schema")
    """The table’s current schema. (Deprecated: use schemas and
    current-schema-id instead)."""

    partition_spec: List[Dict[str, Any]] = Field(alias="partition-spec")
    """The table’s current partition spec, stored as only fields.
    Note that this is used by writers to partition data, but is
    not used when reading because reads use the specs stored in
    manifest files. (Deprecated: use partition-specs and default-spec-id
    instead)."""


class TableMetadataV2(TableMetadataCommonFields, IcebergBaseModel):
    """Represents version 2 of the Table Metadata.

    This extends Version 1 with row-level deletes, and adds some additional
    information to the schema, such as all the historical schemas, partition-specs,
    sort-orders.

    For more information:
    https://iceberg.apache.org/spec/#version-2-row-level-deletes
    """

    @model_validator(mode="before")
    def cleanup_snapshot_id(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        return cleanup_snapshot_id(data)

    @model_validator(mode="after")
    def check_schemas(cls, table_metadata: TableMetadata) -> TableMetadata:
        return check_schemas(table_metadata)

    @model_validator(mode="after")
    def check_partition_specs(cls, table_metadata: TableMetadata) -> TableMetadata:
        return check_partition_specs(table_metadata)

    @model_validator(mode="after")
    def check_sort_orders(cls, table_metadata: TableMetadata) -> TableMetadata:
        return check_sort_orders(table_metadata)

    @model_validator(mode="after")
    def construct_refs(cls, table_metadata: TableMetadata) -> TableMetadata:
        return construct_refs(table_metadata)

    format_version: Literal[2] = Field(alias="format-version", default=2)
    """An integer version number for the format. Currently, this can be 1 or 2
    based on the spec. Implementations must throw an exception if a table’s
    version is higher than the supported version."""

    last_sequence_number: int = Field(alias="last-sequence-number", default=INITIAL_SEQUENCE_NUMBER)
    """The table’s highest assigned sequence number, a monotonically
    increasing long that tracks the order of snapshots in a table."""


TableMetadata = Annotated[Union[TableMetadataV1, TableMetadataV2], Field(discriminator="format_version")]


def new_table_metadata(
    schema: Schema,
    partition_spec: PartitionSpec,
    sort_order: SortOrder,
    location: str,
    properties: Properties = EMPTY_DICT,
    table_uuid: Optional[uuid.UUID] = None,
) -> TableMetadata:
    fresh_schema = assign_fresh_schema_ids(schema)
    fresh_partition_spec = assign_fresh_partition_spec_ids(partition_spec, schema, fresh_schema)
    fresh_sort_order = assign_fresh_sort_order_ids(sort_order, schema, fresh_schema)

    if table_uuid is None:
        table_uuid = uuid.uuid4()

    return TableMetadataV2(
        location=location,
        schemas=[fresh_schema],
        last_column_id=fresh_schema.highest_field_id,
        current_schema_id=fresh_schema.schema_id,
        partition_specs=[fresh_partition_spec],
        default_spec_id=fresh_partition_spec.spec_id,
        sort_orders=[fresh_sort_order],
        default_sort_order_id=fresh_sort_order.order_id,
        properties=properties,
        last_partition_id=fresh_partition_spec.last_assigned_field_id,
        table_uuid=table_uuid,
    )


class TableMetadataWrapper(IcebergRootModel[TableMetadata]):
    root: TableMetadata


class TableMetadataUtil:
    """Helper class for parsing TableMetadata."""

    @staticmethod
    def parse_raw(data: str) -> TableMetadata:
        try:
            return TableMetadataWrapper.model_validate_json(data).root
        except PydanticValidationError as e:
            raise ValidationError(e) from e

    @staticmethod
    def parse_obj(data: Dict[str, Any]) -> TableMetadata:
        if "format-version" not in data:
            raise ValidationError(f"Missing format-version in TableMetadata: {data}")
        format_version = data["format-version"]

        if format_version == 1:
            return TableMetadataV1(**data)
        elif format_version == 2:
            return TableMetadataV2(**data)
        else:
            raise ValidationError(f"Unknown format version: {format_version}")


TableMetadata = Annotated[Union[TableMetadataV1, TableMetadataV2], Field(discriminator="format_version")]  # type: ignore
