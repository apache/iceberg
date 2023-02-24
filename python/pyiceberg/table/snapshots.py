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
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Union,
)

from pydantic import Field, PrivateAttr, root_validator

from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFile, ManifestFile, read_manifest_list
from pyiceberg.schema import Schema
from pyiceberg.typedef import IcebergBaseModel

if TYPE_CHECKING:
    from pyiceberg.table import Table

OPERATION = "operation"


class Operation(Enum):
    """Describes the operation

    Possible operation values are:
        - append: Only data files were added and no files were removed.
        - replace: Data and delete files were added and removed without changing table data; i.e., compaction, changing the data file format, or relocating data files.
        - overwrite: Data and delete files were added and removed in a logical overwrite operation.
        - delete: Data files were removed and their contents logically deleted and/or delete files were added to delete rows.
    """

    APPEND = "append"
    REPLACE = "replace"
    OVERWRITE = "overwrite"
    DELETE = "delete"

    def __repr__(self) -> str:
        return f"Operation.{self.name}"


class Summary(IcebergBaseModel):
    """
    The snapshot summary’s operation field is used by some operations,
    like snapshot expiration, to skip processing certain snapshots.
    """

    __root__: Dict[str, Union[str, Operation]]
    _additional_properties: Dict[str, str] = PrivateAttr()

    @root_validator
    def check_operation(cls, values: Dict[str, Dict[str, Union[str, Operation]]]) -> Dict[str, Dict[str, Union[str, Operation]]]:
        if operation := values["__root__"].get(OPERATION):
            if isinstance(operation, str):
                values["__root__"][OPERATION] = Operation(operation.lower())
        else:
            raise ValueError("Operation not set")
        return values

    def __init__(
        self, operation: Optional[Operation] = None, __root__: Optional[Dict[str, Union[str, Operation]]] = None, **data: Any
    ) -> None:
        super().__init__(__root__={"operation": operation, **data} if not __root__ else __root__)
        self._additional_properties = {
            k: v for k, v in self.__root__.items() if k != OPERATION  # type: ignore # We know that they are all string, and we don't want to check
        }

    @property
    def operation(self) -> Operation:
        operation = self.__root__[OPERATION]
        if isinstance(operation, Operation):
            return operation
        else:
            # Should never happen
            raise ValueError(f"Unknown type of operation: {operation}")

    @property
    def additional_properties(self) -> Dict[str, str]:
        return self._additional_properties

    def __repr__(self) -> str:
        repr_properties = f", **{repr(self._additional_properties)}" if self._additional_properties else ""
        return f"Summary({repr(self.operation)}{repr_properties})"


class Snapshot(IcebergBaseModel):
    snapshot_id: int = Field(alias="snapshot-id")
    parent_snapshot_id: Optional[int] = Field(alias="parent-snapshot-id")
    sequence_number: Optional[int] = Field(alias="sequence-number", default=None)
    timestamp_ms: int = Field(alias="timestamp-ms")
    manifest_list: Optional[str] = Field(alias="manifest-list", description="Location of the snapshot's manifest list file")
    summary: Optional[Summary] = Field()
    schema_id: Optional[int] = Field(alias="schema-id", default=None)

    def __str__(self) -> str:
        operation = f"{self.summary.operation}: " if self.summary else ""
        parent_id = f", parent_id={self.parent_snapshot_id}" if self.parent_snapshot_id else ""
        schema_id = f", schema_id={self.schema_id}" if self.schema_id is not None else ""
        result_str = f"{operation}id={self.snapshot_id}{parent_id}{schema_id}"
        return result_str

    def manifests(self, io: FileIO) -> List[ManifestFile]:
        if self.manifest_list is not None:
            file = io.new_input(self.manifest_list)
            return list(read_manifest_list(file))
        return []

    def added_data_files(self, io: FileIO) -> Generator[DataFile, None, None]:
        for manifest in self.manifests(io):
            yield from [entry.data_file for entry in manifest.fetch_manifest_entry(io)]


class MetadataLogEntry(IcebergBaseModel):
    metadata_file: str = Field(alias="metadata-file")
    timestamp_ms: int = Field(alias="timestamp-ms")


class SnapshotLogEntry(IcebergBaseModel):
    snapshot_id: int = Field(alias="snapshot-id")
    timestamp_ms: int = Field(alias="timestamp-ms")


def is_ancestor_of(table: "Table", snapshot_id: int, ancestor_snapshot_id: int) -> bool:
    """
    Returns whether ancestor_snapshot_id is an ancestor of snapshot_id using the given lookup function.

    Args:
        table: The table
        snapshot_id: The snapshot id of the snapshot
        ancestor_snapshot_id: The snapshot id of the possible ancestor

    Returns:
        True if it is an ancestor or not
    """
    snapshots = ancestors_of(snapshot_id, table.snapshot_by_id)
    for snapshot in snapshots:
        if snapshot.snapshot_id == ancestor_snapshot_id:
            return True
    return False


def is_parent_ancestor_of(table: "Table", snapshot_id: int, ancestor_parent_snapshot_id: int) -> bool:
    """
    Returns whether some ancestor of snapshot_id has parent_id matches ancestor_parent_snapshot_id

    Args:
        table: The table
        snapshot_id: The snapshot id of the snapshot
        ancestor_parent_snapshot_id: The snapshot id of the possible parent ancestor

    Returns:
        True if there is an ancestor with a parent
    """
    snapshots = ancestors_of(snapshot_id, table.snapshot_by_id)
    for snapshot in snapshots:
        if snapshot.parent_snapshot_id == ancestor_parent_snapshot_id:
            return True
    return False


def current_ancestors(table: "Table") -> Iterable[Snapshot]:
    """
    Returns an iterable that traverses the table's snapshots from the current to the last known ancestor.

    Args:
        table: The table

    Returns:
        An iterable of all the ancestors
    """
    if current_snapshot := table.current_snapshot():
        return ancestors_of(current_snapshot, table.snapshot_by_id)
    return []


def current_ancestor_ids(table: "Table") -> Iterable[int]:
    """
    Return the snapshot IDs for the ancestors of the current table state.

    Ancestor IDs are ordered by commit time, descending. The first ID is
    the current snapshot, followed by its parent, and so on.

    Args:
        table: The table

    Returns:
        An iterable of all the snapshot IDs
    """
    if current_snapshot := table.current_snapshot():
        return ancestor_ids(current_snapshot, table.snapshot_by_id)
    return []


def oldest_ancestor(table: "Table") -> Optional[Snapshot]:
    """
    Traverses the history of the table's current snapshot and finds the oldest Snapshot.

    Args:
        table: The table

    Returns:
        None if there is no current snapshot in the table, else the oldest Snapshot.
    """
    oldest_snapshot: Optional[Snapshot] = None

    for snapshot in current_ancestors(table):
        oldest_snapshot = snapshot

    return oldest_snapshot


def oldest_ancestor_of(table: "Table", snapshot_id: int) -> Optional[Snapshot]:
    """
    Traverses the history and finds the oldest ancestor of the specified snapshot.

    Oldest ancestor is defined as the ancestor snapshot whose parent is null or has been
    expired. If the specified snapshot has no parent or parent has been expired, the specified
    snapshot itself is returned.

    Args:
        table: The table
        snapshot_id: the ID of the snapshot to find the oldest ancestor

    Returns:
        None if there is no current snapshot in the table, else the oldest Snapshot.
    """
    oldest_snapshot: Optional[Snapshot] = None

    for snapshot in ancestors_of(snapshot_id, table.snapshot_by_id):
        oldest_snapshot = snapshot

    return oldest_snapshot


def oldest_ancestor_after(table: "Table", timestamp_ms: int) -> Snapshot:
    """
    Looks up the snapshot after a given point in time

    Args:
        table: The table
        timestamp_ms: The timestamp in millis since the Unix epoch

    Returns:
        The snapshot after the given point in time

    Raises:
        ValueError: When there is no snapshot older than the given time
    """
    if last_snapshot := table.current_snapshot():
        for snapshot in current_ancestors(table):
            if snapshot.timestamp_ms < timestamp_ms:
                return last_snapshot
            elif snapshot.timestamp_ms == timestamp_ms:
                return snapshot

            last_snapshot = snapshot

        if last_snapshot is not None and last_snapshot.parent_snapshot_id is None:
            return last_snapshot

    raise ValueError(f"Cannot find snapshot older than: {timestamp_ms}")


def snapshots_ids_between(table: "Table", from_snapshot_id: int, to_snapshot_id: int) -> Iterable[int]:
    """
    Returns list of snapshot ids in the range - (fromSnapshotId, toSnapshotId]

    This method assumes that fromSnapshotId is an ancestor of toSnapshotId.

    Args:
        table: The table
        from_snapshot_id: The starting snapshot ID
        to_snapshot_id: The ending snapshot ID

    Returns:
        The list of snapshot IDs that are between the given snapshot IDs
    """

    def lookup(snapshot_id: int) -> Optional[Snapshot]:
        return table.snapshot_by_id(snapshot_id) if snapshot_id != from_snapshot_id else None

    return ancestor_ids(table.snapshot_by_id(snapshot_id=to_snapshot_id), lookup)


def ancestor_ids(latest_snapshot: Union[int, Snapshot], lookup: Callable[[int], Optional[Snapshot]]) -> Iterable[int]:
    """
    Returns list of the snapshot IDs of the ancestors

    Args:
        latest_snapshot: The snapshot where to start from
        lookup: Lookup function to get the snapshot for the snapshot ID

    Returns:
        The list of snapshot IDs that are ancestor of the given snapshot
    """

    def get_id(snapshot: Snapshot) -> int:
        return snapshot.snapshot_id

    return map(get_id, ancestors_of(latest_snapshot, lookup))


def ancestors_of(latest_snapshot: Union[int, Snapshot], lookup: Callable[[int], Optional[Snapshot]]) -> Iterable[Snapshot]:
    """
    Returns list of snapshot that are ancestor of the given snapshot

    Args:
        latest_snapshot: The snapshot where to start from
        lookup: Lookup function to get the snapshot for the snapshot ID

    Returns:
        The list of snapshots that are ancestor of the given snapshot
    """
    if isinstance(latest_snapshot, int):
        start = lookup(latest_snapshot)
        if start is None:
            raise ValueError(f"Cannot find snapshot: {latest_snapshot}")
    else:
        start = latest_snapshot

    def snapshot_generator() -> Generator[Snapshot, None, None]:
        next_snapshot_id = start.snapshot_id  # type: ignore
        # https://github.com/apache/iceberg/issues/6930
        # next = start.parent_snapshot_id
        while next_snapshot_id is not None:
            if snap := lookup(next_snapshot_id):
                yield snap
                next_snapshot_id = snap.parent_snapshot_id
            else:
                break

    return snapshot_generator()


def ancestors_between(
    latest_snapshot_id: int, oldest_snapshot_id: Optional[int], lookup: Callable[[int], Snapshot]
) -> Iterable[Snapshot]:
    """
    Returns list of snapshot that are ancestor between two IDs

    Args:
        latest_snapshot_id: The latest snapshot
        oldest_snapshot_id: The oldest snapshot
        lookup: Lookup function to get the snapshot for the snapshot ID

    Returns:
        The list of snapshots that are ancestor between the two IDs
    """
    if oldest_snapshot_id is not None:
        if latest_snapshot_id == oldest_snapshot_id:
            return []

        def lookup_callable(snapshot_id: int) -> Optional[Snapshot]:
            return lookup(snapshot_id) if oldest_snapshot_id != snapshot_id else None

        return ancestors_of(latest_snapshot_id, lookup_callable)
    return ancestors_of(latest_snapshot_id, lookup)


def new_files(
    base_snapshot_id: int, latest_snapshot_id: int, lookup: Callable[[int], Snapshot], io: FileIO
) -> Iterable[DataFile]:
    """
    Returns list of DataFiles that are added along the way

    Args:
        base_snapshot_id: The latest snapshot
        latest_snapshot_id: The oldest snapshot
        lookup: Lookup function to get the snapshot for the snapshot ID
        io: FileIO to fetch files

    Returns:
        List of DataFiles that are added along the way
    """
    added_files: List[DataFile] = []

    last_snapshot: Optional[Snapshot] = None

    for snapshot in ancestors_of(latest_snapshot_id, lookup):
        last_snapshot = snapshot

        if snapshot.snapshot_id == base_snapshot_id:
            return added_files

        added_files += list(snapshot.added_data_files(io))

    if last_snapshot is not None and last_snapshot.snapshot_id != base_snapshot_id:
        raise ValueError(
            f"Cannot determine history between read snapshot {base_snapshot_id} and the last known ancestor {last_snapshot}"
        )

    return added_files


def snapshot_after(table: "Table", snapshot_id: int) -> Snapshot:
    """Traverses the history of the table's current snapshot
    and finds the snapshot with the given snapshot id as its parent.

    Args:
        table: The table
        snapshot_id: The snapshot ID

    Returns:
        the snapshot for which the given snapshot is the parent

    Raises:
        ValueError: When the snapshot isn't found
    """
    for current in current_ancestors(table):
        if current.parent_snapshot_id == snapshot_id:
            return current

    raise ValueError(f"Cannot find snapshot after {snapshot_id}: not an ancestor of table's current snapshot")


def snapshot_id_as_of_time(table: "Table", timestamp_ms: int) -> int:
    """
    Returns the ID of the most recent snapshot for the table as of the timestamp.

    Args:
        table: The table
        timestamp_ms: the timestamp in millis since the Unix epoch

    Returns:
        The snapshot ID

    Raises:
        ValueError: When the snapshot id cannot be found or there are no snapshots older
                    than the given timestamp.
    """
    snapshot_id = None

    for snapshot in current_ancestors(table):
        if snapshot.timestamp_ms <= timestamp_ms:
            snapshot_id = snapshot.snapshot_id

    if snapshot_id is None:
        raise ValueError(f"Cannot find a snapshot older than: {timestamp_ms}")

    return snapshot_id


def schema_for(table: "Table", snapshot_id: int) -> Schema:
    """Returns the schema of the table for the specified snapshot.

    Args:
        table: The table
        snapshot_id: The snapshot ID that will match with the schema

    Returns:
        The schema of the snapshot, if available, otherwise the table schema
    """
    snapshot = table.snapshot_by_id(snapshot_id)
    schema_id = snapshot.schema_id

    if schema_id is not None:
        return table.schema_by_id(schema_id)

    return table.schema()


def schema_as_of_time(table: "Table", timestamp_ms: int) -> Schema:
    """Returns the schema of the table for the specified snapshot.

    Args:
        table: The table
        timestamp_ms: The timestamp in millis since the Unix epoch

    Returns:
        The schema of the snapshot, if available, otherwise the table schema
    """
    snapshot_id = snapshot_id_as_of_time(table, timestamp_ms)
    return schema_for(table, snapshot_id)
