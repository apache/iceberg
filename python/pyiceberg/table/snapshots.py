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
    Optional,
)

from pydantic import Field, PrivateAttr, model_serializer

from pyiceberg.io import FileIO
from pyiceberg.manifest import ManifestFile, read_manifest_list
from pyiceberg.typedef import IcebergBaseModel

OPERATION = "operation"


class Operation(Enum):
    """Describes the operation.

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
        """Return the string representation of the Operation class."""
        return f"Operation.{self.name}"


class Summary(IcebergBaseModel):
    """A class that stores the summary information for a Snapshot.

    The snapshot summary’s operation field is used by some operations,
    like snapshot expiration, to skip processing certain snapshots.
    """

    operation: Operation = Field()
    _additional_properties: Dict[str, str] = PrivateAttr()

    def __init__(self, operation: Operation, **data: Any) -> None:
        super().__init__(operation=operation, **data)
        self._additional_properties = data

    @model_serializer
    def ser_model(self) -> Dict[str, str]:
        return {
            "operation": str(self.operation.value),
            **self._additional_properties,
        }

    @property
    def additional_properties(self) -> Dict[str, str]:
        return self._additional_properties

    def __repr__(self) -> str:
        """Return the string representation of the Summary class."""
        repr_properties = f", **{repr(self._additional_properties)}" if self._additional_properties else ""
        return f"Summary({repr(self.operation)}{repr_properties})"


class Snapshot(IcebergBaseModel):
    snapshot_id: int = Field(alias="snapshot-id")
    parent_snapshot_id: Optional[int] = Field(alias="parent-snapshot-id", default=None)
    sequence_number: Optional[int] = Field(alias="sequence-number", default=None)
    timestamp_ms: int = Field(alias="timestamp-ms")
    manifest_list: Optional[str] = Field(
        alias="manifest-list", description="Location of the snapshot's manifest list file", default=None
    )
    summary: Optional[Summary] = Field(default=None)
    schema_id: Optional[int] = Field(alias="schema-id", default=None)

    def __str__(self) -> str:
        """Return the string representation of the Snapshot class."""
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


class MetadataLogEntry(IcebergBaseModel):
    metadata_file: str = Field(alias="metadata-file")
    timestamp_ms: int = Field(alias="timestamp-ms")


class SnapshotLogEntry(IcebergBaseModel):
    snapshot_id: int = Field(alias="snapshot-id")
    timestamp_ms: int = Field(alias="timestamp-ms")
