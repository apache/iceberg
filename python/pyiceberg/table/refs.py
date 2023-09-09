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
from typing import Optional

from pydantic import Field

from pyiceberg.typedef import IcebergBaseModel

MAIN_BRANCH = "main"


class SnapshotRefType(str, Enum):
    BRANCH = "branch"
    TAG = "tag"

    def __repr__(self) -> str:
        """Return the string representation of the SnapshotRefType class."""
        return f"SnapshotRefType.{self.name}"


class SnapshotRef(IcebergBaseModel):
    snapshot_id: int = Field(alias="snapshot-id")
    snapshot_ref_type: SnapshotRefType = Field(alias="type")
    min_snapshots_to_keep: Optional[int] = Field(alias="min-snapshots-to-keep", default=None)
    max_snapshot_age_ms: Optional[int] = Field(alias="max-snapshot-age-ms", default=None)
    max_ref_age_ms: Optional[int] = Field(alias="max-ref-age-ms", default=None)
