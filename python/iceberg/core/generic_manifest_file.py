# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from iceberg.api import (ManifestFile,
                         StructLike)


class GenericManifestFile(ManifestFile, StructLike):

    @staticmethod
    def generic_manifest_from_file(file, spec_id):
        return GenericManifestFile(file.path, None, spec_id, None,
                                   None, None, None,
                                   None)

    @staticmethod
    def generic_manifest_from_path(path, length, spec_id, snapshot_id,
                                   added_files_count, existing_files_count, deleted_files_count,
                                   partitions):
        return GenericManifestFile(path, length, spec_id, snapshot_id,
                                   added_files_count, existing_files_count, deleted_files_count,
                                   partitions)

    def __init__(self, path, length, spec_id, snapshot_id,
                 added_files_count, existing_files_count, deleted_files_count,
                 partitions):
        self.manifest_path = path
        self.length = length
        self.spec_id = spec_id
        self.snapshot_id = snapshot_id
        self.added_files_count = added_files_count
        self.existing_files_count = existing_files_count
        self.deleted_files_count = deleted_files_count
        self.partitions = partitions
        self.from_projection_pos = None
