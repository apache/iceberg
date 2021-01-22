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


import json

from iceberg.api import (ManifestFile,
                         StructLike)
from iceberg.core.avro import IcebergToAvro

from .generic_partition_field_summary import GenericPartitionFieldSummary


class GenericManifestFile(ManifestFile, StructLike):
    AVRO_SCHEMA = IcebergToAvro.type_to_schema(ManifestFile.schema().as_struct(), "manifest_file")

    GET_POS_MAP = {0: lambda curr_manifest: curr_manifest.manifest_path,
                   1: lambda curr_manifest: curr_manifest.lazy_length(),
                   2: lambda curr_manifest: curr_manifest.spec_id,
                   3: lambda curr_manifest: curr_manifest.snapshot_id,
                   4: lambda curr_manifest: curr_manifest.added_files_count,
                   5: lambda curr_manifest: curr_manifest.existing_files_count,
                   6: lambda curr_manifest: curr_manifest.deleted_files_count,
                   7: lambda curr_manifest: curr_manifest.partitions}

    @staticmethod
    def generic_manifest_from_file(file, spec_id):
        return GenericManifestFile(file=file, spec_id=spec_id)

    @staticmethod
    def generic_manifest_from_path(path, length, spec_id, snapshot_id,
                                   added_files_count, existing_files_count, deleted_files_count,
                                   partitions):
        return GenericManifestFile(path, length, spec_id, snapshot_id,
                                   added_files_count, existing_files_count, deleted_files_count,
                                   partitions)

    def __init__(self, path=None, file=None, spec_id=None, length=None, snapshot_id=None,
                 added_files_count=None, existing_files_count=None, deleted_files_count=None,
                 partitions=None):
        if file is not None:
            self.file = file
            self.manifest_path = file.location()
        else:
            self.manifest_path = path

        self._length = length
        self.spec_id = spec_id
        self.snapshot_id = snapshot_id
        self._added_files_count = added_files_count
        self._existing_files_count = existing_files_count
        self._deleted_files_count = deleted_files_count
        self.partitions = partitions
        self.from_projection_pos = None

    @property
    def length(self):
        return self.lazy_length()

    @property
    def added_files_count(self):
        return self._added_files_count

    @property
    def existing_files_count(self):
        return self._existing_files_count

    @property
    def deleted_files_count(self):
        return self._deleted_files_count

    def lazy_length(self):
        if self._length is None:
            if self.file is not None:
                self._length = self.file.get_length()
            else:
                return None
        else:
            return self._length

    def size(self):
        return len(ManifestFile.schema().columns())

    def get(self, pos, cast_type=None):
        if self.from_projection_pos:
            pos = self.from_projection_pos[pos]

        get_func = GenericManifestFile.GET_POS_MAP.get(pos)
        if get_func is None:
            raise RuntimeError("Unknown field ordinal: %s" % pos)

        if cast_type is not None:
            return cast_type(get_func(self))

        return get_func(self)

    def set(self, pos, value):
        if self.from_projection_pos:
            pos = self.from_projection_pos[pos]

        if pos == 0:
            self.manifest_path = str(value)
        elif pos == 1:
            self._length = int(value)
        elif pos == 2:
            self.spec_id = int(value)
        elif pos == 3:
            self.snapshot_id = int(value)
        elif pos == 4:
            self._added_files_count = int(value)
        elif pos == 5:
            self._existing_files_count = int(value)
        elif pos == 6:
            self._deleted_files_count = int(value)
        elif pos == 7:
            self.partitions = value

    def copy(self):
        return GenericManifestFile(path=self.manifest_path, spec_id=self.spec_id, length=self.length,
                                   snapshot_id=self.snapshot_id, added_files_count=self.added_files_count,
                                   existing_files_count=self.existing_files_count,
                                   deleted_files_count=self.deleted_files_count,
                                   partitions=list(self.partitions))

    @staticmethod
    def get_schema():
        return GenericManifestFile.AVRO_SCHEMA

    @staticmethod
    def to_avro_record_json(manifest):
        return json.dumps(GenericManifestFile.to_avro_record_dict(manifest))

    @staticmethod
    def to_avro_record_dict(manifest):
        return {"manifest_path": manifest.manifest_path,
                "manifest_length": manifest._length,
                "partition_spec_id": manifest.spec_id,
                "added_snapshot_id": manifest.snapshot_id,
                "added_data_files_count": manifest.added_files_count,
                "existing_data_files_count": manifest.existing_files_count,
                "deleted_data_files_count": manifest.deleted_files_count,
                "partitions": manifest.partitions}

    @staticmethod
    def from_avro_record_json(row):
        partitions = row.get("partitions")
        if partitions is not None:
            partitions = [GenericPartitionFieldSummary(contains_null=partition["contains_null"],
                                                       lower_bound=partition["lower_bound"],
                                                       upper_bound=partition["upper_bound"])
                          for partition in row.get("partitions")]
        return GenericManifestFile(path=row.get("manifest_path"),
                                   length=row.get("manifest_length"),
                                   spec_id=row.get("partition_spec_id"),
                                   added_files_count=row.get("added_data_files_count"),
                                   existing_files_count=row.get("existing_data_files_count"),
                                   deleted_files_count=row.get("existing_data_files_count"),
                                   partitions=partitions)

    def __eq__(self, other):
        if id(self) == id(other):
            return True

        if other is None or not isinstance(other, GenericManifestFile):
            return False

        return self.__key() == other.__key()

    def __hash__(self):
        return hash(self.__key())

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "GenericManifestFile({})".format(self.manifest_path)

    def __key(self):
        return (GenericManifestFile.__class__,
                self.manifest_path)
