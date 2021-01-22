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

import logging
from pathlib import Path
import uuid

from iceberg.exceptions import CommitFailedException, ValidationException

from .file_system import FileSystemInputFile, FileSystemOutputFile
from .util import get_fs
from ..table_metadata_parser import TableMetadataParser
from ..table_operations import TableOperations
from ..table_properties import TableProperties

_logger = logging.getLogger(__name__)


class FilesystemTableOperations(TableOperations):

    def __init__(self, location, conf):
        self.conf = conf
        self.location = Path(location)
        self.should_refresh = True
        self.version = None
        self.current_metadata = None

    def current(self):
        if self.should_refresh:
            return self.refresh()

        return self.current_metadata

    def refresh(self):
        ver = self.version if self.version is not None else self.read_version_hint()
        metadata_file = self.metadata_file(ver)
        fs = get_fs(str(metadata_file), self.conf)

        if ver is not None and not fs.exists(metadata_file):
            if ver == 0:
                return None
            raise ValidationException("Metadata file is missing: %s" % metadata_file)

        while fs.exists(str(self.metadata_file(ver + 1))):
            ver += 1
            metadata_file = self.metadata_file(ver)

        self.version = ver
        self.current_metadata = TableMetadataParser.read(self, FileSystemInputFile.from_location(str(metadata_file),
                                                                                                 self.conf))
        self.should_refresh = False
        return self.current_metadata

    def commit(self, base, metadata):
        if base != self.current():
            raise CommitFailedException("Cannot commit changes based on stale table metadata")

        if not (base is None or base.location() == metadata.location()):
            raise RuntimeError("Hadoop path-based tables cannot be relocated")
        if TableProperties.WRITE_METADATA_LOCATION in metadata.properties:
            raise RuntimeError("Hadoop path-based tables cannot be relocated")

        temp_metadata_file = self.metadata_path("{}{}".format(uuid.uuid4(),
                                                              TableMetadataParser.get_file_extension(self.conf)))
        TableMetadataParser.write(metadata, FileSystemOutputFile.from_path(str(temp_metadata_file), self.conf))

        next_version = (self.version if self.version is not None else 0) + 1
        final_metadata_file = self.metadata_file(next_version)
        fs = get_fs(str(final_metadata_file), self.conf)

        if fs.exists(final_metadata_file):
            raise CommitFailedException("Version %s already exists: %s" % (next_version, final_metadata_file))

        if not fs.rename(temp_metadata_file, final_metadata_file):
            raise CommitFailedException("Failed to commit changes using rename: %s" % final_metadata_file)

        self.write_version_hint(next_version)
        self.should_refresh = True

    def new_input_file(self, path):
        return FileSystemInputFile.from_location(path, self.conf)

    def new_output_file(self, path):
        return FileSystemOutputFile.from_path(path, self.conf)

    def new_metadata_file(self, filename):
        raise RuntimeError("Not yet implemented")

    def delete_file(self, path):
        get_fs(path, self.conf).delete(path)

    def new_snapshot_id(self):
        raise RuntimeError("Not yet implemented")

    def metadata_file_location(self, file):
        return str(self.metadata_path(file))

    def metadata_file(self, version):
        return self.metadata_path("v{}{}".format(version, TableMetadataParser.get_file_extension(self.conf)))

    def metadata_path(self, filename):
        return self.location / Path("metadata") / Path(filename)

    def version_hint_file(self):
        return self.metadata_path("version-hint.text")

    def read_version_hint(self):
        version_hint_file = str(self.version_hint_file())
        fs = get_fs(version_hint_file, self.conf)

        if not fs.exists(version_hint_file):
            return 0
        else:
            with fs.open(version_hint_file, "r") as fo:
                return int(fo.readline().replace("\n", ""))

    def write_version_hint(self, version):
        version_hint_file = str(self.version_hint_file())
        fs = get_fs(version_hint_file, self.conf)
        try:

            with fs.create(version_hint_file, True) as fo:
                fo.write("{}".format(version))

        except RuntimeError as e:
            _logger.warning("Unable to update version hint: %s" % e)
