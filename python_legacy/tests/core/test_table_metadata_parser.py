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

import binascii

from iceberg.core import ConfigProperties, TableMetadataParser
from iceberg.core.filesystem import FileSystemInputFile, FileSystemOutputFile


def test_compression_property(expected, prop):
    config = {ConfigProperties.COMPRESS_METADATA: prop}
    output_file = FileSystemOutputFile.from_path(TableMetadataParser.get_file_extension(config), dict)
    TableMetadataParser.write(expected, output_file)
    assert prop == is_compressed(TableMetadataParser.get_file_extension(config))
    read = TableMetadataParser.read(None,
                                    FileSystemInputFile.from_location(TableMetadataParser.get_file_extension(config), None))
    verify_metadata(read, expected)


def verify_metadata(read, expected):
    assert expected.schema.as_struct() == read.schema.as_struct()
    assert expected.location == read.location
    assert expected.last_column_id == read.last_column_id
    assert expected.properties == read.properties


def is_compressed(file):
    with open(file, 'rb') as test_f:
        return binascii.hexlify(test_f.read(2)) == b'1f8b'
