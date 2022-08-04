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

import codecs
import json
from typing import Union

from pyiceberg.io.base import InputFile, InputStream, OutputFile
from pyiceberg.table.metadata import TableMetadata, TableMetadataV1, TableMetadataV2


class FromByteStream:
    """A collection of methods that deserialize dictionaries into Iceberg objects"""

    @staticmethod
    def table_metadata(byte_stream: InputStream, encoding: str = "utf-8") -> TableMetadata:
        """Instantiate a TableMetadata object from a byte stream

        Args:
            byte_stream: A file-like byte stream object
            encoding (default "utf-8"): The byte encoder to use for the reader
        """
        reader = codecs.getreader(encoding)
        metadata = json.load(reader(byte_stream))  # type: ignore
        return TableMetadata.parse_obj(metadata)  # type: ignore


class FromInputFile:
    """A collection of methods that deserialize InputFiles into Iceberg objects"""

    @staticmethod
    def table_metadata(input_file: InputFile, encoding: str = "utf-8") -> TableMetadata:
        """Create a TableMetadata instance from an input file

        Args:
            input_file (InputFile): A custom implementation of the iceberg.io.file.InputFile abstract base class
            encoding (str): Encoding to use when loading bytestream

        Returns:
            TableMetadata: A table metadata instance

        """
        # 😢 https://github.com/apache/iceberg/pull/5436
        # with PyArrowFile(metadata_location) as file:
        #     metadata = TableMetadata.parse_obj(json.loads(file.read()))
        f = input_file.open()
        metadata = FromByteStream.table_metadata(byte_stream=f, encoding=encoding)
        f.close()
        return metadata


class ToOutputFile:
    """A collection of methods that serialize Iceberg objects into files given an OutputFile instance"""

    @staticmethod
    def table_metadata(
        metadata: Union[TableMetadataV1, TableMetadataV2], output_file: OutputFile, overwrite: bool = False
    ) -> None:
        """Write a TableMetadata instance to an output file

        Args:
            output_file (OutputFile): A custom implementation of the iceberg.io.file.OutputFile abstract base class
            overwrite (bool): Where to overwrite the file if it already exists. Defaults to `False`.
        """
        f = output_file.create(overwrite=overwrite)
        f.write(metadata.json().encode("utf-8"))
        f.close()
