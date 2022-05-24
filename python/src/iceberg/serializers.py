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

from iceberg.io.base import InputFile, OutputFile
from iceberg.table.metadata import TableMetadata


class FromDict:
    """A collection of methods that deserialize dictionaries into Iceberg objects"""

    @staticmethod
    def table_metadata(d: dict) -> TableMetadata:
        """Instantiates a TableMetadata object using a dictionary

        Args:
            d: A dictionary object that conforms to table metadata specification
        Returns:
            TableMetadata: A table metadata instance
        """
        return TableMetadata(  # type: ignore
            format_version=d.get("format-version"),  # type: ignore
            table_uuid=d.get("table-uuid"),  # type: ignore
            location=d.get("location"),  # type: ignore
            last_sequence_number=d.get("last-sequence-number"),  # type: ignore
            last_updated_ms=d.get("last-updated-ms"),  # type: ignore
            last_column_id=d.get("last-column-id"),  # type: ignore
            schema=d.get("schema"),  # type: ignore
            schemas=d.get("schemas"),  # type: ignore
            current_schema_id=d.get("current-schema-id"),  # type: ignore
            partition_spec=d.get("partition-spec") or [],  # type: ignore
            partition_specs=d.get("partition-specs") or [],  # type: ignore
            default_spec_id=d.get("default-spec-id"),  # type: ignore
            last_partition_id=d.get("last-partition-id"),  # type: ignore
            properties=d.get("properties") or {},  # type: ignore
            current_snapshot_id=d.get("current-snapshot-id"),  # type: ignore
            snapshots=d.get("snapshots") or [],  # type: ignore
            snapshot_log=d.get("snapshot-log") or [],  # type: ignore
            metadata_log=d.get("metadata-log") or [],  # type: ignore
            sort_orders=d.get("sort-orders") or [],  # type: ignore
            default_sort_order_id=d.get("default-sort-order-id"),  # type: ignore
        )  # type: ignore


class ToDict:
    """A collection of methods that serialize Iceberg objects into dictionaries"""

    @staticmethod
    def table_metadata(metadata: TableMetadata) -> dict:
        """Generate a dictionary representation of a TableMetadata instance

        Returns:
            dict: A dictionary representation of a TableMetadata instance
        """
        d = {
            "format-version": metadata.format_version,
            "table-uuid": metadata.table_uuid,
            "location": metadata.location,
            "last-updated-ms": metadata.last_updated_ms,
            "last-column-id": metadata.last_column_id,
            "schemas": metadata.schemas,
            "current-schema-id": metadata.current_schema_id,
            "partition-specs": metadata.partition_specs,
            "default-spec-id": metadata.default_spec_id,
            "last-partition-id": metadata.last_partition_id,
            "properties": metadata.properties,
            "current-snapshot-id": metadata.current_snapshot_id,
            "snapshots": metadata.snapshots,
            "snapshot-log": metadata.snapshot_log,
            "metadata-log": metadata.metadata_log,
            "sort-orders": metadata.sort_orders,
            "default-sort-order-id": metadata.default_sort_order_id,
        }

        if metadata.format_version == 1:
            d["schema"] = metadata.schema
            d["partition-spec"] = metadata.partition_spec
        if metadata.format_version == 2:
            d["last-sequence-number"] = metadata.last_sequence_number

        return d


class FromByteStream:
    """A collection of methods that deserialize dictionaries into Iceberg objects"""

    @staticmethod
    def table_metadata(byte_stream, encoding: str = "utf-8") -> TableMetadata:
        """Instantiate a TableMetadata object from a byte stream

        Args:
            byte_stream: A file-like byte stream object
            encoding (default "utf-8"): The byte encoder to use for the reader
        """
        reader = codecs.getreader(encoding)
        metadata = json.load(reader(byte_stream))
        return FromDict.table_metadata(metadata)


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
        return FromByteStream.table_metadata(byte_stream=input_file.open(), encoding=encoding)


class ToOutputFile:
    """A collection of methods that serialize Iceberg objects into files given an OutputFile instance"""

    @staticmethod
    def table_metadata(metadata: TableMetadata, output_file: OutputFile, overwrite: bool = False) -> None:
        """Write a TableMetadata instance to an output file

        Args:
            output_file (OutputFile): A custom implementation of the iceberg.io.file.OutputFile abstract base class
            overwrite (bool): Where to overwrite the file if it already exists. Defaults to `False`.
        """
        f = output_file.create(overwrite=overwrite)
        f.write(json.dumps(ToDict.table_metadata(metadata)).encode("utf-8"))
