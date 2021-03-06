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

from iceberg.api import Schema
from iceberg.api.types import (BinaryType,
                               BooleanType,
                               DateType,
                               DecimalType,
                               DoubleType,
                               FixedType,
                               FloatType,
                               IntegerType,
                               ListType,
                               LongType,
                               MapType,
                               NestedField,
                               StringType,
                               StructType,
                               TimestampType)
from iceberg.api.types import Type
import pyarrow as pa
from pyarrow.parquet import lib, ParquetFile

_logger = logging.getLogger(__name__)

arrow_type_map = {lib.Type_BOOL: lambda x=None: BooleanType.get(),
                  lib.Type_DATE32: lambda x=None: DateType.get(),
                  lib.Type_DECIMAL128: lambda x=None: DecimalType.of(x.precision, x.scale),
                  lib.Type_DOUBLE: lambda x=None: DoubleType.get(),
                  lib.Type_FIXED_SIZE_BINARY: lambda x=None: FixedType.of_length(x.byte_width),
                  lib.Type_BINARY: lambda x=None: BinaryType.get(),
                  lib.Type_FLOAT: lambda x=None: FloatType.get(),
                  lib.Type_STRING: lambda x=None: StringType.get(),
                  lib.Type_INT32: lambda x=None: IntegerType.get(),
                  lib.Type_INT64: lambda x=None: LongType.get(),
                  lib.Type_TIMESTAMP: lambda x=None: (TimestampType.without_timezone()
                                                      if x.tz is None
                                                      else TimestampType.with_timezone())
                  }


def get_nested_field(field_id: int, field_name: str, field_type: Type, nullable: bool) -> NestedField:
    if nullable:
        return NestedField.optional(field_id, field_name, field_type)
    else:
        return NestedField.required(field_id, field_name, field_type)


def get_list_field(col_type: pa.ListType) -> ListType:
    if col_type.value_field.nullable:
        return ListType.of_optional(int(col_type.value_field.metadata[b"PARQUET:field_id"].decode("utf-8")),
                                    arrow_type_map[col_type.value_field.type.id](col_type.value_field.type))
    else:
        return ListType.of_required(col_type.value_field.metadata[b"PARQUET:field_id"].decode("utf-8"),
                                    arrow_type_map[col_type.value_field.type.id](col_type.value_field.type))


def get_field(col: pa.Field) -> NestedField:
    try:
        return get_nested_field(int(col.metadata[b"PARQUET:field_id"].decode("utf-8")),
                                col.name,
                                arrow_type_map[col.type.id](col.type),
                                col.nullable)
    except KeyError:
        _logger.error(f"\t{int(col.metadata[b'PARQUET:field_id'].decode('utf-8'))}: {col.type}")
        raise


def get_struct_field(col_type: pa.StructType) -> StructType:
    if col_type.num_fields > 0:
        if col_type[0].name == "map":
            return get_inferred_map(col_type)
        else:
            return StructType.of([get_field(child) for child in col_type])
    else:
        raise RuntimeError("Unable to convert type to iceberg %s" % col_type)


def get_inferred_map(col_type: pa.StructType):
    base_field = col_type[0]
    key_field = get_field(col_type[0].type.value_field.type[0])
    value_field = get_field(col_type[0].type.value_field.type[1])

    if base_field.nullable:
        return MapType.of_optional(key_field.field_id, value_field.field_id,
                                   key_field.type, value_field.type)
    else:
        return MapType.of_required(key_field.field_id, value_field.field_id,
                                   key_field.type, value_field.type)


def get_map_field(col):
    # Spark Map type gets serialized to Struct<List<Struct<>>>
    raise NotImplementedError("Arrow Map type not implemented yet")


# adding function mappings for nested types
arrow_type_map.update({lib.Type_LIST: get_list_field,
                       lib.Type_MAP: get_map_field,
                       lib.Type_STRUCT: get_struct_field})


def convert_parquet_to_iceberg(parquet_file: ParquetFile) -> Schema:  # noqa: ignore=C901
    """
    Given two Iceberg schema's returns a list of column_names for all id's in the
    file schema that are projected in the expected schema

    Parameters
    ----------
    parquet_file : pyarrow.parquet.ParquetFile
        The Parquet File to use to extract the iceberg schema

    Returns
    -------
    iceberg.api.Schema
        returns an equivalent iceberg Schema based on the arrow schema read from the file
    """
    return arrow_to_iceberg(parquet_file.schema_arrow)


def arrow_to_iceberg(arrow_schema: pa.Schema) -> Schema:
    """
    Use an arrow schema, which contains the field_id metadata, to create an equivalent iceberg Schema

    Parameters
    ----------
    arrow_schema : pyarrow.Schema
        An Arrow schema with the parquet field_id metadata

    Returns
    -------
    iceberg.api.Schema
        returns an equivalent iceberg Schema based on the arrow schema read from the file
    """
    return Schema([get_field(col) for col in arrow_schema])
