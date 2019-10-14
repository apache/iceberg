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

import fastparquet
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
                               TimestampType,
                               TimeType)


parquet_type_name_map = {0: lambda x=None: BooleanType.get(),
                         1: lambda x=None: IntegerType.get(),
                         2: lambda x=None: LongType.get(),
                         3: lambda x=None: LongType.get(),
                         4: lambda x=None: FloatType.get(),
                         5: lambda x=None: DoubleType.get(),
                         6: lambda x: FixedType.of_length(x),
                         7: lambda x=None: BinaryType.get()}
parquet_ctype_name_map = {0: lambda x=None, y=None: StringType.get(),
                          4: lambda x=None, y=None: StringType.get(),
                          5: lambda x, y: DecimalType.of(x, y),
                          6: lambda x=None, y=None: DateType.get(),
                          7: lambda x=None, y=None: TimeType.get(),
                          8: lambda x=None, y=None: TimeType.get(),
                          9: lambda x=None, y=None: TimestampType.with_timezone(),
                          10: lambda x=None, y=None: TimestampType.with_timezone(),
                          11: lambda x=None, y=None: IntegerType.get(),
                          12: lambda x=None, y=None: IntegerType.get(),
                          15: lambda x=None, y=None: IntegerType.get(),
                          16: lambda x=None, y=None: IntegerType.get(),
                          17: lambda x=None, y=None: IntegerType.get(),
                          18: lambda x=None, y=None: LongType.get(),
                          19: lambda x=None, y=None: StringType.get(),
                          20: lambda x=None, y=None: StringType.get()}


def convert_parquet_to_iceberg(schema):
    """
    Entry point to convert a fastparquet schema into an iceberg schema

    Parameters
    ----------
    schema : fastparquet.schema.SchemaHelper
        An fastparquet schema of the file being read

    Returns
    -------
    iceberg.api.Schema
        The equivalent iceberg schema for the given fastparquet schema
    """
    return convert_field(schema.root)


def convert_field(root):  # noqa: ignore=C901
    """
    Given two Iceberg schema's returns a list of column_names for all id's in the
    file schema that are projected in the expected schema

    Parameters
    ----------
    root : fastparquet.parquet_thrift.parquet.ttypes.SchemaElement
        The current element to convert

    Returns
    -------
    Union[NestedField, StructType]
        Returns the NestedField tht matches the given schema element or the collection of
        NestFields wrapped in a StructType at the terminal processing iteration
    """

    # TODO: Convert conversion to use pyarrow schema

    try:
        iceberg_type = parquet_ctype_name_map[root.converted_type](root.precision, root.scale)
    except KeyError:
        try:
            iceberg_type = parquet_type_name_map[root.type]()
        except KeyError:
            if root.converted_type == fastparquet.parquet_thrift.ConvertedType.MAP:
                repeated_key_value = root.children["map"]

                if len(repeated_key_value.children) == 2:
                    key_result = convert_field(repeated_key_value.children["key"])
                    value_result = convert_field(repeated_key_value.children["value"])

                if value_result.is_optional:
                    map_type = MapType.of_optional(key_result.field_id, value_result.field_id,
                                                   key_result.type, value_result.type)
                else:
                    map_type = MapType.of_required(key_result.field_id, value_result.field_id,
                                                   key_result.type, value_result.type)

                return get_nested_field(root.field_id, root.name, map_type, root.repetition_type == 0)

            elif root.converted_type == fastparquet.parquet_thrift.ConvertedType.LIST:
                repeated = root.children["list"]
                element = repeated.children["element"]
                element_type = convert_field(element)
                if element.repetition_type == 0:
                    list_type = ListType.of_required(element.field_id, element_type.type)
                else:
                    list_type = ListType.of_required(element.field_id, element_type.type)

                return get_nested_field(root.field_id, root.name, list_type, root.repetition_type == 0)

            else:
                fields = []
                if len(root.children) > 0:
                    for child in root.children.values():
                        fields.append(convert_field(child))
                else:
                    raise RuntimeError("Unable to convert type to iceberg %s" % root)

                if root.name == "table":
                    return Schema(fields)
                else:
                    struct_type = StructType.of(fields)
                    return get_nested_field(root.field_id, root.name, struct_type, root.repetition_type == 0)

    return get_nested_field(root.field_id, root.name, iceberg_type, root.repetition_type == 0)


def get_nested_field(field_id, field_name, field_type, is_required):
    if is_required:
        return NestedField.required(field_id, field_name, field_type)
    else:
        return NestedField.optional(field_id, field_name, field_type)
