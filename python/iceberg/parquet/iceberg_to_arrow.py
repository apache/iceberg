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
from typing import Callable, Dict

from iceberg.api import Schema
from iceberg.api.types import NestedField, TypeID
from iceberg.core import SchemaParser
import pyarrow as pa


ICEBERG_TO_ARROW_MAP: Dict[TypeID, Callable] = {
    TypeID.BOOLEAN: lambda type_var: pa.lib.bool_(),
    TypeID.BINARY: lambda type_var: pa.lib.binary(),
    TypeID.DATE: lambda type_var: pa.lib.date32(),
    TypeID.DECIMAL: lambda type_var: pa.lib.decimal128(type_var.precision, type_var.scale),
    TypeID.DOUBLE: lambda type_var: pa.lib.float64(),
    TypeID.FIXED: lambda type_var: pa.lib.binary(length=type_var.length),
    TypeID.FLOAT: lambda type_var: pa.lib.float32(),
    TypeID.INTEGER: lambda type_var: pa.lib.int32(),
    TypeID.LIST: lambda type_var: convert_field(type_var),
    TypeID.LONG: lambda type_var: pa.lib.int64(),
    TypeID.MAP: lambda type_var: convert_field(type_var),
    TypeID.STRING: lambda type_var: pa.lib.string(),
    TypeID.STRUCT: lambda type_var: convert_field(type_var),
    TypeID.TIME: lambda type_var: pa.lib.time64("us"),
    TypeID.TIMESTAMP: lambda type_var: pa.timestamp("us", tz="UTC" if type_var.adjust_to_utc else None),
}


def iceberg_to_arrow(iceberg_schema: Schema) -> pa.Schema:
    """
    Use an iceberg schema, to create an equivalent arrow schema with intact field_id metadata

    Parameters
    ----------
    iceberg_schema : Schema
        An iceberg schema to map

    Returns
    -------
    pyarrow.Schema
        returns an equivalent pyarrow Schema based on the iceberg schema provided. Performs
    """
    arrow_fields = []
    for field in iceberg_schema.as_struct().fields:
        arrow_fields.append(convert_field(field))

    return pa.schema(arrow_fields,
                     metadata={b'iceberg.schema': json.dumps(SchemaParser.to_dict(iceberg_schema)).encode("utf-8")})


def convert_field(iceberg_field: NestedField) -> pa.Field:
    """
        Map an iceberg field to a pyarrow field. Recursively calls itself for nested fields
        there is currently a limitation in the ability to convert map types since the pyarrow python bindings
        don't expose a way to set fields for the key and value fields in a map

        Parameters
        ----------
        iceberg_field: NestedField
            An iceberg schema to map

        Returns
        -------
        pa.Field
            returns an equivalent pyarow Field based on the Nested Field.
        """
    if iceberg_field.type.is_primitive_type():
        try:
            arrow_field = ICEBERG_TO_ARROW_MAP[iceberg_field.type.type_id](iceberg_field.type)
        except KeyError:
            raise ValueError(f"Unable to convert {iceberg_field}")

    elif iceberg_field.type.type_id == TypeID.STRUCT:
        arrow_field = pa.struct([convert_field(field) for field in iceberg_field.type.fields])
    elif iceberg_field.type.type_id == TypeID.LIST:
        try:
            arrow_field = pa.list_(ICEBERG_TO_ARROW_MAP[iceberg_field.type.type_id](iceberg_field.type.element_field))
        except KeyError:
            raise ValueError(f"Unable to convert {iceberg_field.type}")
    elif iceberg_field.type.type_id == TypeID.MAP:
        raise NotImplementedError("Unable to serialize Map types; python arrow bindings can't pass key/val metadata")

    else:
        raise ValueError(f"Unable to convert {iceberg_field}")

    return pa.field(iceberg_field.name,
                    arrow_field,
                    iceberg_field.is_optional,
                    {b'PARQUET:field_id': str(iceberg_field.id).encode("utf-8")})
