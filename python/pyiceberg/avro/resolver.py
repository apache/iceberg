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
from functools import singledispatch
from typing import (
    List,
    Optional,
    Tuple,
    Union,
)

from pyiceberg.avro.reader import (
    ConstructReader,
    ListReader,
    MapReader,
    NoneReader,
    OptionReader,
    Reader,
    StructReader,
    primitive_reader,
)
from pyiceberg.exceptions import ValidationError
from pyiceberg.schema import Schema, promote, visit
from pyiceberg.types import (
    IcebergType,
    ListType,
    MapType,
    PrimitiveType,
    StructType,
)


@singledispatch
def resolve(file_schema: Union[Schema, IcebergType], read_schema: Union[Schema, IcebergType]) -> Reader:
    """This resolves the file and read schema

    The function traverses the schema in post-order fashion

     Args:
         file_schema (Schema | IcebergType): The schema of the file
         read_schema (Schema | IcebergType): The requested read schema which is equal, subset or superset of the file schema

     Raises:
         NotImplementedError: If attempting to resolve an unrecognized object type
    """
    raise NotImplementedError(f"Cannot resolve non-type: {file_schema}")


@resolve.register(Schema)
def _(file_schema: Schema, read_schema: Schema) -> Reader:
    """Visit a Schema and starts resolving it by converting it to a struct"""
    return resolve(file_schema.as_struct(), read_schema.as_struct())


@resolve.register(StructType)
def _(file_struct: StructType, read_struct: IcebergType) -> Reader:
    """Iterates over the file schema, and checks if the field is in the read schema"""

    if not isinstance(read_struct, StructType):
        raise ValidationError(f"File/read schema are not aligned for {file_struct}, got {read_struct}")

    results: List[Tuple[Optional[int], Reader]] = []
    read_fields = {field.field_id: (pos, field) for pos, field in enumerate(read_struct.fields)}

    for file_field in file_struct.fields:
        if file_field.field_id in read_fields:
            read_pos, read_field = read_fields[file_field.field_id]
            result_reader = resolve(file_field.field_type, read_field.field_type)
        else:
            read_pos = None
            result_reader = visit(file_field.field_type, ConstructReader())
        result_reader = result_reader if file_field.required else OptionReader(result_reader)
        results.append((read_pos, result_reader))

    file_fields = {field.field_id: field for field in file_struct.fields}
    for pos, read_field in enumerate(read_struct.fields):
        if read_field.field_id not in file_fields:
            if read_field.required:
                raise ValidationError(f"{read_field} is non-optional, and not part of the file schema")
            # Just set the new field to None
            results.append((pos, NoneReader()))

    return StructReader(tuple(results))


@resolve.register(ListType)
def _(file_list: ListType, read_list: IcebergType) -> Reader:
    if not isinstance(read_list, ListType):
        raise ValidationError(f"File/read schema are not aligned for {file_list}, got {read_list}")
    element_reader = resolve(file_list.element_type, read_list.element_type)
    return ListReader(element_reader)


@resolve.register(MapType)
def _(file_map: MapType, read_map: IcebergType) -> Reader:
    if not isinstance(read_map, MapType):
        raise ValidationError(f"File/read schema are not aligned for {file_map}, got {read_map}")
    key_reader = resolve(file_map.key_type, read_map.key_type)
    value_reader = resolve(file_map.value_type, read_map.value_type)

    return MapReader(key_reader, value_reader)


@resolve.register(PrimitiveType)
def _(file_type: PrimitiveType, read_type: IcebergType) -> Reader:
    """Converting the primitive type into an actual reader that will decode the physical data"""
    if not isinstance(read_type, PrimitiveType):
        raise ValidationError(f"Cannot promote {file_type} to {read_type}")

    if file_type != read_type:
        # In the case of a promotion, we want to check if it is valid
        promote(file_type, read_type)
    return primitive_reader(file_type)
