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
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
)

from iceberg.avro.reader import (
    ConstructReader,
    ListReader,
    MapReader,
    NoneReader,
    OptionReader,
    Reader,
    StructReader,
    primitive_reader,
)
from iceberg.schema import Schema, visit
from iceberg.types import (
    BinaryType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    PrimitiveType,
    StringType,
    StructType,
)


class ResolveException(Exception):
    pass


@singledispatch
def resolve(write_schema, read_schema) -> Reader:
    """This resolves the write and read schema

    The function traverses the schema in post-order fashion

     Args:
         write_schema (Schema | IcebergType): The write schema of the Avro file
         read_schema (Schema | IcebergType): The requested read schema which is equal or a subset of the write schema

     Raises:
         NotImplementedError: If attempting to resolve an unrecognized object type
    """
    raise NotImplementedError("Cannot resolve non-type: %s" % write_schema)


@resolve.register(Schema)
def _(write_schema: Schema, read_schema: Schema) -> Reader:
    """Visit a Schema and starts resolving it by converting it to a struct"""
    return resolve(write_schema.as_struct(), read_schema.as_struct())


@resolve.register(StructType)
def _(write_struct: StructType, read_struct: StructType) -> Reader:
    """Iterates over the write schema, and checks if the field is in the read schema"""
    results: List[Tuple[Optional[int], Reader]] = []

    read_fields = {field.name: (pos, field) for pos, field in enumerate(read_struct.fields)}

    for write_field in write_struct.fields:
        if write_field.name in read_fields:
            read_pos, read_field = read_fields[write_field.name]
            result_reader = resolve(write_field.field_type, read_field.field_type)
        else:
            read_pos = None
            result_reader = visit(write_field.field_type, ConstructReader())
        result_reader = result_reader if write_field.required else OptionReader(result_reader)
        results.append((read_pos, result_reader))

    write_fields = {field.name: field for field in write_struct.fields}
    for pos, read_field in enumerate(read_struct.fields):
        if read_field.name not in write_fields:
            if read_field.required:
                raise ResolveException(f"{read_field} is in not in the write schema, and is required")
            # Just set the new field to None
            results.append((pos, NoneReader()))

    return StructReader(tuple(results))


@resolve.register(ListType)
def _(write_list: ListType, read_list: ListType) -> Reader:
    if not isinstance(read_list, ListType):
        raise ResolveException(f"Cannot change {write_list} into {read_list}")
    element_reader = resolve(write_list.element.field_type, read_list.element.field_type)
    return ListReader(element_reader)


@resolve.register(MapType)
def _(write_map: MapType, read_map: MapType) -> Reader:
    if not isinstance(read_map, MapType):
        raise ResolveException(f"Cannot change {write_map} into {read_map}")
    key_reader = resolve(write_map.key.field_type, read_map.key.field_type)
    value_reader = resolve(write_map.value.field_type, read_map.value.field_type)

    return MapReader(key_reader, value_reader)


ALLOWED_PROMOTIONS: Dict[Type[PrimitiveType], Set[Type[PrimitiveType]]] = {
    # For now we only support the binary compatible ones
    IntegerType: {LongType},
    StringType: {BinaryType},
    BinaryType: {StringType},
    # These are all allowed according to the Avro spec
    # IntegerType: {LongType, FloatType, DoubleType},
    # LongType: {FloatType, DoubleType},
    # FloatType: {DoubleType},
    # StringType: {BinaryType},
    # BinaryType: {StringType},
}


@resolve.register(PrimitiveType)
def _(write_type: PrimitiveType, read_type: IcebergType) -> Reader:
    """Converting the primitive type into an actual reader that will decode the physical data"""
    if not isinstance(read_type, PrimitiveType):
        raise ResolveException(f"Cannot promote {write_type} to {read_type}")

    if write_type != read_type:
        if allowed_promotions := ALLOWED_PROMOTIONS.get(type(write_type)):
            if read_type not in allowed_promotions:
                raise ResolveException(f"Promotion from {read_type} to {write_type} is not allowed")
        else:
            raise ResolveException(f"Promotion from {read_type} to {write_type} is not allowed")

    return primitive_reader(read_type)
