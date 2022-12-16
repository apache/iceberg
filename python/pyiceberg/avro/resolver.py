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
)
from pyiceberg.schema import Schema, visit
from pyiceberg.types import (
    BinaryType,
    DecimalType,
    DoubleType,
    FloatType,
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
def resolve(file_schema: Union[Schema, IcebergType], read_schema: Union[Schema, IcebergType]) -> Reader:
    """This resolves the file and read schema

    The function traverses the schema in post-order fashion

     Args:
         file_schema (Schema | IcebergType): The schema of the Avro file
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
        raise ResolveException(f"File/read schema are not aligned for {file_struct}, got {read_struct}")

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
                raise ResolveException(f"{read_field} is non-optional, and not part of the file schema")
            # Just set the new field to None
            results.append((pos, NoneReader()))

    return StructReader(tuple(results))


@resolve.register(ListType)
def _(file_list: ListType, read_list: IcebergType) -> Reader:
    if not isinstance(read_list, ListType):
        raise ResolveException(f"File/read schema are not aligned for {file_list}, got {read_list}")
    element_reader = resolve(file_list.element_type, read_list.element_type)
    return ListReader(element_reader)


@resolve.register(MapType)
def _(file_map: MapType, read_map: IcebergType) -> Reader:
    if not isinstance(read_map, MapType):
        raise ResolveException(f"File/read schema are not aligned for {file_map}, got {read_map}")
    key_reader = resolve(file_map.key_type, read_map.key_type)
    value_reader = resolve(file_map.value_type, read_map.value_type)

    return MapReader(key_reader, value_reader)


@resolve.register(PrimitiveType)
def _(file_type: PrimitiveType, read_type: IcebergType) -> Reader:
    """Converting the primitive type into an actual reader that will decode the physical data"""
    if not isinstance(read_type, PrimitiveType):
        raise ResolveException(f"Cannot promote {file_type} to {read_type}")

    # In the case of a promotion, we want to check if it is valid
    if file_type != read_type:
        return promote(file_type, read_type)
    return visit(read_type, ConstructReader())


@singledispatch
def promote(file_type: IcebergType, read_type: IcebergType) -> Reader:
    """Promotes reading a file type to a read type

    Args:
        file_type (IcebergType): The type of the Avro file
        read_type (IcebergType): The requested read type

    Raises:
        ResolveException: If attempting to resolve an unrecognized object type
    """
    raise ResolveException(f"Cannot promote {file_type} to {read_type}")


@promote.register(IntegerType)
def _(file_type: IntegerType, read_type: IcebergType) -> Reader:
    if isinstance(read_type, LongType):
        # Ints/Longs are binary compatible in Avro, so this is okay
        return visit(read_type, ConstructReader())
    else:
        raise ResolveException(f"Cannot promote an int to {read_type}")


@promote.register(FloatType)
def _(file_type: FloatType, read_type: IcebergType) -> Reader:
    if isinstance(read_type, DoubleType):
        # We should just read the float, and return it, since it both returns a float
        return visit(file_type, ConstructReader())
    else:
        raise ResolveException(f"Cannot promote an float to {read_type}")


@promote.register(StringType)
def _(file_type: StringType, read_type: IcebergType) -> Reader:
    if isinstance(read_type, BinaryType):
        return visit(read_type, ConstructReader())
    else:
        raise ResolveException(f"Cannot promote an string to {read_type}")


@promote.register(BinaryType)
def _(file_type: BinaryType, read_type: IcebergType) -> Reader:
    if isinstance(read_type, StringType):
        return visit(read_type, ConstructReader())
    else:
        raise ResolveException(f"Cannot promote an binary to {read_type}")


@promote.register(DecimalType)
def _(file_type: DecimalType, read_type: IcebergType) -> Reader:
    if isinstance(read_type, DecimalType):
        if file_type.precision <= read_type.precision and file_type.scale == file_type.scale:
            return visit(read_type, ConstructReader())
        else:
            raise ResolveException(f"Cannot reduce precision from {file_type} to {read_type}")
    else:
        raise ResolveException(f"Cannot promote an decimal to {read_type}")
