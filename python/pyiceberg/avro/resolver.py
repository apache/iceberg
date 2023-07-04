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
# pylint: disable=arguments-renamed,unused-argument
from enum import Enum
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from pyiceberg.avro.decoder import BinaryDecoder
from pyiceberg.avro.reader import (
    BinaryReader,
    BooleanReader,
    DateReader,
    DecimalReader,
    DefaultReader,
    DoubleReader,
    FixedReader,
    FloatReader,
    IntegerReader,
    ListReader,
    MapReader,
    NoneReader,
    OptionReader,
    Reader,
    StringReader,
    StructReader,
    TimeReader,
    TimestampReader,
    TimestamptzReader,
    UUIDReader,
)
from pyiceberg.exceptions import ResolveError
from pyiceberg.schema import (
    PartnerAccessor,
    PrimitiveWithPartnerVisitor,
    Schema,
    promote,
    visit_with_partner,
)
from pyiceberg.typedef import EMPTY_DICT, Record, StructProtocol
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

STRUCT_ROOT = -1


def construct_reader(
    file_schema: Union[Schema, IcebergType], read_types: Dict[int, Callable[..., StructProtocol]] = EMPTY_DICT
) -> Reader:
    """Constructs a reader from a file schema.

    Args:
        file_schema (Schema | IcebergType): The schema of the Avro file.

    Raises:
        NotImplementedError: If attempting to resolve an unrecognized object type.
    """
    return resolve(file_schema, file_schema, read_types)


def resolve(
    file_schema: Union[Schema, IcebergType],
    read_schema: Union[Schema, IcebergType],
    read_types: Dict[int, Callable[..., StructProtocol]] = EMPTY_DICT,
    read_enums: Dict[int, Callable[..., Enum]] = EMPTY_DICT,
) -> Reader:
    """Resolves the file and read schema to produce a reader.

    Args:
        file_schema (Schema | IcebergType): The schema of the Avro file
        read_schema (Schema | IcebergType): The requested read schema which is equal, subset or superset of the file schema.
        read_types (Dict[int, Callable[..., StructProtocol]]): A dict of types to use for struct data.
        read_enums (Dict[int, Callable[..., Enum]]): A dict of fields that have to be converted to an enum.

    Raises:
        NotImplementedError: If attempting to resolve an unrecognized object type.
    """
    return visit_with_partner(
        file_schema, read_schema, SchemaResolver(read_types, read_enums), SchemaPartnerAccessor()
    )  # type: ignore


class EnumReader(Reader):
    """An Enum reader to wrap primitive values into an Enum."""

    enum: Callable[..., Enum]
    reader: Reader

    def __init__(self, enum: Callable[..., Enum], reader: Reader) -> None:
        self.enum = enum
        self.reader = reader

    def read(self, decoder: BinaryDecoder) -> Enum:
        return self.enum(self.reader.read(decoder))

    def skip(self, decoder: BinaryDecoder) -> None:
        pass


class SchemaResolver(PrimitiveWithPartnerVisitor[IcebergType, Reader]):
    read_types: Dict[int, Callable[..., StructProtocol]]
    read_enums: Dict[int, Callable[..., Enum]]
    context: List[int]

    def __init__(
        self,
        read_types: Dict[int, Callable[..., StructProtocol]] = EMPTY_DICT,
        read_enums: Dict[int, Callable[..., Enum]] = EMPTY_DICT,
    ) -> None:
        self.read_types = read_types
        self.read_enums = read_enums
        self.context = []

    def schema(self, schema: Schema, expected_schema: Optional[IcebergType], result: Reader) -> Reader:
        return result

    def before_field(self, field: NestedField, field_partner: Optional[NestedField]) -> None:
        self.context.append(field.field_id)

    def after_field(self, field: NestedField, field_partner: Optional[NestedField]) -> None:
        self.context.pop()

    def struct(self, struct: StructType, expected_struct: Optional[IcebergType], field_readers: List[Reader]) -> Reader:
        read_struct_id = self.context[STRUCT_ROOT] if len(self.context) > 0 else STRUCT_ROOT
        struct_callable = self.read_types.get(read_struct_id, Record)

        if not expected_struct:
            return StructReader(tuple(enumerate(field_readers)), struct_callable, struct)

        if not isinstance(expected_struct, StructType):
            raise ResolveError(f"File/read schema are not aligned for struct, got {expected_struct}")

        expected_positions: Dict[int, int] = {field.field_id: pos for pos, field in enumerate(expected_struct.fields)}

        # first, add readers for the file fields that must be in order
        results: List[Tuple[Optional[int], Reader]] = [
            (
                expected_positions.get(field.field_id),
                # Check if we need to convert it to an Enum
                result_reader if not (enum_type := self.read_enums.get(field.field_id)) else EnumReader(enum_type, result_reader),
            )
            for field, result_reader in zip(struct.fields, field_readers)
        ]

        file_fields = {field.field_id: field for field in struct.fields}
        for pos, read_field in enumerate(expected_struct.fields):
            if read_field.field_id not in file_fields:
                if isinstance(read_field, NestedField) and read_field.initial_default is not None:
                    # The field is not in the file, but there is a default value
                    # and that one can be required
                    results.append((pos, DefaultReader(read_field.initial_default)))
                elif read_field.required:
                    raise ResolveError(f"{read_field} is non-optional, and not part of the file schema")
                else:
                    # Just set the new field to None
                    results.append((pos, NoneReader()))

        return StructReader(tuple(results), struct_callable, expected_struct)

    def field(self, field: NestedField, expected_field: Optional[IcebergType], field_reader: Reader) -> Reader:
        return field_reader if field.required else OptionReader(field_reader)

    def list(self, list_type: ListType, expected_list: Optional[IcebergType], element_reader: Reader) -> Reader:
        if expected_list and not isinstance(expected_list, ListType):
            raise ResolveError(f"File/read schema are not aligned for list, got {expected_list}")

        return ListReader(element_reader if list_type.element_required else OptionReader(element_reader))

    def map(self, map_type: MapType, expected_map: Optional[IcebergType], key_reader: Reader, value_reader: Reader) -> Reader:
        if expected_map and not isinstance(expected_map, MapType):
            raise ResolveError(f"File/read schema are not aligned for map, got {expected_map}")

        return MapReader(key_reader, value_reader if map_type.value_required else OptionReader(value_reader))

    def primitive(self, primitive: PrimitiveType, expected_primitive: Optional[IcebergType]) -> Reader:
        if expected_primitive is not None:
            if not isinstance(expected_primitive, PrimitiveType):
                raise ResolveError(f"File/read schema are not aligned for {primitive}, got {expected_primitive}")

            # ensure that the type can be projected to the expected
            if primitive != expected_primitive:
                promote(primitive, expected_primitive)

        return super().primitive(primitive, expected_primitive)

    def visit_boolean(self, boolean_type: BooleanType, partner: Optional[IcebergType]) -> Reader:
        return BooleanReader()

    def visit_integer(self, integer_type: IntegerType, partner: Optional[IcebergType]) -> Reader:
        return IntegerReader()

    def visit_long(self, long_type: LongType, partner: Optional[IcebergType]) -> Reader:
        return IntegerReader()

    def visit_float(self, float_type: FloatType, partner: Optional[IcebergType]) -> Reader:
        return FloatReader()

    def visit_double(self, double_type: DoubleType, partner: Optional[IcebergType]) -> Reader:
        return DoubleReader()

    def visit_decimal(self, decimal_type: DecimalType, partner: Optional[IcebergType]) -> Reader:
        return DecimalReader(decimal_type.precision, decimal_type.scale)

    def visit_date(self, date_type: DateType, partner: Optional[IcebergType]) -> Reader:
        return DateReader()

    def visit_time(self, time_type: TimeType, partner: Optional[IcebergType]) -> Reader:
        return TimeReader()

    def visit_timestamp(self, timestamp_type: TimestampType, partner: Optional[IcebergType]) -> Reader:
        return TimestampReader()

    def visit_timestampz(self, timestamptz_type: TimestamptzType, partner: Optional[IcebergType]) -> Reader:
        return TimestamptzReader()

    def visit_string(self, string_type: StringType, partner: Optional[IcebergType]) -> Reader:
        return StringReader()

    def visit_uuid(self, uuid_type: UUIDType, partner: Optional[IcebergType]) -> Reader:
        return UUIDReader()

    def visit_fixed(self, fixed_type: FixedType, partner: Optional[IcebergType]) -> Reader:
        return FixedReader(len(fixed_type))

    def visit_binary(self, binary_type: BinaryType, partner: Optional[IcebergType]) -> Reader:
        return BinaryReader()


class SchemaPartnerAccessor(PartnerAccessor[IcebergType]):
    def schema_partner(self, partner: Optional[IcebergType]) -> Optional[IcebergType]:
        if isinstance(partner, Schema):
            return partner.as_struct()

        raise ResolveError(f"File/read schema are not aligned for schema, got {partner}")

    def field_partner(self, partner: Optional[IcebergType], field_id: int, field_name: str) -> Optional[IcebergType]:
        if isinstance(partner, StructType):
            field = partner.field(field_id)
        else:
            raise ResolveError(f"File/read schema are not aligned for struct, got {partner}")

        return field.field_type if field else None

    def list_element_partner(self, partner_list: Optional[IcebergType]) -> Optional[IcebergType]:
        if isinstance(partner_list, ListType):
            return partner_list.element_type

        raise ResolveError(f"File/read schema are not aligned for list, got {partner_list}")

    def map_key_partner(self, partner_map: Optional[IcebergType]) -> Optional[IcebergType]:
        if isinstance(partner_map, MapType):
            return partner_map.key_type

        raise ResolveError(f"File/read schema are not aligned for map, got {partner_map}")

    def map_value_partner(self, partner_map: Optional[IcebergType]) -> Optional[IcebergType]:
        if isinstance(partner_map, MapType):
            return partner_map.value_type

        raise ResolveError(f"File/read schema are not aligned for map, got {partner_map}")
