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
from typing import (
    Dict,
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
    Reader,
    StructReader,
)
from pyiceberg.exceptions import ResolveError
from pyiceberg.schema import (
    PartnerAccessor,
    Schema,
    SchemaWithPartnerVisitor,
    promote,
    visit,
    visit_with_partner,
)
from pyiceberg.types import (
    IcebergType,
    ListType,
    MapType,
    NestedField,
    PrimitiveType,
    StructType,
)


def resolve(file_schema: Union[Schema, IcebergType], read_schema: Union[Schema, IcebergType]) -> Reader:
    """This resolves the file and read schema

    The function traverses the schema in post-order fashion

     Args:
         file_schema (Schema | IcebergType): The schema of the Avro file
         read_schema (Schema | IcebergType): The requested read schema which is equal, subset or superset of the file schema

     Raises:
         NotImplementedError: If attempting to resolve an unrecognized object type
    """
    return visit_with_partner(file_schema, read_schema, SchemaResolver(), SchemaPartnerAccessor())


class SchemaResolver(SchemaWithPartnerVisitor[IcebergType, Reader]):
    def schema(self, schema: Schema, expected_schema: Optional[IcebergType], result: Reader) -> Reader:
        return result

    def struct(self, struct: StructType, expected_struct: Optional[IcebergType], field_readers: List[Reader]) -> Reader:
        if expected_struct and not isinstance(expected_struct, StructType):
            raise ResolveError(f"File/read schema are not aligned for struct, got {expected_struct}")

        results: List[Tuple[Optional[int], Reader]] = []
        expected_positions: Dict[int, int] = {field.field_id: pos for pos, field in enumerate(expected_struct.fields)}

        # first, add readers for the file fields that must be in order
        for field, result_reader in zip(struct.fields, field_readers):
            read_pos = expected_positions.get(field.field_id)
            results.append((read_pos, result_reader))

        file_fields = {field.field_id: field for field in struct.fields}
        for pos, read_field in enumerate(expected_struct.fields):
            if read_field.field_id not in file_fields:
                if read_field.required:
                    raise ResolveError(f"{read_field} is non-optional, and not part of the file schema")
                # Just set the new field to None
                results.append((pos, NoneReader()))

        return StructReader(tuple(results))

    def field(self, field: NestedField, expected_field: Optional[IcebergType], field_reader: Reader) -> Reader:
        return field_reader

    def list(self, list_type: ListType, expected_list: Optional[IcebergType], element_reader: Reader) -> Reader:
        if expected_list and not isinstance(expected_list, ListType):
            raise ResolveError(f"File/read schema are not aligned for list, got {expected_list}")

        return ListReader(element_reader)

    def map(self, map_type: MapType, expected_map: Optional[IcebergType], key_reader: Reader, value_reader: Reader) -> Reader:
        if expected_map and not isinstance(expected_map, MapType):
            raise ResolveError(f"File/read schema are not aligned for map, got {expected_map}")

        return MapReader(key_reader, value_reader)

    def primitive(self, primitive: PrimitiveType, expected_primitive: Optional[IcebergType]) -> Reader:
        if expected_primitive is not None:
            if not isinstance(expected_primitive, PrimitiveType):
                raise ResolveError(f"File/read schema are not aligned for {primitive}, got {expected_primitive}")

            # ensure that the type can be projected to the expected
            if primitive != expected_primitive:
                promote(primitive, expected_primitive)

        return visit(primitive, ConstructReader())


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
