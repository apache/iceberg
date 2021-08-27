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

import math
from typing import List

from .type import (Type,
                   TypeID)
from .types import (ListType,
                    MapType,
                    NestedField,
                    StructType)
from ...exceptions import ValidationException

MAX_PRECISION = list()
REQUIRED_LENGTH = [-1 for item in range(40)]

MAX_PRECISION.append(0)
for i in range(1, 24):
    MAX_PRECISION.append(int(math.floor(math.log10(math.pow(2, 8 * i - 1) - 1))))

for i in range(len(REQUIRED_LENGTH)):
    for j in range(len(MAX_PRECISION)):
        if i <= MAX_PRECISION[j]:
            REQUIRED_LENGTH[i] = j
            break
    if REQUIRED_LENGTH[i] < 0:
        raise RuntimeError("Could not find required length for precision %s" % i)


def select(schema, field_ids):
    import iceberg.api.schema
    if schema is None:
        raise RuntimeError("Schema cannot be None")
    if field_ids is None:
        raise RuntimeError("Field ids cannot be None")

    result = visit(schema, PruneColumns(field_ids))
    if schema.as_struct() == result:
        return schema
    elif result is not None:
        if schema.get_aliases() is not None:
            return iceberg.api.schema.Schema(result.as_nested_type().fields, schema.get_aliases())
        else:
            return iceberg.api.schema.Schema(result.as_nested_type().fields)

    return iceberg.api.schema.Schema(list(), schema.get_aliases())


def get_projected_ids(schema):
    import iceberg.api.schema
    if isinstance(schema, iceberg.api.schema.Schema):
        return visit(schema, GetProjectedIds())
    elif isinstance(schema, Type):
        if schema.is_primitive_type():
            return set()

        return set(visit(schema, GetProjectedIds))

    else:
        raise RuntimeError("Argument %s must be Schema or a Type" % schema)


def select_not(schema, field_ids):
    projected_ids = get_projected_ids(schema)
    projected_ids.difference(field_ids)

    return select(schema, projected_ids)


def join(left, right):
    import iceberg.api.schema
    return iceberg.api.schema.Schema(left + right)


def index_by_name(struct):
    return visit(struct, IndexByName())


def index_by_id(struct):
    return visit(struct, IndexById())


def assign_fresh_ids(type_var, next_id):
    from ..schema import Schema
    if isinstance(type_var, Type):
        return visit(type_var, AssignFreshIds(next_id))
    elif isinstance(type_var, Schema):
        schema = type_var
        return Schema(list(visit(schema.as_struct(), AssignFreshIds(next_id))
                           .as_nested_type().fields))


def visit(arg, visitor): # noqa: ignore=C901
    from ..schema import Schema
    if isinstance(visitor, CustomOrderSchemaVisitor):
        return visit_custom_order(arg, visitor)
    elif isinstance(arg, Schema):
        return visitor.schema(arg, visit(arg.as_struct(), visitor))
    elif isinstance(arg, Type):
        type_var = arg
        if type_var.type_id == TypeID.STRUCT:
            struct = type_var.as_nested_type().as_struct_type()
            results = list()
            for field in struct.fields:
                visitor.field_ids.append(field.field_id)
                visitor.field_names.append(field.name)
                result = None
                try:
                    result = visit(field.type, visitor)
                except NotImplementedError:
                    # will remove it after missing functions are implemented.
                    pass
                finally:
                    visitor.field_ids.pop()
                    visitor.field_names.pop()
                results.append(visitor.field(field, result))
            return visitor.struct(struct, results)
        elif type_var.type_id == TypeID.LIST:
            list_var = type_var.as_nested_type().as_list_type()
            visitor.field_ids.append(list_var.element_id)
            try:
                element_result = visit(list_var.element_type, visitor)
            except NotImplementedError:
                # will remove it after missing functions are implemented.
                pass
            finally:
                visitor.field_ids.pop()

            return visitor.list(list_var, element_result)
        elif type_var.type_id == TypeID.MAP:
            raise NotImplementedError()
        else:
            return visitor.primitive(arg.as_primitive_type())
    else:
        raise RuntimeError("Invalid type for arg: %s" % arg)


def visit_custom_order(arg, visitor):
    from ..schema import Schema
    if isinstance(arg, Schema):
        schema = arg
        return visitor.schema(arg, VisitFuture(schema.as_struct(), visitor))
    elif isinstance(arg, Type):
        type_var = arg
        if type_var.type_id == TypeID.STRUCT:
            struct = type_var.as_nested_type().as_struct_type()
            results = list()
            fields = struct.fields
            for field in fields:
                results.append(VisitFieldFuture(field, visitor))
            struct = visitor.struct(struct, [x.get() for x in results])
            return struct
        elif type_var.type_id == TypeID.LIST:
            list_var = type_var.as_nested_type().as_list_type()
            return visitor.list(list_var, VisitFuture(list_var.element_type, visitor))
        elif type_var.type_id == TypeID.MAP:
            raise NotImplementedError()

        return visitor.primitive(type_var.as_primitive_type())


class SchemaVisitor(object):

    def __init__(self):
        self.field_names = list()
        self.field_ids = list()

    def schema(self, schema, struct_result):
        return None

    def struct(self, struct, field_results):
        return None

    def field(self, field, field_result):
        return None

    def list(self, list_var, element_result):
        return None

    def map(self, map_var, key_result, value_result):
        return None

    def primitive(self, primitive_var):
        return None


class CustomOrderSchemaVisitor(object):
    def __init__(self):
        super(CustomOrderSchemaVisitor, self).__init__()

    def schema(self, schema, struct_result):
        return None

    def struct(self, struct, field_results):
        return None

    def field(self, field, field_result):
        return None

    def list(self, list_var, element_result):
        return None

    def map(self, map_var, key_result, value_result):
        return None

    def primitive(self, primitive_var):
        return None


class VisitFuture(object):

    def __init__(self, type, visitor):
        self.type = type
        self.visitor = visitor

    def get(self):
        return visit(self.type, self.visitor)


class VisitFieldFuture(object):

    def __init__(self, field, visitor):
        self.field = field
        self.visitor = visitor

    def get(self):
        return self.visitor.field(self.field, VisitFuture(self.field.type, self.visitor).get)


def decimal_required_bytes(precision):
    if precision < 0 or precision > 40:
        raise RuntimeError("Unsupported decimal precision: %s" % precision)

    return REQUIRED_LENGTH[precision]


class GetProjectedIds(SchemaVisitor):

    def __init__(self):
        super(GetProjectedIds, self).__init__()
        self.field_ids = list()

    def schema(self, schema, struct_result):
        return self.field_ids

    def struct(self, struct, field_results):
        return self.field_ids

    def field(self, field, field_result):
        if field_result is None:
            self.field_ids.append(field.field_id)

        return self.field_ids

    def list(self, list_var, element_result):
        if element_result is None:
            for field in list_var.fields():
                self.field_ids.append(field.field_id)

        return self.field_ids

    def map(self, map_var, key_result, value_result):
        if value_result is None:
            for field in map_var.fields():
                self.field_ids.append(field.field_id)

        return self.field_ids


class PruneColumns(SchemaVisitor):

    def __init__(self, selected):
        super(PruneColumns, self).__init__()
        self.selected = list(selected)

    def schema(self, schema, struct_result):
        return struct_result

    def struct(self, struct, field_results):
        fields = struct.fields
        selected_fields = list()
        same_types = True

        for i, projected_type in enumerate(field_results):
            field = fields[i]
            if projected_type is not None:
                if field.type == projected_type:
                    selected_fields.append(field)
                elif projected_type is not None:
                    same_types = False
                    if field.is_optional:
                        selected_fields.append(NestedField.optional(field.field_id,
                                                                    field.name,
                                                                    projected_type))
                    else:
                        selected_fields.append(NestedField.required(field.field_id,
                                                                    field.name,
                                                                    projected_type))

        if len(selected_fields) != 0:
            if len(selected_fields) == len(fields) and same_types:
                return struct
            else:
                return StructType.of(selected_fields)

    def field(self, field, field_result):
        if field.field_id in self.selected:
            return field.type
        elif field_result is not None:
            return field_result

    def primitive(self, primitive_var):
        return None


class IndexByName(SchemaVisitor):

    DOT = "."

    def __init__(self):
        super(IndexByName, self).__init__()
        self.name_to_id = dict()

    def schema(self, schema, struct_result):
        return self.name_to_id

    def struct(self, struct, field_results):
        return self.name_to_id

    def field(self, field, field_result):
        self.add_field(field.name, field.field_id)

    def list(self, list_var, element_result):
        for field in list_var.fields():
            self.add_field(field.name, field.field_id)

    def map(self, map_var, key_result, value_result):
        for field in map_var.fields():
            self.add_field(field.name, field.field_id)

    def add_field(self, name, field_id):
        full_name = name
        if self.field_names is not None and len(self.field_names) > 0:
            full_name = IndexByName.DOT.join([IndexByName.DOT.join(self.field_names), name])

        existing_field_id = self.name_to_id.get(full_name)
        ValidationException.check(existing_field_id is None, "Invalid schema: multiple fields for name %s: %s and %s",
                                  (full_name, existing_field_id, field_id))

        self.name_to_id[full_name] = field_id


class IndexById(SchemaVisitor):

    def __init__(self):
        super(IndexById, self).__init__()
        self.index = dict()

    def schema(self, schema, struct_result):
        return self.index

    def struct(self, struct, field_results):
        return self.index

    def field(self, field, field_result):
        self.index[field.field_id] = field

    def list(self, list_var, element_result):
        for field in list_var.fields():
            self.index[field.field_id] = field

    def map(self, map_var, key_result, value_result):
        for field in map_var.fields:
            self.index[field.field_id] = field


class NextID(object):
    def __init__(self):
        raise NotImplementedError()

    def get(self):
        raise NotImplementedError()


class AssignFreshIds(CustomOrderSchemaVisitor):
    def __init__(self, next_id):
        super(AssignFreshIds, self).__init__()
        self.next_id = next_id

    def schema(self, schema, struct_result):
        return self.next_id()

    def struct(self, struct, field_results):
        fields = struct.fields
        length = len(struct.fields)
        new_ids = list()

        for _ in range(length):
            new_ids.append(self.next_id())

        new_fields = list()
        types = iter(field_results)
        for i in range(length):
            field = fields[i]
            type = next(types)
            if field.is_optional:
                new_fields.append(NestedField.optional(new_ids[i], field.name, type))
            else:
                new_fields.append(NestedField.required(new_ids[i], field.name, type))

        return StructType.of(new_fields)

    def field(self, field, field_result):
        return field_result()

    def list(self, list_var, element_result):
        new_id = self.next_id()
        if list_var.is_element_optional():
            return ListType.of_optional(new_id, element_result.get())
        else:
            return ListType.of_required(new_id, element_result.get())

    def map(self, map_var, key_result, value_result):
        new_key_id = self.next_id()
        new_value_id = self.next_id()

        if map_var.is_value_optional():
            return MapType.of_optional(new_key_id, new_value_id, key_result(), value_result())
        else:
            return MapType.of_required(new_key_id, new_value_id, key_result(), value_result())

    def primitive(self, primitive_var):
        return primitive_var


class CheckCompatibility(CustomOrderSchemaVisitor):

    @staticmethod
    def write_compatibility_errors(read_schema, write_schema):
        visit(read_schema, CheckCompatibility(write_schema, True))

    @staticmethod
    def read_compatibility_errors(read_schema, write_schema):
        visit(write_schema, CheckCompatibility(read_schema, False))

    NO_ERRORS: List[str] = []

    def __init__(self, schema, check_ordering):
        self.schema = schema
        self.check_ordering = check_ordering
        self.current_type = None

    def schema(self, schema, struct_result):
        self.current_type = self.schema.as_struct()
        try:
            struct_result.get()
        finally:
            self.current_type = None

    def struct(self, struct, field_results):
        if struct is None:
            raise RuntimeError("Evaluation must start with a schema.")

        if not self.current_type.is_struct_type():
            return [": %s cannot be read as a struct" % self.current_type]

        errors = []

        for field_errors in field_results:
            errors = errors + field_errors

        if self.check_ordering:
            new_struct = self.current_type.as_struct_type()
            id_to_ord = {}
            for i, val in enumerate(new_struct.fields):
                id_to_ord[val.field_id] = i

            last_ordinal = -1

            for read_field in self.struct.fields:
                id_var = read_field.field_id

                field = struct.field(id=id_var)
                if field is not None:
                    ordinal = id_to_ord[id]
                    if last_ordinal >= ordinal:
                        errors.append("%s is out of order before %s" % (read_field.name,
                                                                        new_struct.fields[last_ordinal].name))
                    last_ordinal = ordinal

        return errors

    def field(self, field, field_result) -> List[str]:
        struct = self.current_type.as_struct_type()
        curr_field = struct.field(field.field_id)
        errors = []

        if curr_field is None:
            if not field.is_optional:
                errors.append("{} is required, but is missing".format(field.name))
            return self.NO_ERRORS

        self.current_type = curr_field.type

        try:
            if not field.is_optional and curr_field.is_optional:
                errors.append(field.name + " should be required, but is optional")

            for error in field_result:
                if error.startswith(":"):
                    errors.append("{}{}".format(field.field_name, error))
                else:
                    errors.append("{}.{}".format(field.field_name, error))

            return errors
        except RuntimeError:
            pass
        finally:
            self.current_type = struct
        return errors

    def list(self, list_var, element_result):
        raise NotImplementedError()

    def map(self, map_var, key_result, value_result):
        raise NotImplementedError()

    def primitive(self, primitive_var):
        raise NotImplementedError()
