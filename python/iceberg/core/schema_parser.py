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

from iceberg.api import Schema
from iceberg.api.types import (from_primitive_string,
                               ListType,
                               MapType,
                               NestedField,
                               StructType,
                               Type,
                               TypeID)


class SchemaParser(object):

    TYPE = "type"
    STRUCT = "struct"
    LIST = "list"
    MAP = "map"
    FIELDS = "fields"
    ELEMENT = "element"
    KEY = "key"
    VALUE = "value"
    DOC = "doc"
    NAME = "name"
    ID = "id"
    ELEMENT_ID = "element-id"
    KEY_ID = "key-id"
    VALUE_ID = "value-id"
    REQUIRED = "required"
    ELEMENT_REQUIRED = "element-required"
    VALUE_REQUIRED = "value-required"

    @staticmethod
    def to_json(type_var, indent=None):
        return json.dumps(SchemaParser.to_dict(type_var), indent=indent)

    @staticmethod
    def to_dict(type_var):
        if isinstance(type_var, Schema):
            struct = type_var.as_struct()
        elif isinstance(type_var, Type):
            struct = type_var

        return SchemaParser._type_to_dict(struct)

    @staticmethod
    def _type_to_dict(type_var):
        if type_var.is_primitive_type():
            return SchemaParser._primitive_to_dict(type_var)
        else:
            nested = type_var.as_nested_type()
            if type_var.type_id == TypeID.STRUCT:
                return SchemaParser._struct_to_dict(nested)
            elif type_var.type_id == TypeID.LIST:
                return SchemaParser._list_to_dict(nested)
            elif type_var.type_id == TypeID.MAP:
                return SchemaParser._map_to_dict(nested)
            else:
                raise RuntimeError("Cannot write unknown type: %s" % type)

    @staticmethod
    def _primitive_to_dict(type_var):
        return str(type_var)

    @staticmethod
    def _struct_to_dict(struct):
        return {SchemaParser.TYPE: SchemaParser.STRUCT,
                SchemaParser.FIELDS: [SchemaParser._struct_field_to_dict(field) for field in struct.fields]}

    @staticmethod
    def _struct_field_to_dict(field):
        schema = {SchemaParser.ID: field.field_id,
                  SchemaParser.NAME: field.name,
                  SchemaParser.REQUIRED: field.is_required,
                  SchemaParser.TYPE: SchemaParser._type_to_dict(field.type)}
        if field.doc is not None:
            schema[SchemaParser.DOC] = field.doc

        return schema

    @staticmethod
    def _list_to_dict(list_type):
        return {SchemaParser.TYPE: SchemaParser.LIST,
                SchemaParser.ELEMENT_ID: list_type.element_id,
                SchemaParser.ELEMENT: SchemaParser._type_to_dict(list_type.element_type),
                SchemaParser.ELEMENT_REQUIRED: list_type.is_element_required()}

    @staticmethod
    def _map_to_dict(map_type):
        return {SchemaParser.TYPE: SchemaParser.MAP,
                SchemaParser.KEY_ID: map_type.key_id(),
                SchemaParser.KEY: SchemaParser._type_to_dict(map_type.key_type()),
                SchemaParser.VALUE_ID: map_type.value_id(),
                SchemaParser.VALUE: SchemaParser._type_to_dict(map_type.value_type()),
                SchemaParser.VALUE_REQUIRED: map_type.is_value_required()}

    @staticmethod
    def type_from_dict(dict_obj):
        if isinstance(dict_obj, (str, bool, int, float)):
            return from_primitive_string(dict_obj)
        else:
            type_str = dict_obj.get(SchemaParser.TYPE)
            if type_str.upper() == "STRUCT":
                return SchemaParser.struct_from_dict(dict_obj)
            elif type_str.upper() == "LIST":
                return SchemaParser.list_from_dict(dict_obj)
            elif type_str.upper() == "MAP":
                return SchemaParser.map_from_dict(dict_obj)
            else:
                raise RuntimeError("Cannot parse type from dict: %s" % dict_obj)

    @staticmethod
    def struct_from_dict(dict_obj):
        struct_fields = list()
        fields = dict_obj.get(SchemaParser.FIELDS)
        for field in fields:
            field_id = field.get(SchemaParser.ID)
            field_name = field.get(SchemaParser.NAME)
            field_type = SchemaParser.type_from_dict(field.get(SchemaParser.TYPE))
            field_doc = field.get(SchemaParser.DOC)

            if field.get(SchemaParser.REQUIRED):
                struct_fields.append(NestedField.required(field_id, field_name, field_type, field_doc))
            else:
                struct_fields.append(NestedField.optional(field_id, field_name, field_type, field_doc))

        return StructType.of(struct_fields)

    @staticmethod
    def list_from_dict(dict_obj):
        elem_id = dict_obj.get(SchemaParser.ELEMENT_ID)
        elem_type = SchemaParser.type_from_dict(dict_obj.get(SchemaParser.ELEMENT))
        is_required = dict_obj.get(SchemaParser.REQUIRED)

        if is_required:
            return ListType.of_required(elem_id, elem_type)
        else:
            return ListType.of_optional(elem_id, elem_type)

    @staticmethod
    def map_from_dict(dict_obj):
        key_id = dict_obj.get(SchemaParser.KEY_ID)
        key_type = SchemaParser.type_from_dict(dict_obj.get(SchemaParser.KEY))

        value_id = dict_obj.get(SchemaParser.VALUE_ID)
        value_type = SchemaParser.type_from_dict(dict_obj.get(SchemaParser.VALUE))

        is_required = dict_obj.get(SchemaParser.VALUE_REQUIRED)

        if is_required:
            return MapType.of_required(key_id, value_id, key_type, value_type)
        else:
            return MapType.of_optional(key_id, value_id, key_type, value_type)

    @staticmethod
    def from_json(json_obj):
        if isinstance(json_obj, str):
            type_var = SchemaParser.type_from_dict(json.loads(json_obj))
        else:
            type_var = SchemaParser.type_from_dict(json_obj)

        if type_var is not None and type_var.is_nested_type() and type_var.as_nested_type().is_struct_type():
            return Schema(type_var.as_nested_type().as_struct_type().fields)
        else:
            raise RuntimeError("Cannot create schema, not a struct type: %s" % type_var)
