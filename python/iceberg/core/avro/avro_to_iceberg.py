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

import fastavro
from iceberg.api import Schema
from iceberg.api.types import (BinaryType,
                               BooleanType,
                               DateType,
                               DoubleType,
                               FloatType,
                               IntegerType,
                               ListType,
                               LongType,
                               MapType,
                               NestedField,
                               StringType,
                               StructType,
                               TimestampType,
                               TimeType,
                               TypeID)


class AvroToIceberg(object):
    FIELD_ID_PROP = "field-id"
    FIELD_TYPE_PROP = "type"
    FIELD_NAME_PROP = "name"
    FIELD_LOGICAL_TYPE_PROP = "logicalType"
    FIELD_FIELDS_PROP = "fields"
    FIELD_ITEMS_PROP = "items"
    FIELD_ELEMENT_ID_PROP = "element-id"

    AVRO_JSON_PRIMITIVE_TYPES = ("boolean", "int", "long", "float", "double", "bytes", "string")
    AVRO_JSON_COMPLEX_TYPES = ("record", "array", "enum", "fixed")

    TYPE_PROCESSING_MAP = {str: lambda x, y: AvroToIceberg.convert_str_type(x, y),
                           dict: lambda x, y: AvroToIceberg.convert_complex_type(x, y),
                           list: lambda x, y: AvroToIceberg.convert_union_type(x, y)}

    COMPLEX_TYPE_PROCESSING_MAP = {"record": lambda x, y: AvroToIceberg.convert_record_type(x, y),
                                   "array": lambda x, y: AvroToIceberg.convert_array_type(x, y),
                                   "map": lambda x, y: AvroToIceberg.convert_map_type(x, y)}

    PRIMITIVE_FIELD_TYPE_MAP = {"boolean": BooleanType.get(),
                                "bytes": BinaryType.get(),
                                "date": DateType.get(),
                                "double": DoubleType.get(),
                                "float": FloatType.get(),
                                "int": IntegerType.get(),
                                "long": LongType.get(),
                                "string": StringType.get(),
                                "time-millis": TimeType.get(),
                                "timestamp-millis": TimestampType.without_timezone()}

    PROCESS_FUNCS = {TypeID.STRUCT: lambda avro_row, field: AvroToIceberg.get_field_from_struct(avro_row, field),
                     TypeID.LIST: lambda avro_row, field: AvroToIceberg.get_field_from_list(avro_row, field),
                     TypeID.MAP: lambda avro_row, field: AvroToIceberg.get_field_from_map(avro_row, field)}

    @staticmethod
    def convert_avro_schema_to_iceberg(avro_schema):
        if avro_schema.get(AvroToIceberg.FIELD_TYPE_PROP) != "record":
            raise RuntimeError("Cannot convert avro schema to iceberg %s" % avro_schema)

        struct = AvroToIceberg.convert_type(avro_schema, None)

        return Schema(struct[0].fields)

    @staticmethod
    def convert_record_type(avro_field, next_id=None):
        avro_field_type = avro_field.get(AvroToIceberg.FIELD_TYPE_PROP)

        if avro_field_type != "record":
            raise RuntimeError("Field type muse be 'record': %s" % avro_field_type)

        fields = avro_field.get(AvroToIceberg.FIELD_FIELDS_PROP)

        iceberg_fields = []
        if next_id is None:
            next_id = len(fields)
        for field in fields:
            iceberg_field, next_id = AvroToIceberg.convert_avro_field_to_iceberg(field, next_id=next_id)
            iceberg_fields.append(iceberg_field)

        return StructType.of(iceberg_fields), next_id

    @staticmethod
    def convert_avro_field_to_iceberg(field, next_id):
        field_type, is_optional, next_id = AvroToIceberg.convert_type(field, next_id)

        if field.get(AvroToIceberg.FIELD_ID_PROP) is None:
            return field_type, next_id

        if is_optional:
            return NestedField.optional(field.get(AvroToIceberg.FIELD_ID_PROP),
                                        field.get(AvroToIceberg.FIELD_NAME_PROP),
                                        field_type), next_id
        else:
            return NestedField.required(field.get(AvroToIceberg.FIELD_ID_PROP),
                                        field.get(AvroToIceberg.FIELD_NAME_PROP),
                                        field_type), next_id

    @staticmethod
    def convert_type(field, next_id=None):
        avro_field_type = field.get(AvroToIceberg.FIELD_TYPE_PROP)

        optional = AvroToIceberg.is_option_schema(avro_field_type)

        processing_func = AvroToIceberg.TYPE_PROCESSING_MAP.get(type(avro_field_type))
        if processing_func is None:
            raise RuntimeError("No function found to process %s" % avro_field_type)

        iceberg_type, next_id = processing_func(field, next_id)

        return iceberg_type, optional, next_id

    @staticmethod
    def convert_str_type(avro_field, next_id=None):
        avro_field_type = avro_field.get(AvroToIceberg.FIELD_TYPE_PROP)
        logical_type = avro_field.get(AvroToIceberg.FIELD_LOGICAL_TYPE_PROP)
        if not isinstance(avro_field_type, str):
            raise RuntimeError("Field type must be of type str: %s" % avro_field_type)

        if avro_field_type in AvroToIceberg.AVRO_JSON_PRIMITIVE_TYPES:
            if logical_type is not None:
                return AvroToIceberg.PRIMITIVE_FIELD_TYPE_MAP.get(logical_type), next_id
            else:
                return AvroToIceberg.PRIMITIVE_FIELD_TYPE_MAP.get(avro_field_type), next_id

        elif avro_field_type in AvroToIceberg.AVRO_JSON_COMPLEX_TYPES:
            if logical_type is not None:
                processing_func = AvroToIceberg.COMPLEX_TYPE_PROCESSING_MAP.get(logical_type)
            else:
                processing_func = AvroToIceberg.COMPLEX_TYPE_PROCESSING_MAP.get(avro_field_type)

            if processing_func is None:
                raise RuntimeError("No function found to process %s" % avro_field_type)

            return processing_func(avro_field, next_id)
        else:
            raise RuntimeError("Unknown type %s" % avro_field_type)

    @staticmethod
    def convert_complex_type(avro_field, next_id=None):
        avro_field_type = avro_field.get(AvroToIceberg.FIELD_TYPE_PROP)
        if not isinstance(avro_field_type, dict):
            raise RuntimeError("Complex field type must be of type dict: %s" % avro_field_type)

        return AvroToIceberg.convert_avro_field_to_iceberg(avro_field_type, next_id)

    @staticmethod
    def convert_union_type(avro_field, next_id=None):
        avro_field_type = avro_field.get(AvroToIceberg.FIELD_TYPE_PROP)
        if not isinstance(avro_field_type, list):
            raise RuntimeError("Union field type must be of type list: %s" % avro_field_type)

        if len(avro_field_type) > 2:
            raise RuntimeError("Cannot process unions larger than 2 items: %s" % avro_field_type)
        for item in avro_field_type:
            if isinstance(item, str) and item == "null":
                continue
            avro_field_type = item
        avro_field[AvroToIceberg.FIELD_TYPE_PROP] = avro_field_type
        items = AvroToIceberg.convert_type(avro_field, next_id)
        return items[0], items[2]

    @staticmethod
    def convert_array_type(avro_field, next_id=None):
        avro_field_type = avro_field.get(AvroToIceberg.FIELD_TYPE_PROP)
        if avro_field_type != "array":
            raise RuntimeError("Avro type must be array: %s" % avro_field_type)
        element_id = avro_field.get(AvroToIceberg.FIELD_ELEMENT_ID_PROP)
        items = avro_field.get(AvroToIceberg.FIELD_ITEMS_PROP)

        is_optional = AvroToIceberg.is_option_schema(items)

        if isinstance(items, str) and items in AvroToIceberg.PRIMITIVE_FIELD_TYPE_MAP:
            item_type = AvroToIceberg.PRIMITIVE_FIELD_TYPE_MAP.get(items)
            if item_type is None:
                raise RuntimeError("No mapping found for type %s" % items)
        else:
            raise RuntimeError("Complex list types not yet implemented")

        if is_optional:
            return ListType.of_optional(element_id, item_type), next_id
        else:
            return ListType.of_required(element_id, item_type), next_id

    @staticmethod
    def convert_map_type(avro_field, next_id=None):
        avro_field_type = avro_field.get(AvroToIceberg.FIELD_TYPE_PROP)
        avro_logical_type = avro_field.get(AvroToIceberg.FIELD_LOGICAL_TYPE_PROP)
        if avro_field_type != "array" or avro_logical_type != "map":
            raise RuntimeError("Avro type must be array and logical type must be map: %s" % avro_logical_type)
        is_optional = False
        items = avro_field.get(AvroToIceberg.FIELD_ITEMS_PROP)
        for field in items.get(AvroToIceberg.FIELD_FIELDS_PROP, list()):
            if field.get(AvroToIceberg.FIELD_NAME_PROP) == "key":
                key_id = field.get(AvroToIceberg.FIELD_ID_PROP)
                if not isinstance(field.get(AvroToIceberg.FIELD_TYPE_PROP), str):
                    raise RuntimeError("Support for complex map keys not yet implemented")
                key_type = AvroToIceberg.PRIMITIVE_FIELD_TYPE_MAP.get(field.get(AvroToIceberg.FIELD_TYPE_PROP))
            elif field.get(AvroToIceberg.FIELD_NAME_PROP) == "value":
                value_id = field.get(AvroToIceberg.FIELD_ID_PROP)
                if not isinstance(field.get(AvroToIceberg.FIELD_TYPE_PROP), str):
                    raise RuntimeError("Support for complex map values not yet imeplemented")
                value_type = AvroToIceberg.PRIMITIVE_FIELD_TYPE_MAP.get(field.get(AvroToIceberg.FIELD_TYPE_PROP))

        if is_optional:
            return MapType.of_optional(key_id, value_id, key_type, value_type), next_id
        else:
            return MapType.of_required(key_id, value_id, key_type, value_type), next_id

    @staticmethod
    def is_option_schema(field_type):
        if isinstance(field_type, list) and len(field_type) == 2 and "null" in field_type:
            return True

        return False

    @staticmethod
    def read_avro_file(iceberg_schema, data_file):
        fo = data_file.new_fo()
        avro_reader = fastavro.reader(fo)
        for avro_row in avro_reader:
            iceberg_row = dict()
            for field in iceberg_schema.as_struct().fields:
                iceberg_row[field.name] = AvroToIceberg.get_field_from_avro(avro_row, field)
            yield iceberg_row
        fo.close()

    @staticmethod
    def read_avro_row(iceberg_schema, avro_reader):
        try:
            for avro_row in avro_reader:
                iceberg_row = dict()
                for field in iceberg_schema.as_struct().fields:
                    iceberg_row[field.name] = AvroToIceberg.get_field_from_avro(avro_row, field)
                yield iceberg_row
        except StopIteration:
            return

    @staticmethod
    def get_field_from_avro(avro_row, field):
        try:
            return AvroToIceberg.PROCESS_FUNCS.get(field.type.type_id,
                                                   AvroToIceberg.get_field_from_primitive)(avro_row, field)
        except KeyError:
            raise RuntimeError("Don't know how to get field of type: %s" % field.type.type_id)

    @staticmethod
    def get_field_from_primitive(avro_row, field):
        try:
            return avro_row[field.name]
        except KeyError:
            if field.is_required:
                raise RuntimeError("Field is required but missing in source %s\n%s:" % (field, avro_row))

    @staticmethod
    def get_field_from_struct(avro_row, field):
        field_obj = {}
        for nested_field in field.type.fields:
            field_obj[nested_field.name] = AvroToIceberg.get_field_from_avro(avro_row[field.name], nested_field)
        return field_obj

    @staticmethod
    def get_field_from_list(avro_row, field):
        try:
            return avro_row[field.name]
        except KeyError:
            if field.is_required:
                raise RuntimeError("Field is required but missing in source %s\n%s:" % (field, avro_row))

    @staticmethod
    def get_field_from_map(avro_row, field):
        val_map = dict()

        try:
            avro_value = avro_row[field.name]
        except KeyError:
            if field.is_required:
                raise RuntimeError("Field is required but missing in source %s\n%s:" % (field, avro_row))
            else:
                return None

        for val in avro_value:
            val_map[val['key']] = val['value']

        return val_map
