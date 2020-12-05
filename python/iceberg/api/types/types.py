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

from .type import (NestedType,
                   PrimitiveType,
                   TypeID)


class BooleanType(PrimitiveType):
    __instance = None

    @staticmethod
    def get():
        if BooleanType.__instance is None:
            BooleanType()
        return BooleanType.__instance

    def __init__(self):
        if BooleanType.__instance is not None:
            raise Exception("Multiple Boolean Types created")
        BooleanType.__instance = self

    @property
    def type_id(self):
        return TypeID.BOOLEAN

    def __repr__(self):
        return "boolean"

    def __str__(self):
        return "boolean"


class IntegerType(PrimitiveType):
    __instance = None

    @staticmethod
    def get():
        if IntegerType.__instance is None:
            IntegerType()
        return IntegerType.__instance

    def __init__(self):
        if IntegerType.__instance is not None:
            raise Exception("Multiple Integer Types created")
        IntegerType.__instance = self

    @property
    def type_id(self):
        return TypeID.INTEGER

    def __repr__(self):
        return "int"

    def __str__(self):
        return "int"


class LongType(PrimitiveType):
    __instance = None

    @staticmethod
    def get():
        if LongType.__instance is None:
            LongType()
        return LongType.__instance

    def __init__(self):
        if LongType.__instance is not None:
            raise Exception("Multiple Long Types created")
        LongType.__instance = self

    @property
    def type_id(self):
        return TypeID.LONG

    def __repr__(self):
        return "long"

    def __str__(self):
        return "long"


class FloatType(PrimitiveType):
    __instance = None

    @staticmethod
    def get():
        if FloatType.__instance is None:
            FloatType()
        return FloatType.__instance

    def __init__(self):
        if FloatType.__instance is not None:
            raise Exception("Multiple Float Types created")
        FloatType.__instance = self

    @property
    def type_id(self):
        return TypeID.FLOAT

    def __repr__(self):
        return "float"

    def __str__(self):
        return "float"


class DoubleType(PrimitiveType):
    __instance = None

    @staticmethod
    def get():
        if DoubleType.__instance is None:
            DoubleType()
        return DoubleType.__instance

    def __init__(self):
        if DoubleType.__instance is not None:
            raise Exception("Multiple Double Types created")
        DoubleType.__instance = self

    @property
    def type_id(self):
        return TypeID.DOUBLE

    def __repr__(self):
        return "double"

    def __str__(self):
        return "double"


class DateType(PrimitiveType):
    __instance = None

    @staticmethod
    def get():
        if DateType.__instance is None:
            DateType()
        return DateType.__instance

    def __init__(self):
        if DateType.__instance is not None:
            raise Exception("Multiple Date Types created")
        DateType.__instance = self

    @property
    def type_id(self):
        return TypeID.DATE

    def __repr__(self):
        return "date"

    def __str__(self):
        return "date"


class TimeType(PrimitiveType):
    __instance = None

    @staticmethod
    def get():
        if TimeType.__instance is None:
            TimeType()
        return TimeType.__instance

    def __init__(self):
        if TimeType.__instance is not None:
            raise Exception("Multiple Time Types created")
        TimeType.__instance = self

    @property
    def type_id(self):
        return TypeID.TIME

    def __repr__(self):
        return "time"

    def __str__(self):
        return "time"


class TimestampType(PrimitiveType):
    __instance_with_tz = None
    __instance_without_tz = None

    @staticmethod
    def with_timezone():
        if not TimestampType.__instance_with_tz:
            TimestampType()
        return TimestampType.__instance_with_tz

    @staticmethod
    def without_timezone():
        if not TimestampType.__instance_without_tz:
            TimestampType(False)
        return TimestampType.__instance_without_tz

    def __init__(self, with_timezone=True):
        self.adjust_to_utc = with_timezone
        if (with_timezone and TimestampType.__instance_with_tz is not None)\
                or (not with_timezone and TimestampType.__instance_without_tz is not None):
            raise Exception("Multiple Timestamp Types created")

        if with_timezone:
            TimestampType.__instance_with_tz = self
        else:
            TimestampType.__instance_without_tz = self

    @property
    def type_id(self):
        return TypeID.TIMESTAMP

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, TimestampType):
            return False

        return self.adjust_to_utc == other.adjust_to_utc

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return TimestampType.__class__, self.adjust_to_utc

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        if self.adjust_to_utc:
            return "timestamptz"
        else:
            return "timestamp"

    def __str__(self):
        return self.__repr__()


class StringType(PrimitiveType):
    __instance = None

    @staticmethod
    def get():
        if StringType.__instance is None:
            StringType()
        return StringType.__instance

    def __init__(self):
        if StringType.__instance is not None:
            raise Exception("Multiple String Types created")
        StringType.__instance = self

    @property
    def type_id(self):
        return TypeID.STRING

    def __repr__(self):
        return "string"

    def __str__(self):
        return "string"


class UUIDType(PrimitiveType):
    __instance = None

    @staticmethod
    def get():
        if UUIDType.__instance is None:
            UUIDType()
        return UUIDType.__instance

    def __init__(self):
        if UUIDType.__instance is not None:
            raise Exception("Multiple UUID Types created")
        UUIDType.__instance = self

    @property
    def type_id(self):
        return TypeID.UUID

    def __repr__(self):
        return "uuid"

    def __str__(self):
        return "uuid"


class FixedType(PrimitiveType):

    @staticmethod
    def of_length(length):
        return FixedType(length)

    def __init__(self, length):
        self._length = length

    @property
    def length(self):
        return self._length

    @property
    def type_id(self):
        return TypeID.FIXED

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, FixedType):
            return False

        return self.length == other.length

    def __key(self):
        return FixedType.__class__, self.length

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "fixed[%s]" % (self.length)

    def __str__(self):
        return self.__repr__()


class BinaryType(PrimitiveType):
    __instance = None

    @staticmethod
    def get():
        if BinaryType.__instance is None:
            BinaryType()
        return BinaryType.__instance

    def __init__(self):
        if BinaryType.__instance is not None:
            raise Exception("Multiple Binary Types created")
        BinaryType.__instance = self

    @property
    def type_id(self):
        return TypeID.BINARY

    def __repr__(self):
        return "binary"

    def __str__(self):
        return "binary"


class DecimalType(PrimitiveType):

    @staticmethod
    def of(precision, scale):
        return DecimalType(precision, scale)

    def __init__(self, precision, scale):
        if int(precision) > 38:
            raise RuntimeError("Decimals with precision larger than 38 are not supported: %s", precision)
        self.precision = int(precision)
        self.scale = int(scale)

    @property
    def type_id(self):
        return TypeID.DECIMAL

    def __repr__(self):
        return "decimal(%s, %s)" % (self.precision, self.scale)

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, DecimalType):
            return False

        return self.precision == other.precision and self.scale == other.scale

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return DecimalType.__class__, self.precision, self.scale


class NestedField():
    length: int

    @staticmethod
    def optional(id, name, type_var, doc=None):
        return NestedField(True, id, name, type_var, doc=doc)

    @staticmethod
    def required(id, name, type, doc=None):
        return NestedField(False, id, name, type, doc=doc)

    def __init__(self, is_optional, id, name, type, doc=None):
        self.is_optional = is_optional
        self.id = id
        self.name = name
        self.type = type
        self.doc = doc

    @property
    def is_required(self):
        return not self.is_optional

    @property
    def field_id(self):
        return self.id

    def __repr__(self):
        return "%s: %s: %s %s(%s)" % (self.id,
                                      self.name,
                                      "optional" if self.is_optional else "required",
                                      self.type,
                                      self.doc)

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, NestedField):
            return False

        return self.is_optional == other.is_optional \
            and self.id == other.id \
            and self.name == other.name and self.type == other.type \
            and self.doc == other.doc

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        type_name = self.type.type_id.name
        return NestedField.__class__, self.is_optional, self.id, self.name, self.doc, type_name


class StructType(NestedType):
    FIELD_SEP = ", "

    @staticmethod
    def of(fields):
        return StructType(fields)

    def __init__(self, fields):
        if fields is None:
            raise RuntimeError("Field list cannot be None")

        self._fields = list()
        for i in range(0, len(fields)):
            self._fields.append(fields[i])

        self._fieldList = None
        self._fieldsByName = None
        self._fieldsByLowercaseName = None
        self._fieldsById = None

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, StructType):
            return False

        return self._fields == other._fields

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def fields(self):
        return self._lazy_field_list()

    def field(self, name=None, id=None):
        if name:
            return self._lazy_fields_by_name().get(name)
        elif id:
            return self._lazy_fields_by_id()[id]

        raise RuntimeError("No valid field info passed in ")

    def case_insensitive_field(self, name):
        return self._lazy_fields_by_lowercase_name().get(name)

    @property
    def type_id(self):
        return TypeID.STRUCT

    def is_struct_type(self):
        return True

    def as_struct_type(self):
        return self

    def __str__(self):
        return "struct<{}>".format(StructType.FIELD_SEP.join(str(x) for x in self.fields))

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return StructType.__class__, self.fields

    def _lazy_field_list(self):
        if self._fieldList is None:
            self._fieldList = tuple(self._fields)
        return self._fieldList

    def _lazy_fields_by_name(self):
        if self._fieldsByName is None:
            self.index_fields()
        return self._fieldsByName

    def _lazy_fields_by_lowercase_name(self):
        if self._fieldsByName is None:
            self.index_fields()
        return self._fieldsByName

    def _lazy_fields_by_id(self):
        if self._fieldsById is None:
            self.index_fields()
        return self._fieldsById

    def index_fields(self):
        self._fieldsByName = dict()
        self._fieldsByLowercaseName = dict()
        self._fieldsById = dict()

        for field in self.fields:
            self._fieldsByName[field.name] = field
            self._fieldsByLowercaseName[field.name.lower()] = field
            self._fieldsById[field.id] = field


class ListType(NestedType):
    @staticmethod
    def of_optional(element_id, element_type):
        if element_type is None:
            raise RuntimeError("Element type cannot be null")
        return ListType(NestedField.optional(element_id, "element", element_type))

    @staticmethod
    def of_required(element_id, element_type):
        if element_type is None:
            raise RuntimeError("Element type cannot be null")
        return ListType(NestedField.required(element_id, "element", element_type))

    def __init__(self, element_field):
        self.element_field = element_field
        self._fields = None

    @property
    def type_id(self):
        return TypeID.LIST

    @property
    def element_type(self):
        return self.element_field.type

    def field_type(self, name):
        if "element" == name:
            return self.element_type

    def field(self, id):
        if self.element_field.id == id:
            return self.element_field

    def fields(self):
        return self._lazyFieldsList()

    @property
    def element_id(self):
        return self.element_field.id

    def is_element_required(self):
        return not self.element_field.is_optional

    def is_element_optional(self):
        return self.element_field.is_optional

    def is_list_type(self):
        return True

    def as_list_type(self):
        return self

    def __str__(self):
        return "list<%s>" % self.element_field.type

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, ListType):
            return False

        return self.element_field == other.element_field

    def __ne__(self, other):
        return self.__eq__(other)

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return StructType.__class__, self.element_field

    def _lazyFieldsList(self):
        if self._fields is None:
            self._fields = [self.element_field]

        return self._fields


class MapType(NestedType):

    @staticmethod
    def of_optional(key_id, value_id, key_type, value_type):
        if value_type is None:
            raise RuntimeError("Value type cannot be null")

        return MapType(NestedField.required(key_id, 'key', key_type),
                       NestedField.optional(value_id, 'value', value_type))

    @staticmethod
    def of_required(key_id, value_id, key_type, value_type):
        if value_type is None:
            raise RuntimeError("Value type cannot be null")

        return MapType(NestedField.required(key_id, 'key', key_type),
                       NestedField.required(value_id, 'value', value_type))

    def __init__(self, key_field, value_field):
        self.key_field = key_field
        self.value_field = value_field
        self._fields = None

    @property
    def type_id(self):
        return TypeID.MAP

    def key_type(self):
        return self.key_field.type

    def value_type(self):
        return self.value_field.type

    def field_type(self, name):
        if "key" == name:
            return self.key_field.type
        elif "value" == name:
            return self.value_field.type

    def field(self, id):
        if self.key_field.id == id:
            return self.key_field
        elif self.value_field.id == id:
            return self.value_field

    def fields(self):
        return self._lazy_field_list()

    def key_id(self):
        return self.key_field.field_id

    def value_id(self):
        return self.value_field.field_id

    def is_value_optional(self):
        return self.value_field.is_optional

    def is_value_required(self):
        return not self.is_value_optional()

    def __str__(self):
        return "map<%s, %s>" % (self.key_field.type, self.value_field.type)

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, MapType):
            return False

        return self.key_field == other.key_field and self.value_field == other.value_field

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return MapType.__class__, self.key_field, self.value_field

    def _lazy_field_list(self):
        return tuple(self.key_field, self.value_field)
