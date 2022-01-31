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

from decimal import Decimal
from enum import Enum, unique
import uuid


@unique
class TypeID(Enum):
    BOOLEAN = {"java_class": "Boolean.class", "python_class": bool, "id": 1}
    INTEGER = {"java_class": "Integer.class", "python_class": int, "id": 2}
    LONG = {"java_class": "Long.class", "python_class": int, "id": 3}
    FLOAT = {"java_class": "Float.class", "python_class": float, "id": 4}
    DOUBLE = {"java_class": "Double.class", "python_class": float, "id": 5}
    DATE = {"java_class": "Integer.class", "python_class": int, "id": 6}
    TIME = {"java_class": "Long.class", "python_class": int, "id": 7}
    TIMESTAMP = {"java_class": "Long.class", "python_class": int, "id": 8}
    STRING = {"java_class": "CharSequence.class", "python_class": str, "id": 9}
    UUID = {"java_class": "java.util.UUID.class", "python_class": uuid.UUID, "id": 10}
    FIXED = {"java_class": "ByteBuffer.class", "python_class": bytes, "id": 11}
    BINARY = {"java_class": "ByteBuffer.class", "python_class": bytearray, "id": 12}
    DECIMAL = {"java_class": "BigDecimal.class", "python_class": Decimal, "id": 13}
    STRUCT = {"java_class": "Void.class", "python_class": None, "id": 14}
    LIST = {"java_class": "Void.class", "python_class": None, "id": 15}
    MAP = {"java_class": "Void.class", "python_class": None, "id": 16}


class Type(object):
    length: int
    scale: int
    precision: int

    def __init__(self):
        pass

    def type_id(self):
        pass

    def is_primitive_type(self):
        return False

    def as_primitive_type(self):
        raise ValueError("Not a primitive type: " + self)

    def as_struct_type(self):
        raise ValueError("Not a struct type: " + self)

    def as_list_type(self):
        raise ValueError("Not a list type: " + self)

    def as_map_type(self):
        raise ValueError("Not a map type: " + self)

    def is_nested_type(self):
        return False

    def is_struct_type(self):
        return False

    def is_list_type(self):
        return False

    def is_map_type(self):
        return False

    def as_nested_type(self):
        raise ValueError("Not a nested type: " + self)


class PrimitiveType(Type):

    def __eq__(self, other):
        return type(self) == type(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_primitive_type(self):
        return True

    def as_primitive_type(self):
        return self


class NestedType(Type):

    def __init__(self):
        super(NestedType, self).__init__()

    def is_nested_type(self):
        return True

    def as_nested_type(self):
        return self

    def fields(self):
        pass

    def field_type(self, name):
        pass

    def field(self, id):
        pass
