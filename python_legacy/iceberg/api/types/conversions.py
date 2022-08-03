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

from decimal import Decimal, ROUND_HALF_UP
import struct
import sys
import uuid

from .type import TypeID
from .type_util import decimal_to_bytes


class Conversions(object):
    HIVE_NULL = "__HIVE_DEFAULT_PARTITION__"
    value_mapping = {TypeID.BOOLEAN: lambda as_str: as_str.lower() == "true" if as_str is not None else False,
                     TypeID.INTEGER: lambda as_str: int(float(as_str)),
                     TypeID.LONG: lambda as_str: int(float(as_str)),
                     TypeID.FLOAT: lambda as_str: float(as_str),
                     TypeID.DOUBLE: lambda as_str: float(as_str),
                     TypeID.STRING: lambda as_str: as_str,
                     TypeID.UUID: lambda as_str: uuid.UUID(as_str),
                     TypeID.FIXED: lambda as_str: bytearray(bytes(as_str, "UTF-8")
                                                            if sys.version_info >= (3, 0)
                                                            else bytes(as_str)),
                     TypeID.BINARY: lambda as_str: bytes(as_str, "UTF-8") if sys.version_info >= (3, 0) else bytes(as_str),
                     TypeID.DECIMAL: lambda as_str: Decimal(as_str),
                     }

    to_byte_buff_mapping = {TypeID.BOOLEAN: lambda type_id, value: struct.pack("<?", 1 if value else 0),
                            TypeID.INTEGER: lambda type_id, value: struct.pack("<i", value),
                            TypeID.DATE: lambda type_id, value: struct.pack("<i", value),
                            TypeID.LONG: lambda type_id, value: struct.pack("<q", value),
                            TypeID.TIME: lambda type_id, value: struct.pack("<q", value),
                            TypeID.TIMESTAMP: lambda type_id, value: struct.pack("<q", value),
                            TypeID.FLOAT: lambda type_id, value: struct.pack("<f", value),
                            TypeID.DOUBLE: lambda type_id, value: struct.pack("<d", value),
                            TypeID.STRING: lambda type_id, value: value.encode('UTF-8'),
                            TypeID.UUID: lambda type_id, value: struct.pack('>QQ', (value.int >> 64)
                                                                            & 0xFFFFFFFFFFFFFFFF, value.int
                                                                            & 0xFFFFFFFFFFFFFFFF),
                            TypeID.FIXED: lambda type_id, value: value,
                            TypeID.BINARY: lambda type_id, value: value,
                            TypeID.DECIMAL: decimal_to_bytes
                            }

    from_byte_buff_mapping = {TypeID.BOOLEAN: lambda type_var, value: struct.unpack('<?', value)[0] != 0,
                              TypeID.INTEGER: lambda type_var, value: struct.unpack('<i', value)[0],
                              TypeID.DATE: lambda type_var, value: struct.unpack('<i', value)[0],
                              TypeID.LONG: lambda type_var, value: struct.unpack('<q', value)[0],
                              TypeID.TIME: lambda type_var, value: struct.unpack('<q', value)[0],
                              TypeID.TIMESTAMP: lambda type_var, value: struct.unpack('<q', value)[0],
                              TypeID.FLOAT: lambda type_var, value: struct.unpack('<f', value)[0],
                              TypeID.DOUBLE: lambda type_var, value: struct.unpack('<d', value)[0],
                              TypeID.STRING: lambda type_var, value: bytes(value).decode("utf-8"),
                              TypeID.UUID: lambda type_var, value:
                              uuid.UUID(int=struct.unpack('>QQ', value)[0] << 64 | struct.unpack('>QQ', value)[1]),
                              TypeID.FIXED: lambda type_var, value: value,
                              TypeID.BINARY: lambda type_var, value: value,
                              TypeID.DECIMAL: lambda type_var, value:
                              Decimal(int.from_bytes(value, 'big', signed=True) * 10**-type_var.scale)
                              .quantize(Decimal("." + "".join(["0" for i in range(1, type_var.scale)]) + "1"),
                                        rounding=ROUND_HALF_UP)
                              }

    @staticmethod
    def from_partition_string(type_var, as_string):
        if as_string is None or Conversions.HIVE_NULL == as_string:
            return None
        part_func = Conversions.value_mapping.get(type_var.type_id)
        if part_func is None:
            raise RuntimeError(f"Unsupported type for from_partition_string: {type_var}")

        return part_func(as_string)

    @staticmethod
    def to_byte_buffer(type_id, value):
        try:
            return Conversions.to_byte_buff_mapping[type_id](type_id, value)
        except KeyError:
            raise TypeError(f"Cannot serialize type, no conversion mapping found for TypeID: {type_id}")

    @staticmethod
    def from_byte_buffer(type_var, buffer_var):
        try:
            return Conversions.from_byte_buff_mapping[type_var.type_id](type_var, buffer_var)
        except KeyError:
            raise TypeError(f"Cannot deserialize type, no conversion mapping found for TypeID: {type_var.type_id}")
