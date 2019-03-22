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

__all__ = ["AssignFreshIds",
           "assign_fresh_ids",
           "from_primitive_string",
           "get_projected_ids",
           "Conversions",
           "CustomOrderSchemaVisitor",
           "NestedType",
           "PrimitiveType",
           "Type",
           "TypeID",
           "BinaryType",
           "BooleanType",
           "DateType",
           "DecimalType",
           "DoubleType",
           "FixedType",
           "FloatType",
           "GetProjectedIds",
           "IntegerType",
           "IndexById",
           "IndexByName",
           "index_by_id",
           "index_by_name",
           "join",
           "ListType",
           "LongType",
           "MapType",
           "NestedField",
           "PruneColumns",
           "SchemaVisitor",
           "select",
           "select_not",
           "StringType",
           "StructType",
           "TimeType",
           "TimestampType",
           "UUIDType",
           "visit",
           "visit_custom_order",
           "VisitFieldFuture",
           "VisitFuture"
           ]

import re

from .conversions import Conversions
from .type import (NestedType,
                   PrimitiveType,
                   Type,
                   TypeID)
from .type_util import (assign_fresh_ids,
                        AssignFreshIds,
                        CustomOrderSchemaVisitor,
                        get_projected_ids,
                        GetProjectedIds,
                        index_by_id,
                        index_by_name,
                        IndexById,
                        IndexByName,
                        join,
                        PruneColumns,
                        SchemaVisitor,
                        select,
                        select_not,
                        visit,
                        visit_custom_order,
                        VisitFieldFuture,
                        VisitFuture)
from .types import (BinaryType,
                    BooleanType,
                    DateType,
                    DecimalType,
                    DoubleType,
                    FixedType,
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
                    UUIDType)

TYPES = {str(BooleanType.get()): BooleanType.get(),
         str(IntegerType.get()): IntegerType.get(),
         str(LongType.get()): LongType.get(),
         str(FloatType.get()): FloatType.get(),
         str(DoubleType.get()): DoubleType.get(),
         str(DateType.get()): DateType.get(),
         str(TimeType.get()): TimeType.get(),
         str(TimestampType.with_timezone()): TimestampType.with_timezone(),
         str(TimestampType.without_timezone()): TimestampType.without_timezone(),
         str(StringType.get()): StringType.get(),
         str(UUIDType.get()): UUIDType.get(),
         str(BinaryType.get()): BinaryType.get()}

FIXED = re.compile("fixed\\[(\\d+)\\]")
DECIMAL = re.compile("decimal\\((\\d+),\\s+(\\d+)\\)")


def from_primitive_string(type_string):
    lower_type_string = type_string.lower()
    if lower_type_string in TYPES.keys():
        return TYPES[lower_type_string]

    matches = FIXED.match(type_string)
    if matches:
        return FixedType.of_length(matches.group(1))

    matches = DECIMAL.match(type_string)
    if matches:
        return DecimalType.of(matches.group(1), matches.group(2))

    raise RuntimeError("Cannot parse type string to primitive: %s", type_string)
