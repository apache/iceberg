package com.netflix.iceberg.hive;
/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;

import java.util.stream.Collectors;

import static java.lang.String.format;


public final class HiveTypeConverter {

  private HiveTypeConverter() {

  }

  public static String convert(Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return "boolean";
      case INTEGER:
        return "int";
      case LONG:
        return "bigint";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case DATE:
        return "date";
      case TIME:
        throw new UnsupportedOperationException("Hive does not support time fields");
      case TIMESTAMP:
        return "timestamp";
      case STRING:
      case UUID:
        return "string";
      case FIXED:
        return "binary";
      case BINARY:
        return "binary";
      case DECIMAL:
        final Types.DecimalType decimalType = (Types.DecimalType) type;
        return format("decimal(%s,%s)", decimalType.precision(), decimalType.scale()); //TODO may be just decimal?
      case STRUCT:
        final Types.StructType structType = type.asStructType();
        final String nameToType = structType.fields().stream().map(
                f -> format("%s:%s", f.name(), convert(f.type()))
        ).collect(Collectors.joining(","));
        return format("struct<%s>", nameToType);
      case LIST:
        final Types.ListType listType = type.asListType();
        return format("array<%s>", convert(listType.elementType()));
      case MAP:
        final Types.MapType mapType = type.asMapType();
        return format("map<%s,%s>", convert(mapType.keyType()), convert(mapType.valueType()));
      default:
        throw new UnsupportedOperationException(type +" is not supported");
    }
  }
}
