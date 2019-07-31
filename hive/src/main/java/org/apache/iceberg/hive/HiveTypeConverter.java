/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;


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
        return "string";
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
        // TODO may be just decimal?
        return String.format("decimal(%s,%s)", decimalType.precision(), decimalType.scale());
      case STRUCT:
        final Types.StructType structType = type.asStructType();
        final String nameToType = structType.fields().stream()
            .map(f -> String.format("%s:%s", f.name(), convert(f.type())))
            .collect(Collectors.joining(","));
        return String.format("struct<%s>", nameToType);
      case LIST:
        final Types.ListType listType = type.asListType();
        return String.format("array<%s>", convert(listType.elementType()));
      case MAP:
        final Types.MapType mapType = type.asMapType();
        return String.format("map<%s,%s>", convert(mapType.keyType()), convert(mapType.valueType()));
      default:
        throw new UnsupportedOperationException(type + " is not supported");
    }
  }
}
