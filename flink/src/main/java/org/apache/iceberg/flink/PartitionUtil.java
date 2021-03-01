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

package org.apache.iceberg.flink;

import java.util.Locale;
import java.util.regex.Matcher;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

class PartitionUtil {

  private PartitionUtil() {
  }

  static String partitionPropKey(String colName) {
    return String.format("%s%s", FlinkTableProperties.PARTITION_BY_PREFIX, colName);
  }

  static boolean isPartitionField(String propKey) {
    return propKey != null && propKey.startsWith(FlinkTableProperties.PARTITION_BY_PREFIX);
  }

  private static String partitionField(String propKey) {
    if (propKey != null && propKey.startsWith(FlinkTableProperties.PARTITION_BY_PREFIX)) {
      return propKey.replace(FlinkTableProperties.PARTITION_BY_PREFIX, "");
    } else {
      return null;
    }
  }

  static void addPartitionField(Schema schema, PartitionSpec.Builder builder, String propKey, String propValue) {
    String colName = partitionField(propKey);
    Preconditions.checkNotNull(colName,
        "Table property '%s' is not an valid partition field property, please use '%s<column-name>'", propKey,
        FlinkTableProperties.PARTITION_BY_PREFIX);

    Types.NestedField nestedField = schema.findField(colName);
    Preconditions.checkNotNull(nestedField, "Cannot find field %s in schema: %s", colName, schema);

    Transform<?, ?> transform = Transforms.fromString(nestedField.type(), propValue);
    String transformString = transform.toString();
    switch (transformType(transformString)) {
      case "bucket":
        builder.bucket(colName, findWidth(transformString));
        break;

      case "truncate":
        builder.truncate(colName, findWidth(transformString));
        break;

      case "identify":
        builder.identity(colName);
        break;

      case "year":
        builder.year(colName);
        break;

      case "month":
        builder.month(colName);
        break;

      case "day":
        builder.day(colName);
        break;

      case "hour":
        builder.hour(colName);
        break;

      case "void":
        builder.alwaysNull(colName);
        break;

      default:
        throw new IllegalArgumentException(
            String.format("Unknown transform %s for field %s", transformString, colName));
    }
  }

  static Term toIcebergTerm(String colName, Transform<?, ?> transform) {
    String transformString = transform.toString();
    switch (transformType(transformString)) {
      case "bucket":
        return Expressions.bucket(colName, findWidth(transformString));

      case "truncate":
        return Expressions.truncate(colName, findWidth(transformString));

      case "identify":
        return Expressions.ref(colName);

      case "year":
        return Expressions.year(colName);

      case "month":
        return Expressions.month(colName);

      case "day":
        return Expressions.day(colName);

      case "hour":
        return Expressions.hour(colName);

      default:
        throw new IllegalArgumentException(String.format("Unknown transform %s", transformString));
    }
  }

  private static String transformType(String transformString) {
    Matcher widthMatcher = Transforms.HAS_WIDTH.matcher(transformString);
    if (widthMatcher.matches()) {
      return widthMatcher.group(1);
    }

    if (transformString.equalsIgnoreCase("identify") ||
        transformString.equalsIgnoreCase("void") ||
        transformString.equalsIgnoreCase("year") ||
        transformString.equalsIgnoreCase("month") ||
        transformString.equalsIgnoreCase("day") ||
        transformString.equalsIgnoreCase("hour")) {
      return transformString.toLowerCase(Locale.ENGLISH);
    }

    return "unknown";
  }

  private static int findWidth(String transform) {
    Matcher widthMatcher = Transforms.HAS_WIDTH.matcher(transform);
    if (widthMatcher.matches()) {
      int parsedWidth;
      try {
        parsedWidth = Integer.parseInt(widthMatcher.group(2));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Unsupported width for transform: " + transform);
      }
      Preconditions.checkArgument(parsedWidth > 0 && parsedWidth < Integer.MAX_VALUE,
          "Unsupported width for transform: " + transform);

      return parsedWidth;
    } else {
      throw new IllegalArgumentException("Cannot parse the width from transform: " + transform);
    }
  }
}
