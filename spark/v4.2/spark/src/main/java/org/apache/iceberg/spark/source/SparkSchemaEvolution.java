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
package org.apache.iceberg.spark.source;

import java.util.Arrays;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.types.DataType;

/**
 * Decides which column changes Spark may apply to a table through automatic schema evolution.
 *
 * <p>The changes are checked against the schema pinned when the table was loaded.
 */
class SparkSchemaEvolution {
  private final Table table;
  private final Schema schema;
  private volatile Set<Integer> mapKeyFieldIds;

  SparkSchemaEvolution(Table table, Schema schema) {
    this.table = table;
    this.schema = schema;
  }

  boolean supportsColumnChange(TableChange.ColumnChange change) {
    if (isMapKeyChange(change)) {
      return false;
    }

    if (change instanceof TableChange.AddColumn) {
      TableChange.AddColumn add = (TableChange.AddColumn) change;
      if (!add.isNullable() || add.defaultValue() != null) {
        return false;
      }

      Type type = tryConvert(add.dataType());
      return type != null && isSupportedAtFormatVersion(type);
    } else if (change instanceof TableChange.UpdateColumnType) {
      return supportsTypeUpdate((TableChange.UpdateColumnType) change);
    } else if (change instanceof TableChange.UpdateColumnNullability) {
      return supportsNullabilityUpdate((TableChange.UpdateColumnNullability) change);
    } else if (change instanceof TableChange.DeleteColumn) {
      return supportsDeleteColumn((TableChange.DeleteColumn) change);
    } else {
      return change instanceof TableChange.RenameColumn
          || change instanceof TableChange.UpdateColumnComment
          || change instanceof TableChange.UpdateColumnPosition;
    }
  }

  private Set<Integer> mapKeyFieldIds() {
    if (mapKeyFieldIds == null) {
      this.mapKeyFieldIds = mapKeyFieldIds(schema);
    }

    return mapKeyFieldIds;
  }

  private static Set<Integer> mapKeyFieldIds(Schema schema) {
    Set<Integer> keyFieldIds = Sets.newHashSet();
    for (Types.NestedField field : TypeUtil.indexById(schema.asStruct()).values()) {
      if (field.type().isMapType()) {
        Types.MapType map = field.type().asMapType();
        keyFieldIds.addAll(TypeUtil.indexById(Types.StructType.of(map.fields().get(0))).keySet());
      }
    }

    return keyFieldIds;
  }

  private boolean isMapKeyChange(TableChange.ColumnChange change) {
    String[] fieldNames = change.fieldNames();
    int pathLength =
        change instanceof TableChange.AddColumn ? fieldNames.length - 1 : fieldNames.length;
    if (pathLength == 0) {
      return false;
    }

    Types.NestedField field =
        schema.findField(String.join(".", Arrays.copyOf(fieldNames, pathLength)));
    return field != null && mapKeyFieldIds().contains(field.fieldId());
  }

  private boolean supportsTypeUpdate(TableChange.UpdateColumnType update) {
    Types.NestedField field = schema.findField(String.join(".", update.fieldNames()));
    if (field == null) {
      return false;
    }

    Type newType = tryConvert(update.newDataType());
    return newType != null
        && newType.isPrimitiveType()
        && TypeUtil.isPromotionAllowed(field.type(), newType.asPrimitiveType())
        && SparkSchemaUtil.convert(newType).equals(update.newDataType());
  }

  private boolean supportsNullabilityUpdate(TableChange.UpdateColumnNullability update) {
    Types.NestedField field = schema.findField(String.join(".", update.fieldNames()));
    return update.nullable() && field != null && !containsIdentifierField(field);
  }

  private boolean supportsDeleteColumn(TableChange.DeleteColumn delete) {
    Types.NestedField field = schema.findField(String.join(".", delete.fieldNames()));
    if (field == null || isCollectionElement(delete.fieldNames())) {
      return false;
    }

    // Iceberg rejects dropping an identifier field or a field whose subtree contains one
    return !containsIdentifierField(field);
  }

  // a list element or a map key or value can only be dropped together with its collection
  private boolean isCollectionElement(String[] fieldNames) {
    if (fieldNames.length < 2) {
      return false;
    }

    Types.NestedField parent =
        schema.findField(String.join(".", Arrays.copyOf(fieldNames, fieldNames.length - 1)));
    return parent != null && (parent.type().isListType() || parent.type().isMapType());
  }

  private boolean containsIdentifierField(Types.NestedField field) {
    Set<Integer> identifierFieldIds = schema.identifierFieldIds();
    return TypeUtil.indexById(Types.StructType.of(field)).keySet().stream()
        .anyMatch(identifierFieldIds::contains);
  }

  private boolean isSupportedAtFormatVersion(Type type) {
    try {
      Schema typeSchema = new Schema(Types.NestedField.optional(-1, "value", type));
      Schema.checkCompatibility(typeSchema, TableUtil.formatVersion(table));
      return true;
    } catch (IllegalArgumentException | IllegalStateException e) {
      return false;
    }
  }

  private static Type tryConvert(DataType sparkType) {
    try {
      return SparkSchemaUtil.convert(sparkType);
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      return null;
    }
  }
}
