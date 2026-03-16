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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.schema.SchemaWithPartnerVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Visitor class which compares an input schema to a table schema and emits a compatibility {@link
 * Result}.
 *
 * <ul>
 *   <li>SAME: The two schemas are semantically identical
 *   <li>DATA_CONVERSION_NEEDED: We can evolve the data associated with the input schema to match
 *       the table schema.
 *   <li>SCHEMA_UPDATE_NEEDED: We need to migrate the table schema to match the input schema.
 * </ul>
 *
 * The input schema fields are compared to the table schema via their names.
 */
public class CompareSchemasVisitor
    extends SchemaWithPartnerVisitor<Integer, CompareSchemasVisitor.Result> {

  private final Schema tableSchema;
  private final boolean caseSensitive;
  private final boolean dropUnusedColumns;

  private CompareSchemasVisitor(
      Schema tableSchema, boolean caseSensitive, boolean dropUnusedColumns) {
    this.tableSchema = tableSchema;
    this.caseSensitive = caseSensitive;
    this.dropUnusedColumns = dropUnusedColumns;
  }

  public static Result visit(
      Schema dataSchema, Schema tableSchema, boolean caseSensitive, boolean dropUnusedColumns) {
    return visit(
        dataSchema,
        -1,
        new CompareSchemasVisitor(tableSchema, caseSensitive, dropUnusedColumns),
        new PartnerIdByNameAccessors(tableSchema, caseSensitive));
  }

  @VisibleForTesting
  @Deprecated
  public static Result visit(Schema dataSchema, Schema tableSchema) {
    return visit(dataSchema, tableSchema, true, false);
  }

  @Override
  public Result schema(Schema dataSchema, Integer tableSchemaId, Result downstream) {
    if (tableSchemaId == null) {
      return Result.SCHEMA_UPDATE_NEEDED;
    }

    return downstream;
  }

  @Override
  @SuppressWarnings("CyclomaticComplexity")
  public Result struct(Types.StructType struct, Integer tableSchemaId, List<Result> fields) {
    if (tableSchemaId == null) {
      return Result.SCHEMA_UPDATE_NEEDED;
    }

    Result result = fields.stream().reduce(Result::merge).orElse(Result.SCHEMA_UPDATE_NEEDED);

    if (result == Result.SCHEMA_UPDATE_NEEDED) {
      return Result.SCHEMA_UPDATE_NEEDED;
    }

    Type tableSchemaType =
        tableSchemaId == -1 ? tableSchema.asStruct() : tableSchema.findField(tableSchemaId).type();
    if (!tableSchemaType.isStructType()) {
      return Result.SCHEMA_UPDATE_NEEDED;
    }

    for (Types.NestedField tableField : tableSchemaType.asStructType().fields()) {
      if (getFieldFromStruct(tableField.name(), struct, caseSensitive) == null
          && (tableField.isRequired() || dropUnusedColumns)) {
        // If a field from the table schema does not exist in the input schema, then we won't visit
        // it. The only choice is to make the table field optional or drop it.
        return Result.SCHEMA_UPDATE_NEEDED;
      }
    }

    if (struct.fields().size() != tableSchemaType.asStructType().fields().size()) {
      return Result.DATA_CONVERSION_NEEDED;
    }

    for (int i = 0; i < struct.fields().size(); ++i) {
      String fieldName = struct.fields().get(i).name();
      String tableFieldName = tableSchemaType.asStructType().fields().get(i).name();
      if ((caseSensitive && !fieldName.equals(tableFieldName))
          || (!caseSensitive && !fieldName.equalsIgnoreCase(tableFieldName))) {
        return Result.DATA_CONVERSION_NEEDED;
      }
    }

    return result;
  }

  @Nullable
  static Types.NestedField getFieldFromStruct(
      String fieldName, Types.StructType struct, boolean caseSensitive) {
    return caseSensitive ? struct.field(fieldName) : struct.caseInsensitiveField(fieldName);
  }

  @Override
  public Result field(Types.NestedField field, Integer tableSchemaId, Result typeResult) {
    if (tableSchemaId == null) {
      return Result.SCHEMA_UPDATE_NEEDED;
    }

    if (typeResult != Result.SAME) {
      return typeResult;
    }

    if (tableSchema.findField(tableSchemaId).isRequired() && field.isOptional()) {
      return Result.SCHEMA_UPDATE_NEEDED;
    } else {
      return Result.SAME;
    }
  }

  @Override
  public Result list(Types.ListType list, Integer tableSchemaId, Result elementsResult) {
    if (tableSchemaId == null) {
      return Result.SCHEMA_UPDATE_NEEDED;
    }

    return elementsResult;
  }

  @Override
  public Result map(
      Types.MapType map, Integer tableSchemaId, Result keyResult, Result valueResult) {
    if (tableSchemaId == null) {
      return Result.SCHEMA_UPDATE_NEEDED;
    }

    return keyResult.merge(valueResult);
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public Result primitive(Type.PrimitiveType primitive, Integer tableSchemaId) {
    if (tableSchemaId == null) {
      return Result.SCHEMA_UPDATE_NEEDED;
    }

    Type tableSchemaType = tableSchema.findField(tableSchemaId).type();
    if (!tableSchemaType.isPrimitiveType()) {
      return Result.SCHEMA_UPDATE_NEEDED;
    }

    Type.PrimitiveType tableSchemaPrimitiveType = tableSchemaType.asPrimitiveType();
    if (primitive.equals(tableSchemaPrimitiveType)) {
      return Result.SAME;
    } else if (primitive.equals(Types.IntegerType.get())
        && tableSchemaPrimitiveType.equals(Types.LongType.get())) {
      return Result.DATA_CONVERSION_NEEDED;
    } else if (primitive.equals(Types.FloatType.get())
        && tableSchemaPrimitiveType.equals(Types.DoubleType.get())) {
      return Result.DATA_CONVERSION_NEEDED;
    } else if (primitive.equals(Types.DateType.get())
        && tableSchemaPrimitiveType.equals(Types.TimestampType.withoutZone())) {
      return Result.DATA_CONVERSION_NEEDED;
    } else if (primitive.typeId() == Type.TypeID.DECIMAL
        && tableSchemaPrimitiveType.typeId() == Type.TypeID.DECIMAL) {
      Types.DecimalType dataType = (Types.DecimalType) primitive;
      Types.DecimalType tableType = (Types.DecimalType) tableSchemaPrimitiveType;
      return dataType.scale() == tableType.scale() && dataType.precision() < tableType.precision()
          ? Result.DATA_CONVERSION_NEEDED
          : Result.SCHEMA_UPDATE_NEEDED;
    } else {
      return Result.SCHEMA_UPDATE_NEEDED;
    }
  }

  static class PartnerIdByNameAccessors implements PartnerAccessors<Integer> {
    private final Schema tableSchema;
    private boolean caseSensitive;

    PartnerIdByNameAccessors(Schema tableSchema, boolean caseSensitive) {
      this.tableSchema = tableSchema;
      this.caseSensitive = caseSensitive;
    }

    @Override
    public Integer fieldPartner(Integer tableSchemaFieldId, int fieldId, String name) {
      Types.StructType struct;
      if (tableSchemaFieldId == -1) {
        struct = tableSchema.asStruct();
      } else {
        struct = tableSchema.findField(tableSchemaFieldId).type().asStructType();
      }

      Types.NestedField field = getFieldFromStruct(name, struct, caseSensitive);
      if (field != null) {
        return field.fieldId();
      }

      return null;
    }

    @Override
    public Integer mapKeyPartner(Integer tableSchemaMapId) {
      Types.NestedField mapField = tableSchema.findField(tableSchemaMapId);
      if (mapField != null) {
        return mapField.type().asMapType().fields().get(0).fieldId();
      }

      return null;
    }

    @Override
    public Integer mapValuePartner(Integer tableSchemaMapId) {
      Types.NestedField mapField = tableSchema.findField(tableSchemaMapId);
      if (mapField != null) {
        return mapField.type().asMapType().fields().get(1).fieldId();
      }

      return null;
    }

    @Override
    public Integer listElementPartner(Integer tableSchemaListId) {
      Types.NestedField listField = tableSchema.findField(tableSchemaListId);
      if (listField != null) {
        return listField.type().asListType().fields().get(0).fieldId();
      }

      return null;
    }
  }

  public enum Result {
    SAME(0),
    DATA_CONVERSION_NEEDED(1),
    SCHEMA_UPDATE_NEEDED(2);

    private static final Map<Integer, Result> BY_ID = Maps.newHashMap();

    static {
      for (Result e : Result.values()) {
        if (BY_ID.put(e.id, e) != null) {
          throw new IllegalArgumentException("Duplicate id: " + e.id);
        }
      }
    }

    private final int id;

    Result(int id) {
      this.id = id;
    }

    private Result merge(Result other) {
      return BY_ID.get(Math.max(this.id, other.id));
    }
  }
}
