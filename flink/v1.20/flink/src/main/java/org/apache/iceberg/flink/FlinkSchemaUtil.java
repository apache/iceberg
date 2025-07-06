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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Converter between Flink types and Iceberg type. The conversion is not a 1:1 mapping that not
 * allows back-and-forth conversion. So some information might get lost during the back-and-forth
 * conversion.
 *
 * <p>This inconsistent types:
 *
 * <ul>
 *   <li>map Iceberg UUID type to Flink BinaryType(16)
 *   <li>map Flink VarCharType(_) and CharType(_) to Iceberg String type
 *   <li>map Flink VarBinaryType(_) to Iceberg Binary type
 *   <li>map Flink TimeType(_) to Iceberg Time type (microseconds)
 *   <li>map Flink TimestampType(_) to Iceberg Timestamp without zone type (microseconds)
 *   <li>map Flink LocalZonedTimestampType(_) to Iceberg Timestamp with zone type (microseconds)
 *   <li>map Flink MultiSetType to Iceberg Map type(element, int)
 * </ul>
 *
 * <p>
 */
public class FlinkSchemaUtil {

  private FlinkSchemaUtil() {}

  /**
   * @deprecated will be removed in 2.0.0; use {@link #convert(ResolvedSchema)} instead.
   */
  @Deprecated
  public static Schema convert(TableSchema schema) {
    LogicalType schemaType = schema.toRowDataType().getLogicalType();
    Preconditions.checkArgument(
        schemaType instanceof RowType, "Schema logical type should be row type.");

    RowType root = (RowType) schemaType;
    Type converted = root.accept(new FlinkTypeToType(root));

    Schema icebergSchema = new Schema(converted.asStructType().fields());
    if (schema.getPrimaryKey().isPresent()) {
      return freshIdentifierFieldIds(icebergSchema, schema.getPrimaryKey().get().getColumns());
    } else {
      return icebergSchema;
    }
  }

  /** Convert the flink table schema to apache iceberg schema with column comment. */
  public static Schema convert(ResolvedSchema flinkSchema) {
    List<Column> tableColumns = flinkSchema.getColumns();
    // copy from org.apache.flink.table.api.Schema#toRowDataType
    DataTypes.Field[] fields =
        tableColumns.stream()
            .map(
                column -> {
                  if (column.getComment().isPresent()) {
                    return DataTypes.FIELD(
                        column.getName(), column.getDataType(), column.getComment().get());
                  } else {
                    return DataTypes.FIELD(column.getName(), column.getDataType());
                  }
                })
            .toArray(DataTypes.Field[]::new);

    LogicalType schemaType = DataTypes.ROW(fields).notNull().getLogicalType();
    Preconditions.checkArgument(
        schemaType instanceof RowType, "Schema logical type should be row type.");

    RowType root = (RowType) schemaType;
    Type converted = root.accept(new FlinkTypeToType(root));
    Schema icebergSchema = new Schema(converted.asStructType().fields());
    return flinkSchema
        .getPrimaryKey()
        .map(pk -> freshIdentifierFieldIds(icebergSchema, pk.getColumns()))
        .orElse(icebergSchema);
  }

  private static Schema freshIdentifierFieldIds(Schema icebergSchema, List<String> primaryKeys) {
    // Locate the identifier field id list.
    Set<Integer> identifierFieldIds = Sets.newHashSet();
    for (String primaryKey : primaryKeys) {
      Types.NestedField field = icebergSchema.findField(primaryKey);
      Preconditions.checkNotNull(
          field,
          "Cannot find field ID for the primary key column %s in schema %s",
          primaryKey,
          icebergSchema);
      identifierFieldIds.add(field.fieldId());
    }
    return new Schema(
        icebergSchema.schemaId(), icebergSchema.asStruct().fields(), identifierFieldIds);
  }

  /**
   * Convert a Flink {@link TableSchema} to a {@link Schema} based on the given schema.
   *
   * <p>This conversion does not assign new ids; it uses ids from the base schema.
   *
   * <p>Data types, field order, and nullability will match the Flink type. This conversion may
   * return a schema that is not compatible with base schema.
   *
   * @param baseSchema a Schema on which conversion is based
   * @param flinkSchema a Flink TableSchema
   * @return the equivalent Schema
   * @throws IllegalArgumentException if the type cannot be converted or there are missing ids
   * @deprecated since 1.10.0, will be removed in 2.0.0. Use {@link #convert(Schema,
   *     ResolvedSchema)} instead.
   */
  @Deprecated
  public static Schema convert(Schema baseSchema, TableSchema flinkSchema) {
    // convert to a type with fresh ids
    Types.StructType struct = convert(flinkSchema).asStruct();
    // reassign ids to match the base schema
    Schema schema = TypeUtil.reassignIds(new Schema(struct.fields()), baseSchema);
    // reassign doc to match the base schema
    schema = TypeUtil.reassignDoc(schema, baseSchema);

    // fix types that can't be represented in Flink (UUID)
    Schema fixedSchema = FlinkFixupTypes.fixup(schema, baseSchema);
    if (flinkSchema.getPrimaryKey().isPresent()) {
      return freshIdentifierFieldIds(fixedSchema, flinkSchema.getPrimaryKey().get().getColumns());
    } else {
      return fixedSchema;
    }
  }

  /**
   * Convert a Flink {@link ResolvedSchema} to a {@link Schema} based on the given schema.
   *
   * <p>This conversion does not assign new ids; it uses ids from the base schema.
   *
   * <p>Data types, field order, and nullability will match the Flink type. This conversion may
   * return a schema that is not compatible with base schema.
   *
   * @param baseSchema a Schema on which conversion is based
   * @param flinkSchema a Flink ResolvedSchema
   * @return the equivalent Schema
   * @throws IllegalArgumentException if the type cannot be converted or there are missing ids
   */
  public static Schema convert(Schema baseSchema, ResolvedSchema flinkSchema) {
    // convert to a type with fresh ids
    Types.StructType struct = convert(flinkSchema).asStruct();
    // reassign ids to match the base schema
    Schema schema = TypeUtil.reassignIds(new Schema(struct.fields()), baseSchema);
    // reassign doc to match the base schema
    schema = TypeUtil.reassignDoc(schema, baseSchema);

    // fix types that can't be represented in Flink (UUID)
    Schema fixedSchema = FlinkFixupTypes.fixup(schema, baseSchema);
    return flinkSchema
        .getPrimaryKey()
        .map(pk -> freshIdentifierFieldIds(fixedSchema, pk.getColumns()))
        .orElse(fixedSchema);
  }

  /**
   * Convert a {@link Schema} to a {@link RowType Flink type}.
   *
   * @param schema a Schema
   * @return the equivalent Flink type
   * @throws IllegalArgumentException if the type cannot be converted to Flink
   */
  public static RowType convert(Schema schema) {
    return (RowType) TypeUtil.visit(schema, new TypeToFlinkType());
  }

  /**
   * Convert a {@link Type} to a {@link LogicalType Flink type}.
   *
   * @param type a Type
   * @return the equivalent Flink type
   * @throws IllegalArgumentException if the type cannot be converted to Flink
   */
  public static LogicalType convert(Type type) {
    return TypeUtil.visit(type, new TypeToFlinkType());
  }

  /**
   * Convert a {@link LogicalType Flink type} to a {@link Type}.
   *
   * @param flinkType a FlinkType
   * @return the equivalent Iceberg type
   */
  public static Type convert(LogicalType flinkType) {
    return flinkType.accept(new FlinkTypeToType());
  }

  /**
   * Convert a {@link RowType} to a {@link TableSchema}.
   *
   * @param rowType a RowType
   * @return Flink TableSchema
   * @deprecated since 1.10.0, will be removed in 2.0.0. Use {@link #toResolvedSchema(RowType)}
   *     instead
   */
  @Deprecated
  public static TableSchema toSchema(RowType rowType) {
    TableSchema.Builder builder = TableSchema.builder();
    for (RowType.RowField field : rowType.getFields()) {
      builder.field(field.getName(), TypeConversions.fromLogicalToDataType(field.getType()));
    }
    return builder.build();
  }

  /**
   * Convert a {@link RowType} to a {@link ResolvedSchema}.
   *
   * @param rowType a RowType
   * @return Flink ResolvedSchema
   */
  public static ResolvedSchema toResolvedSchema(RowType rowType) {
    List<Column> columns = Lists.newArrayListWithExpectedSize(rowType.getFieldCount());
    for (RowType.RowField field : rowType.getFields()) {
      columns.add(
          Column.physical(field.getName(), TypeConversions.fromLogicalToDataType(field.getType())));
    }

    return ResolvedSchema.of(columns);
  }

  /**
   * Convert a {@link Schema} to a {@link TableSchema}.
   *
   * @param schema iceberg schema to convert.
   * @return Flink TableSchema.
   * @deprecated since 1.10.0, will be removed in 2.0.0. Use {@link #toResolvedSchema(Schema)}
   *     instead
   */
  @Deprecated
  public static TableSchema toSchema(Schema schema) {
    TableSchema.Builder builder = TableSchema.builder();

    // Add columns.
    for (RowType.RowField field : convert(schema).getFields()) {
      builder.field(field.getName(), TypeConversions.fromLogicalToDataType(field.getType()));
    }

    // Add primary key.
    Set<Integer> identifierFieldIds = schema.identifierFieldIds();
    if (!identifierFieldIds.isEmpty()) {
      List<String> columns = Lists.newArrayListWithExpectedSize(identifierFieldIds.size());
      for (Integer identifierFieldId : identifierFieldIds) {
        String columnName = schema.findColumnName(identifierFieldId);
        Preconditions.checkNotNull(
            columnName, "Cannot find field with id %s in schema %s", identifierFieldId, schema);

        columns.add(columnName);
      }
      builder.primaryKey(columns.toArray(new String[0]));
    }

    return builder.build();
  }

  /**
   * Convert a {@link Schema} to a {@link ResolvedSchema}.
   *
   * @param schema iceberg schema to convert.
   * @return Flink ResolvedSchema.
   */
  public static ResolvedSchema toResolvedSchema(Schema schema) {
    RowType rowType = convert(schema);
    List<Column> columns = Lists.newArrayListWithExpectedSize(rowType.getFieldCount());

    // Add columns.
    for (RowType.RowField field : rowType.getFields()) {
      columns.add(
          Column.physical(field.getName(), TypeConversions.fromLogicalToDataType(field.getType())));
    }

    // Add primary key.
    Set<Integer> identifierFieldIds = schema.identifierFieldIds();
    UniqueConstraint uniqueConstraint = null;
    if (!identifierFieldIds.isEmpty()) {
      List<String> primaryKeyColumns =
          Lists.newArrayListWithExpectedSize(identifierFieldIds.size());
      for (Integer identifierFieldId : identifierFieldIds) {
        String columnName = schema.findColumnName(identifierFieldId);
        Preconditions.checkNotNull(
            columnName, "Cannot find field with id %s in schema %s", identifierFieldId, schema);

        primaryKeyColumns.add(columnName);
      }

      uniqueConstraint =
          UniqueConstraint.primaryKey(UUID.randomUUID().toString(), primaryKeyColumns);

      validatePrimaryKey(uniqueConstraint, columns);
    }

    return new ResolvedSchema(columns, Collections.emptyList(), uniqueConstraint);
  }

  /**
   * Copied from
   * org.apache.flink.table.catalog.DefaultSchemaResolver#validatePrimaryKey(org.apache.flink.table.catalog.UniqueConstraint,
   * java.util.List)
   */
  private static void validatePrimaryKey(UniqueConstraint primaryKey, List<Column> columns) {
    final Map<String, Column> columnsByNameLookup =
        columns.stream().collect(Collectors.toMap(Column::getName, Function.identity()));

    final Set<String> duplicateColumns =
        primaryKey.getColumns().stream()
            .filter(name -> Collections.frequency(primaryKey.getColumns(), name) > 1)
            .collect(Collectors.toSet());

    if (!duplicateColumns.isEmpty()) {
      throw new ValidationException(
          String.format(
              "Invalid primary key '%s'. A primary key must not contain duplicate columns. Found: %s",
              primaryKey.getName(), duplicateColumns));
    }

    for (String columnName : primaryKey.getColumns()) {
      Column column = columnsByNameLookup.get(columnName);
      if (column == null) {
        throw new ValidationException(
            String.format(
                "Invalid primary key '%s'. Column '%s' does not exist.",
                primaryKey.getName(), columnName));
      }

      if (!column.isPhysical()) {
        throw new ValidationException(
            String.format(
                "Invalid primary key '%s'. Column '%s' is not a physical column.",
                primaryKey.getName(), columnName));
      }

      final LogicalType columnType = column.getDataType().getLogicalType();
      if (columnType.isNullable()) {
        throw new ValidationException(
            String.format(
                "Invalid primary key '%s'. Column '%s' is nullable.",
                primaryKey.getName(), columnName));
      }
    }
  }
}
