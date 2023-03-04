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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
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

  public static Schema convert(CatalogBaseTable table) {
    if (table instanceof ResolvedCatalogTable) {
      ResolvedCatalogTable catalogBaseTable = (ResolvedCatalogTable) table;
      return convert(catalogBaseTable.getResolvedSchema());
    }
    throw new IllegalArgumentException("Unknown kind of catalog base table: " + table.getClass());
  }

  /** Convert the flink table schema to apache iceberg schema. */
  public static Schema convert(ResolvedSchema schema) {
    LogicalType schemaType = schema.toPhysicalRowDataType().getLogicalType();
    Preconditions.checkArgument(
        schemaType instanceof RowType, "Schema logical type should be RowType.");

    RowType root = (RowType) schemaType;
    Type converted = root.accept(new FlinkTypeToType(root));

    Schema iSchema = new Schema(converted.asStructType().fields());
    return freshIdentifierFieldIds(iSchema, schema);
  }

  private static Schema freshIdentifierFieldIds(Schema iSchema, ResolvedSchema schema) {
    // Locate the identifier field id list.
    Set<Integer> identifierFieldIds = Sets.newHashSet();
    if (schema.getPrimaryKey().isPresent()) {
      for (String column : schema.getPrimaryKey().get().getColumns()) {
        Types.NestedField field = iSchema.findField(column);
        Preconditions.checkNotNull(
            field,
            "Cannot find field ID for the primary key column %s in schema %s",
            column,
            iSchema);
        identifierFieldIds.add(field.fieldId());
      }
    }

    return new Schema(iSchema.schemaId(), iSchema.asStruct().fields(), identifierFieldIds);
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
    return freshIdentifierFieldIds(fixedSchema, flinkSchema);
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
   * Convert a {@link RowType} to a {@link ResolvedSchema}.
   *
   * @param rowType a RowType
   * @return Flink ResolvedSchema
   */
  public static ResolvedSchema toSchema(RowType rowType) {
    List<Column> columns = Lists.newArrayList();
    for (RowType.RowField field : rowType.getFields()) {
      Column column =
          Column.physical(field.getName(), TypeConversions.fromLogicalToDataType(field.getType()));
      columns.add(column);
    }
    return ResolvedSchema.of(columns);
  }

  /**
   * Convert a {@link Schema} to a {@link ResolvedSchema}.
   *
   * @param schema iceberg schema to convert.
   * @return Flink ResolvedSchema.
   */
  public static ResolvedSchema toSchema(Schema schema) {

    // Add columns.
    List<Column> schemaColumns = Lists.newArrayList();
    for (RowType.RowField field : convert(schema).getFields()) {
      schemaColumns.add(
          Column.physical(field.getName(), TypeConversions.fromLogicalToDataType(field.getType())));
    }

    // Add primary key.
    Set<Integer> identifierFieldIds = schema.identifierFieldIds();
    UniqueConstraint primaryKey = null;
    if (!identifierFieldIds.isEmpty()) {
      List<String> columns = Lists.newArrayListWithExpectedSize(identifierFieldIds.size());
      for (Integer identifierFieldId : identifierFieldIds) {
        String columnName = schema.findColumnName(identifierFieldId);
        Preconditions.checkNotNull(
            columnName, "Cannot find field with id %s in schema %s", identifierFieldId, schema);
        columns.add(columnName);
      }
      primaryKey = UniqueConstraint.primaryKey(UUID.randomUUID().toString(), columns);
    }

    if (primaryKey != null) {
      validatePrimaryKey(schemaColumns, primaryKey);
    }
    return new ResolvedSchema(schemaColumns, Collections.emptyList(), primaryKey);
  }

  private static void validatePrimaryKey(List<Column> columns, UniqueConstraint primaryKey) {
    Map<String, Column> columnsByNameLookup =
        columns.stream().collect(Collectors.toMap(Column::getName, Function.identity()));

    for (String columnName : primaryKey.getColumns()) {
      Column column = columnsByNameLookup.get(columnName);
      if (column == null) {
        throw new ValidationException(
            String.format(
                "Could not create a PRIMARY KEY '%s'. Column '%s' does not exist.",
                primaryKey.getName(), columnName));
      }

      if (!column.isPhysical()) {
        throw new ValidationException(
            String.format(
                "Could not create a PRIMARY KEY '%s'. Column '%s' is not a physical column.",
                primaryKey.getName(), columnName));
      }

      if (column.getDataType().getLogicalType().isNullable()) {
        throw new ValidationException(
            String.format(
                "Could not create a PRIMARY KEY '%s'. Column '%s' is nullable.",
                primaryKey.getName(), columnName));
      }
    }
  }
}
