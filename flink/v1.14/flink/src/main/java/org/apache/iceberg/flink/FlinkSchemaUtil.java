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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.SchemaResolver;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Converter between Flink types and Iceberg type.
 * The conversion is not a 1:1 mapping that not allows back-and-forth conversion. So some information might get lost
 * during the back-and-forth conversion.
 * <p>
 * This inconsistent types:
 * <ul>
 *   <li>map Iceberg UUID type to Flink BinaryType(16)</li>
 *   <li>map Flink VarCharType(_) and CharType(_) to Iceberg String type</li>
 *   <li>map Flink VarBinaryType(_) to Iceberg Binary type</li>
 *   <li>map Flink TimeType(_) to Iceberg Time type (microseconds)</li>
 *   <li>map Flink TimestampType(_) to Iceberg Timestamp without zone type (microseconds)</li>
 *   <li>map Flink LocalZonedTimestampType(_) to Iceberg Timestamp with zone type (microseconds)</li>
 *   <li>map Flink MultiSetType to Iceberg Map type(element, int)</li>
 * </ul>
 * <p>
 */
public class FlinkSchemaUtil {

  public static final String FLINK_PREFIX = "flink.";

  public static final String COMPUTED_COLUMNS = "computed-columns.";
  public static final String COMPUTED_COLUMNS_PREFIX = FLINK_PREFIX + COMPUTED_COLUMNS;

  public static final String WATERMARK = "watermark.";
  public static final String WATERMARK_PREFIX = FLINK_PREFIX + WATERMARK;

  private FlinkSchemaUtil() {
  }

  /**
   * Convert the flink table schema to apache iceberg schema.
   */
  public static Schema convert(TableSchema schema) {
    LogicalType schemaType = schema.toPhysicalRowDataType().getLogicalType();
    Preconditions.checkArgument(schemaType instanceof RowType, "Schema logical type should be RowType.");

    RowType root = (RowType) schemaType;
    Type converted = root.accept(new FlinkTypeToType(root));

    Schema iSchema = new Schema(converted.asStructType().fields());
    return freshIdentifierFieldIds(iSchema, schema);
  }

  private static Schema freshIdentifierFieldIds(Schema iSchema, TableSchema schema) {
    // Locate the identifier field id list.
    Set<Integer> identifierFieldIds = Sets.newHashSet();
    if (schema.getPrimaryKey().isPresent()) {
      for (String column : schema.getPrimaryKey().get().getColumns()) {
        Types.NestedField field = iSchema.findField(column);
        Preconditions.checkNotNull(field,
            "Cannot find field ID for the primary key column %s in schema %s", column, iSchema);
        identifierFieldIds.add(field.fieldId());
      }
    }

    return new Schema(iSchema.schemaId(), iSchema.asStruct().fields(), identifierFieldIds);
  }

  /**
   * Convert a Flink {@link TableSchema} to a {@link Schema} based on the given schema.
   * <p>
   * This conversion does not assign new ids; it uses ids from the base schema.
   * <p>
   * Data types, field order, and nullability will match the Flink type. This conversion may return
   * a schema that is not compatible with base schema.
   *
   * @param baseSchema a Schema on which conversion is based
   * @param flinkSchema a Flink TableSchema
   * @return the equivalent Schema
   * @throws IllegalArgumentException if the type cannot be converted or there are missing ids
   */
  public static Schema convert(Schema baseSchema, TableSchema flinkSchema) {
    // convert to a type with fresh ids
    Types.StructType struct = convert(flinkSchema).asStruct();
    // reassign ids to match the base schema
    Schema schema = TypeUtil.reassignIds(new Schema(struct.fields()), baseSchema);
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
   * Convert a {@link RowType} to a {@link TableSchema}.
   *
   * @param rowType a RowType
   * @return Flink TableSchema
   */
  public static TableSchema toSchema(RowType rowType) {
    TableSchema.Builder builder = TableSchema.builder();
    for (RowType.RowField field : rowType.getFields()) {
      builder.field(field.getName(), TypeConversions.fromLogicalToDataType(field.getType()));
    }
    return builder.build();
  }

  /**
   * Convert a {@link Schema} to a {@link TableSchema}.
   *
   * @param schema iceberg schema to convert.
   * @return Flink TableSchema.
   */
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
        Preconditions.checkNotNull(columnName, "Cannot find field with id %s in schema %s", identifierFieldId, schema);

        columns.add(columnName);
      }
      builder.primaryKey(columns.toArray(new String[0]));
    }

    return builder.build();
  }


   /**
   * Convert a {@link Schema} to a {@link Schema}.
   *
   * @param schema iceberg schema to convert.
   * @return Flink Schema.
   */
  public static org.apache.flink.table.api.Schema toSchema(Schema schema, Map<String, String> properties) {

    org.apache.flink.table.api.Schema.Builder builder = org.apache.flink.table.api.Schema.newBuilder();

    // get watermark and computed columns
    Map<String, String> watermarkMap = Maps.newHashMap();
    Map<String, String> computedColumnsMap = Maps.newHashMap();
    properties.keySet().stream()
        .filter(k -> k.startsWith(FLINK_PREFIX) && properties.get(k) != null)
        .forEach(k -> {
          final String name = k.substring(k.lastIndexOf('.') + 1);
          String expr = properties.get(k);
          if (k.startsWith(WATERMARK_PREFIX)) {
            watermarkMap.put(name, expr);
          } else if (k.startsWith(COMPUTED_COLUMNS_PREFIX)) {
            computedColumnsMap.put(name, expr);
          }
        });

    // add physical columns.
    for (RowType.RowField field : convert(schema).getFields()) {
      builder.column(field.getName(), TypeConversions.fromLogicalToDataType(field.getType()));
    }

    // add computed columns.
    computedColumnsMap.forEach(builder::columnByExpression);

    // add watermarks.
    watermarkMap.forEach(builder::watermark);

    // add primary key.
    List<String> primaryKey = getPrimaryKeyFromSchema(schema);
    if (!primaryKey.isEmpty()) {
      builder.primaryKey(primaryKey.toArray(new String[0]));
    }

    return builder.build();
  }

  /**
   * Convert a {@link CatalogTable} to a {@link ResolvedSchema}.
   *
   * @param table flink unresolved schema to convert.
   * @return Flink ResolvedSchema.
   */
  public static ResolvedSchema convertToResolvedSchema(CatalogTable table) {
    StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
    StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
    CatalogManager catalogManager = ((TableEnvironmentImpl) streamTableEnvironment).getCatalogManager();
    SchemaResolver schemaResolver = catalogManager.getSchemaResolver();
    return table.getUnresolvedSchema().resolve(schemaResolver);
  }

  /**
   * Generate table properties for watermark and computed columns from flink resolved schema.
   *
   * @param schema flink resolved schema.
   * @return Table properties map.
   */
  public static Map<String, String> generateTablePropertiesFromResolvedSchema(ResolvedSchema schema) {
    Map<String, String> properties = Maps.newHashMap();

    // save watermark
    schema.getWatermarkSpecs().forEach(column -> {
      String name = column.getRowtimeAttribute();
      properties.put(
          FlinkSchemaUtil.WATERMARK_PREFIX + name,
          column.getWatermarkExpression().asSerializableString());
    });

    // save computed columns
    schema.getColumns().stream()
        .filter(column -> column instanceof Column.ComputedColumn)
        .forEach(tableColumn -> {
          Column.ComputedColumn column = (Column.ComputedColumn) tableColumn;
          String name = column.getName();
          properties.put(
              FlinkSchemaUtil.COMPUTED_COLUMNS_PREFIX + name,
              column.getExpression().asSerializableString());
        });

    return properties;
  }

  public static List<String> getPrimaryKeyFromSchema(Schema schema) {
    Set<Integer> identifierFieldIds = schema.identifierFieldIds();
    if (!identifierFieldIds.isEmpty()) {
      List<String> columns = Lists.newArrayListWithExpectedSize(identifierFieldIds.size());
      for (Integer identifierFieldId : identifierFieldIds) {
        String columnName = schema.findColumnName(identifierFieldId);
        Preconditions.checkNotNull(columnName,
                "Cannot find field with id %s in schema %s", identifierFieldId, schema);
        columns.add(columnName);
      }
      return columns;
    }
    return Lists.newArrayList();
  }
}
