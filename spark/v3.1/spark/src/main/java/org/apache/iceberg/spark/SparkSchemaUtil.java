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
package org.apache.iceberg.spark;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.math.LongMath;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

/** Helper methods for working with Spark/Hive metadata. */
public class SparkSchemaUtil {
  private SparkSchemaUtil() {}

  /**
   * Returns a {@link Schema} for the given table with fresh field ids.
   *
   * <p>This creates a Schema for an existing table by looking up the table's schema with Spark and
   * converting that schema. Spark/Hive partition columns are included in the schema.
   *
   * @param spark a Spark session
   * @param name a table name and (optional) database
   * @return a Schema for the table, if found
   */
  public static Schema schemaForTable(SparkSession spark, String name) {
    StructType sparkType = spark.table(name).schema();
    Type converted = SparkTypeVisitor.visit(sparkType, new SparkTypeToType(sparkType));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  /**
   * Returns a {@link PartitionSpec} for the given table.
   *
   * <p>This creates a partition spec for an existing table by looking up the table's schema and
   * creating a spec with identity partitions for each partition column.
   *
   * @param spark a Spark session
   * @param name a table name and (optional) database
   * @return a PartitionSpec for the table
   * @throws AnalysisException if thrown by the Spark catalog
   */
  public static PartitionSpec specForTable(SparkSession spark, String name)
      throws AnalysisException {
    List<String> parts = Lists.newArrayList(Splitter.on('.').limit(2).split(name));
    String db = parts.size() == 1 ? "default" : parts.get(0);
    String table = parts.get(parts.size() == 1 ? 0 : 1);

    PartitionSpec spec =
        identitySpec(
            schemaForTable(spark, name), spark.catalog().listColumns(db, table).collectAsList());
    return spec == null ? PartitionSpec.unpartitioned() : spec;
  }

  /**
   * Convert a {@link Schema} to a {@link DataType Spark type}.
   *
   * @param schema a Schema
   * @return the equivalent Spark type
   * @throws IllegalArgumentException if the type cannot be converted to Spark
   */
  public static StructType convert(Schema schema) {
    return (StructType) TypeUtil.visit(schema, new TypeToSparkType());
  }

  /**
   * Convert a {@link Type} to a {@link DataType Spark type}.
   *
   * @param type a Type
   * @return the equivalent Spark type
   * @throws IllegalArgumentException if the type cannot be converted to Spark
   */
  public static DataType convert(Type type) {
    return TypeUtil.visit(type, new TypeToSparkType());
  }

  /**
   * Convert a Spark {@link StructType struct} to a {@link Schema} with new field ids.
   *
   * <p>This conversion assigns fresh ids.
   *
   * <p>Some data types are represented as the same Spark type. These are converted to a default
   * type.
   *
   * <p>To convert using a reference schema for field ids and ambiguous types, use {@link
   * #convert(Schema, StructType)}.
   *
   * @param sparkType a Spark StructType
   * @return the equivalent Schema
   * @throws IllegalArgumentException if the type cannot be converted
   */
  public static Schema convert(StructType sparkType) {
    return convert(sparkType, false);
  }

  /**
   * Convert a Spark {@link StructType struct} to a {@link Schema} with new field ids.
   *
   * <p>This conversion assigns fresh ids.
   *
   * <p>Some data types are represented as the same Spark type. These are converted to a default
   * type.
   *
   * <p>To convert using a reference schema for field ids and ambiguous types, use {@link
   * #convert(Schema, StructType)}.
   *
   * @param sparkType a Spark StructType
   * @param useTimestampWithoutZone boolean flag indicates that timestamp should be stored without
   *     timezone
   * @return the equivalent Schema
   * @throws IllegalArgumentException if the type cannot be converted
   */
  public static Schema convert(StructType sparkType, boolean useTimestampWithoutZone) {
    Type converted = SparkTypeVisitor.visit(sparkType, new SparkTypeToType(sparkType));
    Schema schema = new Schema(converted.asNestedType().asStructType().fields());
    if (useTimestampWithoutZone) {
      schema = SparkFixupTimestampType.fixup(schema);
    }
    return schema;
  }

  /**
   * Convert a Spark {@link DataType struct} to a {@link Type} with new field ids.
   *
   * <p>This conversion assigns fresh ids.
   *
   * <p>Some data types are represented as the same Spark type. These are converted to a default
   * type.
   *
   * <p>To convert using a reference schema for field ids and ambiguous types, use {@link
   * #convert(Schema, StructType)}.
   *
   * @param sparkType a Spark DataType
   * @return the equivalent Type
   * @throws IllegalArgumentException if the type cannot be converted
   */
  public static Type convert(DataType sparkType) {
    return SparkTypeVisitor.visit(sparkType, new SparkTypeToType());
  }

  /**
   * Convert a Spark {@link StructType struct} to a {@link Schema} based on the given schema.
   *
   * <p>This conversion does not assign new ids; it uses ids from the base schema.
   *
   * <p>Data types, field order, and nullability will match the spark type. This conversion may
   * return a schema that is not compatible with base schema.
   *
   * @param baseSchema a Schema on which conversion is based
   * @param sparkType a Spark StructType
   * @return the equivalent Schema
   * @throws IllegalArgumentException if the type cannot be converted or there are missing ids
   */
  public static Schema convert(Schema baseSchema, StructType sparkType) {
    // convert to a type with fresh ids
    Types.StructType struct =
        SparkTypeVisitor.visit(sparkType, new SparkTypeToType(sparkType)).asStructType();
    // reassign ids to match the base schema
    Schema schema = TypeUtil.reassignIds(new Schema(struct.fields()), baseSchema);
    // fix types that can't be represented in Spark (UUID and Fixed)
    return SparkFixupTypes.fixup(schema, baseSchema);
  }

  /**
   * Prune columns from a {@link Schema} using a {@link StructType Spark type} projection.
   *
   * <p>This requires that the Spark type is a projection of the Schema. Nullability and types must
   * match.
   *
   * @param schema a Schema
   * @param requestedType a projection of the Spark representation of the Schema
   * @return a Schema corresponding to the Spark projection
   * @throws IllegalArgumentException if the Spark type does not match the Schema
   */
  public static Schema prune(Schema schema, StructType requestedType) {
    return new Schema(
        TypeUtil.visit(schema, new PruneColumnsWithoutReordering(requestedType, ImmutableSet.of()))
            .asNestedType()
            .asStructType()
            .fields());
  }

  /**
   * Prune columns from a {@link Schema} using a {@link StructType Spark type} projection.
   *
   * <p>This requires that the Spark type is a projection of the Schema. Nullability and types must
   * match.
   *
   * <p>The filters list of {@link Expression} is used to ensure that columns referenced by filters
   * are projected.
   *
   * @param schema a Schema
   * @param requestedType a projection of the Spark representation of the Schema
   * @param filters a list of filters
   * @return a Schema corresponding to the Spark projection
   * @throws IllegalArgumentException if the Spark type does not match the Schema
   */
  public static Schema prune(Schema schema, StructType requestedType, List<Expression> filters) {
    Set<Integer> filterRefs = Binder.boundReferences(schema.asStruct(), filters, true);
    return new Schema(
        TypeUtil.visit(schema, new PruneColumnsWithoutReordering(requestedType, filterRefs))
            .asNestedType()
            .asStructType()
            .fields());
  }

  /**
   * Prune columns from a {@link Schema} using a {@link StructType Spark type} projection.
   *
   * <p>This requires that the Spark type is a projection of the Schema. Nullability and types must
   * match.
   *
   * <p>The filters list of {@link Expression} is used to ensure that columns referenced by filters
   * are projected.
   *
   * @param schema a Schema
   * @param requestedType a projection of the Spark representation of the Schema
   * @param filter a filters
   * @return a Schema corresponding to the Spark projection
   * @throws IllegalArgumentException if the Spark type does not match the Schema
   */
  public static Schema prune(
      Schema schema, StructType requestedType, Expression filter, boolean caseSensitive) {
    Set<Integer> filterRefs =
        Binder.boundReferences(schema.asStruct(), Collections.singletonList(filter), caseSensitive);

    return new Schema(
        TypeUtil.visit(schema, new PruneColumnsWithoutReordering(requestedType, filterRefs))
            .asNestedType()
            .asStructType()
            .fields());
  }

  private static PartitionSpec identitySpec(Schema schema, Collection<Column> columns) {
    List<String> names = Lists.newArrayList();
    for (Column column : columns) {
      if (column.isPartition()) {
        names.add(column.name());
      }
    }

    return identitySpec(schema, names);
  }

  private static PartitionSpec identitySpec(Schema schema, List<String> partitionNames) {
    if (partitionNames == null || partitionNames.isEmpty()) {
      return null;
    }

    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (String partitionName : partitionNames) {
      builder.identity(partitionName);
    }

    return builder.build();
  }

  /**
   * Estimate approximate table size based on Spark schema and total records.
   *
   * @param tableSchema Spark schema
   * @param totalRecords total records in the table
   * @return approximate size based on table schema
   */
  public static long estimateSize(StructType tableSchema, long totalRecords) {
    if (totalRecords == Long.MAX_VALUE) {
      return totalRecords;
    }

    long result;
    try {
      result = LongMath.checkedMultiply(tableSchema.defaultSize(), totalRecords);
    } catch (ArithmeticException e) {
      result = Long.MAX_VALUE;
    }
    return result;
  }

  public static void validateMetadataColumnReferences(Schema tableSchema, Schema readSchema) {
    List<String> conflictingColumnNames =
        readSchema.columns().stream()
            .map(Types.NestedField::name)
            .filter(
                name ->
                    MetadataColumns.isMetadataColumn(name) && tableSchema.findField(name) != null)
            .collect(Collectors.toList());

    ValidationException.check(
        conflictingColumnNames.isEmpty(),
        "Table column names conflict with names reserved for Iceberg metadata columns: %s.\n"
            + "Please, use ALTER TABLE statements to rename the conflicting table columns.",
        conflictingColumnNames);
  }
}
