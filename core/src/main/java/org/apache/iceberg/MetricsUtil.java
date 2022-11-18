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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsUtil {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsUtil.class);

  private MetricsUtil() {}

  /**
   * Construct mapping relationship between column id to NaN value counts from input metrics and
   * metrics config.
   */
  public static Map<Integer, Long> createNanValueCounts(
      Stream<FieldMetrics<?>> fieldMetrics, MetricsConfig metricsConfig, Schema inputSchema) {
    Preconditions.checkNotNull(metricsConfig, "metricsConfig is required");

    if (fieldMetrics == null || inputSchema == null) {
      return Maps.newHashMap();
    }

    return fieldMetrics
        .filter(
            metrics ->
                metricsMode(inputSchema, metricsConfig, metrics.id()) != MetricsModes.None.get())
        .collect(Collectors.toMap(FieldMetrics::id, FieldMetrics::nanValueCount));
  }

  /** Extract MetricsMode for the given field id from metrics config. */
  public static MetricsModes.MetricsMode metricsMode(
      Schema inputSchema, MetricsConfig metricsConfig, int fieldId) {
    Preconditions.checkNotNull(inputSchema, "inputSchema is required");
    Preconditions.checkNotNull(metricsConfig, "metricsConfig is required");

    String columnName = inputSchema.findColumnName(fieldId);
    return metricsConfig.columnMode(columnName);
  }

  public static final List<ReadableMetricCol> READABLE_COL_METRICS =
      ImmutableList.of(
          new ReadableMetricCol("column_size", f -> Types.LongType.get(), "Total size on disk"),
          new ReadableMetricCol(
              "value_count", f -> Types.LongType.get(), "Total count, including null and NaN"),
          new ReadableMetricCol("null_value_count", f -> Types.LongType.get(), "Null value count"),
          new ReadableMetricCol("nan_value_count", f -> Types.LongType.get(), "NaN value count"),
          new ReadableMetricCol("lower_bound", Types.NestedField::type, "Lower bound"),
          new ReadableMetricCol("upper_bound", Types.NestedField::type, "Upper bound"));

  public static final String READABLE_METRICS = "readable_metrics";

  public static class ReadableMetricCol {
    private final String name;
    private final Function<Types.NestedField, Type> typeFunction;
    private final String doc;

    ReadableMetricCol(String name, Function<Types.NestedField, Type> typeFunction, String doc) {
      this.name = name;
      this.typeFunction = typeFunction;
      this.doc = doc;
    }

    String name() {
      return name;
    }

    Type type(Types.NestedField field) {
      return typeFunction.apply(field);
    }

    String doc() {
      return doc;
    }
  }

  /**
   * Represents a struct of metrics for a primitive column
   *
   * @param <T> primitive column type
   */
  public static class ReadableColMetricsStruct<T> implements StructLike {

    private final String columnName;
    private final Long columnSize;
    private final Long valueCount;
    private final Long nullValueCount;
    private final Long nanValueCount;
    private final T lowerBound;
    private final T upperBound;
    private final Map<Integer, Integer> projectionMap;

    public ReadableColMetricsStruct(
        String columnName,
        Long columnSize,
        Long valueCount,
        Long nullValueCount,
        Long nanValueCount,
        T lowerBound,
        T upperBound,
        Types.NestedField projection) {
      this.columnName = columnName;
      this.columnSize = columnSize;
      this.valueCount = valueCount;
      this.nullValueCount = nullValueCount;
      this.nanValueCount = nanValueCount;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.projectionMap = readableMetricsProjection(projection);
    }

    @Override
    public int size() {
      return projectionMap.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      Object value = get(pos);
      return value == null ? null : javaClass.cast(value);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("ReadableMetricsStruct is read only");
    }

    private Object get(int pos) {
      int projectedPos = projectionMap.get(pos);
      switch (projectedPos) {
        case 0:
          return columnSize;
        case 1:
          return valueCount;
        case 2:
          return nullValueCount;
        case 3:
          return nanValueCount;
        case 4:
          return lowerBound;
        case 5:
          return upperBound;
        default:
          throw new IllegalArgumentException(
              String.format("Invalid projected pos %d", projectedPos));
      }
    }

    /** Returns map of projected position to actual position of this struct's fields */
    private Map<Integer, Integer> readableMetricsProjection(Types.NestedField projection) {
      Map<Integer, Integer> result = Maps.newHashMap();

      Set<String> projectedFields =
          Sets.newHashSet(
              projection.type().asStructType().fields().stream()
                  .map(Types.NestedField::name)
                  .collect(Collectors.toSet()));

      int projectedIndex = 0;
      for (int fieldIndex = 0; fieldIndex < READABLE_COL_METRICS.size(); fieldIndex++) {
        ReadableMetricCol readableMetric = READABLE_COL_METRICS.get(fieldIndex);

        if (projectedFields.contains(readableMetric.name())) {
          result.put(projectedIndex, fieldIndex);
          projectedIndex++;
        }
      }
      return result;
    }

    String columnName() {
      return columnName;
    }
  }

  /**
   * Represents a struct, consisting of all {@link ReadableColMetricsStruct} for all primitive
   * columns of the table
   */
  public static class ReadableMetricsStruct implements StructLike {

    private final List<StructLike> columnMetrics;

    public ReadableMetricsStruct(List<StructLike> columnMetrics) {
      this.columnMetrics = columnMetrics;
    }

    @Override
    public int size() {
      return columnMetrics.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(columnMetrics.get(pos));
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("ReadableMetricsStruct is read only");
    }
  }

  /**
   * Calculates a dynamic schema for readable_metrics to add to metadata tables. The type will be
   * the struct {@link ReadableColMetricsStruct}, composed of {@link ReadableMetricsStruct} for all
   * primitive columns in the data table
   *
   * @param dataTableSchema schema of data table
   * @param metadataTableSchema schema of existing metadata table (to ensure id uniqueness)
   * @param baseId first id to assign. This algorithm assigns field ids by incrementing this value
   *     and avoiding conflict with existing metadata table schema
   * @return schema of readable_metrics struct
   */
  public static Schema readableMetricsSchema(
      Schema dataTableSchema, Schema metadataTableSchema, int baseId) {
    List<Types.NestedField> fields = Lists.newArrayList();
    Set<Integer> usedIds = metadataTableSchema.idToName().keySet();

    class NextFieldId {
      private int next;

      NextFieldId() {
        this.next = baseId;
      }

      int next() {
        do {
          next++;
        } while (usedIds.contains(next));
        return next;
      }
    }
    NextFieldId next = new NextFieldId();

    Map<Integer, String> idToName = dataTableSchema.idToName();
    for (int id : idToName.keySet()) {
      Types.NestedField field = dataTableSchema.findField(id);

      if (field.type().isPrimitiveType()) {
        String colName = idToName.get(id);

        fields.add(
            Types.NestedField.of(
                next.next(),
                true,
                colName,
                Types.StructType.of(
                    READABLE_COL_METRICS.stream()
                        .map(m -> optional(next.next(), m.name(), m.type(field), m.doc()))
                        .collect(Collectors.toList())),
                String.format("Metrics for column %s", colName)));
      }
    }

    fields.sort(Comparator.comparing(Types.NestedField::name));
    return new Schema(
        optional(
            next.next(),
            "readable_metrics",
            Types.StructType.of(fields),
            "Column metrics in readable form"));
  }

  /**
   * Return a readable metrics struct row from file metadata
   *
   * @param schema schema of original data table
   * @param file content file with metrics
   * @param projectedSchema user requested projection
   * @return {@link ReadableMetricsStruct}
   */
  public static ReadableMetricsStruct readableMetricsStruct(
      Schema schema, ContentFile<?> file, Types.StructType projectedSchema) {
    Map<Integer, String> idToName = schema.idToName();
    List<ReadableColMetricsStruct<?>> colMetrics = Lists.newArrayList();

    for (int id : idToName.keySet()) {
      String qualifiedName = idToName.get(id);
      Types.NestedField field = schema.findField(id);

      if (field.type().isPrimitiveType()
          && // Iceberg stores metrics only for primitive types
          projectedSchema.field(qualifiedName) != null) { // User has requested this column metric
        colMetrics.add(
            new ReadableColMetricsStruct(
                qualifiedName,
                file.columnSizes() == null ? null : file.columnSizes().get(id),
                file.valueCounts() == null ? null : file.valueCounts().get(id),
                file.nullValueCounts() == null ? null : file.nullValueCounts().get(id),
                file.nanValueCounts() == null ? null : file.nanValueCounts().get(id),
                file.lowerBounds() == null
                    ? null
                    : Conversions.fromByteBuffer(field.type(), file.lowerBounds().get(id)),
                file.upperBounds() == null
                    ? null
                    : Conversions.fromByteBuffer(field.type(), file.upperBounds().get(id)),
                projectedSchema.field(qualifiedName)));
      }
    }

    colMetrics.sort(Comparator.comparing(ReadableColMetricsStruct::columnName));
    return new ReadableMetricsStruct(
        colMetrics.stream().map(m -> (StructLike) m).collect(Collectors.toList()));
  }
}
