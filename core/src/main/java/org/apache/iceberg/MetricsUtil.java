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
import java.util.concurrent.atomic.AtomicInteger;
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

public class MetricsUtil {

  private MetricsUtil() {}

  /**
   * Copies a metrics object without lower and upper bounds for given fields.
   *
   * @param excludedFieldIds field IDs for which the lower and upper bounds must be dropped
   * @return a new metrics object without lower and upper bounds for given fields
   */
  public static Metrics copyWithoutFieldBounds(Metrics metrics, Set<Integer> excludedFieldIds) {
    return new Metrics(
        metrics.recordCount(),
        metrics.columnSizes(),
        metrics.valueCounts(),
        metrics.nullValueCounts(),
        metrics.nanValueCounts(),
        copyWithoutKeys(metrics.lowerBounds(), excludedFieldIds),
        copyWithoutKeys(metrics.upperBounds(), excludedFieldIds));
  }

  private static <K, V> Map<K, V> copyWithoutKeys(Map<K, V> map, Set<K> keys) {
    if (map == null) {
      return null;
    }

    Map<K, V> filteredMap = Maps.newHashMap(map);

    for (K key : keys) {
      filteredMap.remove(key);
    }

    return filteredMap.isEmpty() ? null : filteredMap;
  }

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

  public static final List<ReadableMetricColDefinition> READABLE_METRIC_COLS =
      ImmutableList.of(
          new ReadableMetricColDefinition(
              "column_size",
              "Total size on disk",
              DataFile.COLUMN_SIZES,
              field -> Types.LongType.get(),
              (file, field) ->
                  file.columnSizes() == null ? null : file.columnSizes().get(field.fieldId())),
          new ReadableMetricColDefinition(
              "value_count",
              "Total count, including null and NaN",
              DataFile.VALUE_COUNTS,
              field -> Types.LongType.get(),
              (file, field) ->
                  file.valueCounts() == null ? null : file.valueCounts().get(field.fieldId())),
          new ReadableMetricColDefinition(
              "null_value_count",
              "Null value count",
              DataFile.NULL_VALUE_COUNTS,
              field -> Types.LongType.get(),
              (file, field) ->
                  file.nullValueCounts() == null
                      ? null
                      : file.nullValueCounts().get(field.fieldId())),
          new ReadableMetricColDefinition(
              "nan_value_count",
              "NaN value count",
              DataFile.NAN_VALUE_COUNTS,
              field -> Types.LongType.get(),
              (file, field) ->
                  file.nanValueCounts() == null
                      ? null
                      : file.nanValueCounts().get(field.fieldId())),
          new ReadableMetricColDefinition(
              "lower_bound",
              "Lower bound",
              DataFile.LOWER_BOUNDS,
              Types.NestedField::type,
              (file, field) ->
                  file.lowerBounds() == null
                      ? null
                      : Conversions.fromByteBuffer(
                          field.type(), file.lowerBounds().get(field.fieldId()))),
          new ReadableMetricColDefinition(
              "upper_bound",
              "Upper bound",
              DataFile.UPPER_BOUNDS,
              Types.NestedField::type,
              (file, field) ->
                  file.upperBounds() == null
                      ? null
                      : Conversions.fromByteBuffer(
                          field.type(), file.upperBounds().get(field.fieldId()))));

  public static final String READABLE_METRICS = "readable_metrics";

  /**
   * Fixed definition of a readable metric column, ie a mapping of a raw metric to a readable metric
   */
  public static class ReadableMetricColDefinition {
    private final String name;
    private final String doc;
    private final Types.NestedField originalCol;
    private final TypeFunction typeFunction;
    private final MetricFunction metricFunction;

    public interface TypeFunction {
      Type type(Types.NestedField originalCol);
    }

    public interface MetricFunction {
      Object metric(ContentFile<?> file, Types.NestedField originalCol);
    }

    /**
     * @param name column name
     * @param doc column doc
     * @param originalCol original (raw) metric column field on metadata table
     * @param typeFunction function that returns the readable metric column type from original field
     *     type
     * @param metricFunction function that returns readable metric from data file
     */
    ReadableMetricColDefinition(
        String name,
        String doc,
        Types.NestedField originalCol,
        TypeFunction typeFunction,
        MetricFunction metricFunction) {
      this.name = name;
      this.doc = doc;
      this.originalCol = originalCol;
      this.typeFunction = typeFunction;
      this.metricFunction = metricFunction;
    }

    Types.NestedField originalCol() {
      return originalCol;
    }

    Type colType(Types.NestedField field) {
      return typeFunction.type(field);
    }

    String name() {
      return name;
    }

    String doc() {
      return doc;
    }

    Object value(ContentFile<?> dataFile, Types.NestedField dataField) {
      return metricFunction.metric(dataFile, dataField);
    }
  }

  /** A struct of readable metric values for a primitive column */
  public static class ReadableColMetricsStruct implements StructLike {

    private final String columnName;
    private final Map<Integer, Integer> projectionMap;
    private final Object[] metrics;

    public ReadableColMetricsStruct(
        String columnName, Types.NestedField projection, Object... metrics) {
      this.columnName = columnName;
      this.projectionMap = readableMetricsProjection(projection);
      this.metrics = metrics;
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
      throw new UnsupportedOperationException("ReadableColMetricsStruct is read only");
    }

    private Object get(int pos) {
      int projectedPos = projectionMap.get(pos);
      return metrics[projectedPos];
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
      for (int fieldIndex = 0; fieldIndex < READABLE_METRIC_COLS.size(); fieldIndex++) {
        ReadableMetricColDefinition readableMetric = READABLE_METRIC_COLS.get(fieldIndex);

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
   * A struct, consisting of all {@link ReadableColMetricsStruct} for all primitive columns of the
   * table
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
   * @return schema of readable_metrics struct
   */
  public static Schema readableMetricsSchema(Schema dataTableSchema, Schema metadataTableSchema) {
    List<Types.NestedField> fields = Lists.newArrayList();
    Map<Integer, String> idToName = dataTableSchema.idToName();
    AtomicInteger nextId = new AtomicInteger(metadataTableSchema.highestFieldId());

    for (int id : idToName.keySet()) {
      Types.NestedField field = dataTableSchema.findField(id);

      if (field.type().isPrimitiveType()) {
        String colName = idToName.get(id);

        fields.add(
            Types.NestedField.of(
                nextId.incrementAndGet(),
                true,
                colName,
                Types.StructType.of(
                    READABLE_METRIC_COLS.stream()
                        .map(
                            m ->
                                optional(
                                    nextId.incrementAndGet(), m.name(), m.colType(field), m.doc()))
                        .collect(Collectors.toList())),
                String.format("Metrics for column %s", colName)));
      }
    }

    fields.sort(Comparator.comparing(Types.NestedField::name));
    return new Schema(
        optional(
            nextId.incrementAndGet(),
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
    List<ReadableColMetricsStruct> colMetrics = Lists.newArrayList();

    for (int id : idToName.keySet()) {
      String qualifiedName = idToName.get(id);
      Types.NestedField field = schema.findField(id);

      Object[] metrics =
          READABLE_METRIC_COLS.stream()
              .map(readableMetric -> readableMetric.value(file, field))
              .toArray();

      if (field.type().isPrimitiveType() // Iceberg stores metrics only for primitive types
          && projectedSchema.field(qualifiedName)
              != null) { // User has requested this column metric
        colMetrics.add(
            new ReadableColMetricsStruct(
                qualifiedName, projectedSchema.field(qualifiedName), metrics));
      }
    }

    colMetrics.sort(Comparator.comparing(ReadableColMetricsStruct::columnName));
    return new ReadableMetricsStruct(
        colMetrics.stream().map(m -> (StructLike) m).collect(Collectors.toList()));
  }

  /** Custom struct that returns a 'readable_metric' column at a specific position */
  static class StructWithReadableMetrics implements StructLike {
    private final StructLike struct;
    private final MetricsUtil.ReadableMetricsStruct readableMetrics;
    private final int projectionColumnCount;
    private final int metricsPosition;

    /**
     * Constructs a struct with readable metrics column
     *
     * @param struct struct on which to append 'readable_metrics' struct
     * @param structSize total number of struct columns, including 'readable_metrics' column
     * @param readableMetrics struct of 'readable_metrics'
     * @param metricsPosition position of 'readable_metrics' column
     */
    StructWithReadableMetrics(
        StructLike struct,
        int structSize,
        MetricsUtil.ReadableMetricsStruct readableMetrics,
        int metricsPosition) {
      this.struct = struct;
      this.readableMetrics = readableMetrics;
      this.projectionColumnCount = structSize;
      this.metricsPosition = metricsPosition;
    }

    @Override
    public int size() {
      return projectionColumnCount;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      if (pos < metricsPosition) {
        return struct.get(pos, javaClass);
      } else if (pos == metricsPosition) {
        return javaClass.cast(readableMetrics);
      } else {
        // columnCount = fileAsStruct column count + the readable metrics field.
        // When pos is greater than metricsPosition, the actual position of the field in
        // fileAsStruct should be subtracted by 1.
        return struct.get(pos - 1, javaClass);
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("StructWithReadableMetrics is read only");
    }
  }
}
