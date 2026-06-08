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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class TestVortexMetrics {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "salary", Types.LongType.get()),
          optional(4, "rating", Types.DoubleType.get()));

  @Test
  public void testFieldMetricsFromWriter() {
    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(SCHEMA);

    addRecord(writer, 1L, "Alice", 1000L, 4.5);
    addRecord(writer, 2L, "Bob", null, Double.NaN);
    addRecord(writer, 3L, "Carol", 3000L, 3.2);

    Map<Integer, FieldMetrics<?>> metricsMap = collectMetricsById(writer);

    // id: required, all non-null
    FieldMetrics<?> idMetrics = metricsMap.get(1);
    assertThat(idMetrics.valueCount()).isEqualTo(3);
    assertThat(idMetrics.nullValueCount()).isEqualTo(0);
    assertThat(idMetrics.lowerBound()).isEqualTo(1L);
    assertThat(idMetrics.upperBound()).isEqualTo(3L);

    // name: optional, all present
    FieldMetrics<?> nameMetrics = metricsMap.get(2);
    assertThat(nameMetrics.valueCount()).isEqualTo(3);
    assertThat(nameMetrics.nullValueCount()).isEqualTo(0);
    assertThat(nameMetrics.lowerBound()).isEqualTo("Alice");
    assertThat(nameMetrics.upperBound()).isEqualTo("Carol");

    // salary: optional, one null
    FieldMetrics<?> salaryMetrics = metricsMap.get(3);
    assertThat(salaryMetrics.valueCount()).isEqualTo(3);
    assertThat(salaryMetrics.nullValueCount()).isEqualTo(1);
    assertThat(salaryMetrics.lowerBound()).isEqualTo(1000L);
    assertThat(salaryMetrics.upperBound()).isEqualTo(3000L);

    // rating: double with NaN
    FieldMetrics<?> ratingMetrics = metricsMap.get(4);
    assertThat(ratingMetrics.valueCount()).isEqualTo(3);
    assertThat(ratingMetrics.nullValueCount()).isEqualTo(0);
    assertThat(ratingMetrics.nanValueCount()).isEqualTo(1);
    assertThat(ratingMetrics.lowerBound()).isEqualTo(3.2);
    assertThat(ratingMetrics.upperBound()).isEqualTo(4.5);
  }

  @Test
  public void testBuildMetricsFullMode() {
    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(SCHEMA);

    addRecord(writer, 1L, "Alice", 1000L, 4.5);
    addRecord(writer, 2L, "Bob", null, 3.2);

    Metrics metrics =
        VortexMetrics.buildMetrics(2L, SCHEMA, MetricsConfig.getDefault(), writer.metrics());

    assertThat(metrics.recordCount()).isEqualTo(2L);

    // value counts
    assertThat(metrics.valueCounts()).containsEntry(1, 2L);
    assertThat(metrics.valueCounts()).containsEntry(2, 2L);
    assertThat(metrics.valueCounts()).containsEntry(3, 2L);

    // null counts
    assertThat(metrics.nullValueCounts()).containsEntry(1, 0L);
    assertThat(metrics.nullValueCounts()).containsEntry(3, 1L);

    // bounds are ByteBuffers
    assertThat(
            (Object) Conversions.fromByteBuffer(Types.LongType.get(), metrics.lowerBounds().get(1)))
        .isEqualTo(1L);
    assertThat(
            (Object) Conversions.fromByteBuffer(Types.LongType.get(), metrics.upperBounds().get(1)))
        .isEqualTo(2L);

    assertThat(
            Conversions.fromByteBuffer(Types.StringType.get(), metrics.lowerBounds().get(2))
                .toString())
        .isEqualTo("Alice");
    assertThat(
            Conversions.fromByteBuffer(Types.StringType.get(), metrics.upperBounds().get(2))
                .toString())
        .isEqualTo("Bob");
  }

  @Test
  public void testMetricsCountsMode() {
    MetricsConfig countsConfig =
        MetricsConfig.fromProperties(
            ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts"));

    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(SCHEMA);
    addRecord(writer, 1L, "Alice", 1000L, 4.5);

    Metrics metrics = VortexMetrics.buildMetrics(1L, SCHEMA, countsConfig, writer.metrics());

    assertThat(metrics.recordCount()).isEqualTo(1L);
    assertThat(metrics.valueCounts()).containsEntry(1, 1L);
    assertThat(metrics.nullValueCounts()).containsEntry(1, 0L);

    // no bounds in counts mode
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }

  @Test
  public void testMetricsNoneMode() {
    MetricsConfig noneConfig =
        MetricsConfig.fromProperties(
            ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none"));

    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(SCHEMA);
    addRecord(writer, 1L, "Alice", 1000L, 4.5);

    Metrics metrics = VortexMetrics.buildMetrics(1L, SCHEMA, noneConfig, writer.metrics());

    assertThat(metrics.recordCount()).isEqualTo(1L);
    assertThat(metrics.valueCounts()).isEmpty();
    assertThat(metrics.nullValueCounts()).isEmpty();
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }

  @Test
  public void testMetricsTruncateMode() {
    MetricsConfig truncateConfig =
        MetricsConfig.fromProperties(
            ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(2)"));

    Schema stringSchema = new Schema(required(1, "name", Types.StringType.get()));

    FieldMetrics<String> fieldMetrics = new FieldMetrics<>(1, 1, 0, "abcdef", "abcdef");

    Metrics metrics =
        VortexMetrics.buildMetrics(1L, stringSchema, truncateConfig, Stream.of(fieldMetrics));

    // lower bound should be truncated to "ab"
    ByteBuffer lowerBound = metrics.lowerBounds().get(1);
    assertThat(Conversions.fromByteBuffer(Types.StringType.get(), lowerBound).toString())
        .isEqualTo("ab");

    // upper bound should be truncated to "ac" (truncateStringMax increments last char)
    ByteBuffer upperBound = metrics.upperBounds().get(1);
    assertThat(Conversions.fromByteBuffer(Types.StringType.get(), upperBound).toString())
        .isEqualTo("ac");
  }

  @Test
  public void testAllNullColumn() {
    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(SCHEMA);
    addRecord(writer, 1L, null, null, null);
    addRecord(writer, 2L, null, null, null);

    Map<Integer, FieldMetrics<?>> metricsMap = collectMetricsById(writer);

    FieldMetrics<?> nameMetrics = metricsMap.get(2);
    assertThat(nameMetrics.valueCount()).isEqualTo(2);
    assertThat(nameMetrics.nullValueCount()).isEqualTo(2);
    assertThat(nameMetrics.hasBounds()).isFalse();
  }

  @Test
  public void testAllNaNDoubleColumn() {
    Schema doubleSchema = new Schema(required(1, "val", Types.DoubleType.get()));
    VortexValueWriter<Record> writer = GenericVortexWriter.buildWriter(doubleSchema);

    GenericRecord record = GenericRecord.create(doubleSchema);
    record.set(0, Double.NaN);
    // write directly through tracker, not through VectorSchemaRoot
    // Just test FieldMetrics directly
    FieldMetrics<Double> fieldMetrics = new FieldMetrics<>(1, 2, 0, 2L, null, null);

    Metrics metrics =
        VortexMetrics.buildMetrics(
            2L, doubleSchema, MetricsConfig.getDefault(), Stream.of(fieldMetrics));

    assertThat(metrics.nanValueCounts()).containsEntry(1, 2L);
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }

  private static void addRecord(
      VortexValueWriter<Record> writer, Long id, String name, Long salary, Double rating) {
    // We can't write through VectorSchemaRoot in unit tests (needs Arrow allocation),
    // but GenericVortexWriter tracks metrics before writing to vectors.
    // Use reflection-free approach: call the writer's metrics tracking directly.
    // Actually, the GenericVortexWriter.write() will throw on null root vectors,
    // so let's use the tracker-level testing approach via metrics() directly.
    // For integration testing we'd need the full Arrow stack.
    //
    // Instead, let's test the tracker directly through GenericVortexWriter internals.
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.set(0, id);
    record.set(1, name);
    record.set(2, salary);
    record.set(3, rating);

    // We need to create an actual VectorSchemaRoot for the write call.
    // Use a shared helper that creates an Arrow root.
    try (org.apache.arrow.memory.BufferAllocator allocator =
            new org.apache.arrow.memory.RootAllocator();
        org.apache.arrow.vector.VectorSchemaRoot root =
            org.apache.arrow.vector.VectorSchemaRoot.create(
                VortexSchemas.toArrowSchema(SCHEMA), allocator)) {
      root.allocateNew();
      writer.write(record, root, 0);
    }
  }

  private static Map<Integer, FieldMetrics<?>> collectMetricsById(
      VortexValueWriter<Record> writer) {
    Map<Integer, FieldMetrics<?>> map = new java.util.HashMap<>();
    writer.metrics().forEach(m -> map.put(m.id(), m));
    return map;
  }
}
