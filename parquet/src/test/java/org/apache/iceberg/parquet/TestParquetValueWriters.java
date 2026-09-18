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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

class TestParquetValueWriters {

  @Test
  void geometryWriterUsesParquetCRS() {
    Types.GeometryType geometryType = Types.GeometryType.of("EPSG:4326");
    Schema schema = new Schema(optional(2, "geom", geometryType));
    MessageType parquetSchema = ParquetSchemaUtil.convert(schema, "table");
    ParquetValueWriter<Record> writer = GenericParquetWriter.create(schema, parquetSchema);

    FieldMetrics<?> metrics = writer.metrics().findFirst().orElseThrow();
    assertThat(metrics.originalType()).isEqualTo(geometryType);
  }

  @Test
  void writerMetricsIncludeNullsFromOptionalParent() {
    Types.StructType statsType =
        Types.StructType.of(
            required(2, "float_value", Types.FloatType.get()),
            required(3, "double_value", Types.DoubleType.get()),
            required(4, "geometry_value", Types.GeometryType.crs84()),
            required(5, "geography_value", Types.GeographyType.crs84()));
    Schema schema = new Schema(optional(1, "stats", statsType));
    MessageType parquetSchema = ParquetSchemaUtil.convert(schema, "table");

    ColumnDescriptor floatDesc =
        parquetSchema.getColumnDescription(new String[] {"stats", "float_value"});
    ColumnDescriptor doubleDesc =
        parquetSchema.getColumnDescription(new String[] {"stats", "double_value"});
    ColumnDescriptor geometryDesc =
        parquetSchema.getColumnDescription(new String[] {"stats", "geometry_value"});
    ColumnDescriptor geographyDesc =
        parquetSchema.getColumnDescription(new String[] {"stats", "geography_value"});

    List<ParquetValueWriter<?>> fieldWriters =
        ImmutableList.of(
            ParquetValueWriters.floats(floatDesc),
            ParquetValueWriters.doubles(doubleDesc),
            ParquetValueWriters.geospatial(geometryDesc),
            ParquetValueWriters.geospatial(geographyDesc));
    ParquetValueWriter<StructLike> writer =
        ParquetValueWriters.option(
            parquetSchema.getType("stats"),
            parquetSchema.getMaxDefinitionLevel(new String[] {"stats"}),
            ParquetValueWriters.recordWriter(statsType, fieldWriters));

    ColumnWriteStore columnStore = mock(ColumnWriteStore.class);
    when(columnStore.getColumnWriter(floatDesc)).thenReturn(mock(ColumnWriter.class));
    when(columnStore.getColumnWriter(doubleDesc)).thenReturn(mock(ColumnWriter.class));
    when(columnStore.getColumnWriter(geometryDesc)).thenReturn(mock(ColumnWriter.class));
    when(columnStore.getColumnWriter(geographyDesc)).thenReturn(mock(ColumnWriter.class));
    writer.setColumnStore(columnStore);

    GenericRecord stats = GenericRecord.create(statsType);
    stats.setField("float_value", 12.0F);
    stats.setField("double_value", 34.0D);
    stats.setField("geometry_value", ByteBuffer.allocate(21));
    stats.setField("geography_value", ByteBuffer.allocate(42));
    writer.write(0, stats);
    writer.write(0, null);

    Map<Integer, FieldMetrics<?>> metricsById =
        writer.metrics().collect(Collectors.toMap(FieldMetrics::id, Function.identity()));
    assertThat(metricsById).containsOnlyKeys(2, 3, 4, 5);
    assertThat(metricsById.values())
        .allSatisfy(
            metrics -> {
              assertThat(metrics.valueCount()).isEqualTo(2);
              assertThat(metrics.nullValueCount()).isEqualTo(1);
            });
    assertThat(metricsById.get(4).avgValueSizeInBytes()).isEqualTo(21);
    assertThat(metricsById.get(5).avgValueSizeInBytes()).isEqualTo(42);
  }

  @Test
  void geospatialValueSizeMetricsExcludeNulls() {
    Schema schema = new Schema(optional(2, "geom", Types.GeometryType.crs84()));
    MessageType parquetSchema = ParquetSchemaUtil.convert(schema, "table");
    Type parquetType = parquetSchema.getType("geom");
    ColumnDescriptor desc = parquetSchema.getColumnDescription(new String[] {"geom"});
    ParquetValueWriter<ByteBuffer> writer =
        ParquetValueWriters.option(
            parquetType,
            parquetSchema.getMaxDefinitionLevel(new String[] {"geom"}),
            ParquetValueWriters.geospatial(desc));

    ColumnWriteStore columnStore = mock(ColumnWriteStore.class);
    when(columnStore.getColumnWriter(desc)).thenReturn(mock(ColumnWriter.class));
    writer.setColumnStore(columnStore);
    writer.write(0, ByteBuffer.allocate(21));
    writer.write(0, ByteBuffer.allocate(42));
    writer.write(0, null);

    FieldMetrics<?> metrics = writer.metrics().findFirst().orElseThrow();
    assertThat(metrics.valueCount()).isEqualTo(3);
    assertThat(metrics.nullValueCount()).isEqualTo(1);
    assertThat(metrics.avgValueSizeInBytes()).isEqualTo(31);
  }

  @Test
  void geometryValueSizeMetricsExcludeNulls() {
    Schema schema = new Schema(optional(2, "geom", Types.GeometryType.crs84()));
    MessageType parquetSchema = ParquetSchemaUtil.convert(schema, "table");
    Type parquetType = parquetSchema.getType("geom");
    ColumnDescriptor desc = parquetSchema.getColumnDescription(new String[] {"geom"});
    ParquetValueWriter<ByteBuffer> writer =
        ParquetValueWriters.option(
            parquetType,
            parquetSchema.getMaxDefinitionLevel(new String[] {"geom"}),
            ParquetValueWriters.geometry(desc, Types.GeometryType.crs84()));

    ColumnWriteStore columnStore = mock(ColumnWriteStore.class);
    when(columnStore.getColumnWriter(desc)).thenReturn(mock(ColumnWriter.class));
    writer.setColumnStore(columnStore);
    writer.write(0, ByteBuffer.allocate(21));
    writer.write(0, ByteBuffer.allocate(42));
    writer.write(0, null);

    // the geometry writer adds bounds but must keep the same average WKB size metric as the
    // counts-only geospatial writer: the average is over the two non-null values, (21 + 42) / 2
    FieldMetrics<?> metrics = writer.metrics().findFirst().orElseThrow();
    assertThat(metrics.valueCount()).isEqualTo(3);
    assertThat(metrics.nullValueCount()).isEqualTo(1);
    assertThat(metrics.avgValueSizeInBytes()).isEqualTo(31);
  }

  @Test
  void repeatedWriterConsumesReusedIterator() {
    RecordingWriter<Integer> elementWriter = new RecordingWriter<>();
    ReusingListWriter listWriter = new ReusingListWriter(2, 1, elementWriter);

    listWriter.write(0, ImmutableList.of(1, 2, 3));
    listWriter.write(0, ImmutableList.of(4, 5));

    List<Iterator<Integer>> handedOut = listWriter.handedOutIterators();
    assertThat(handedOut).hasSize(2);
    assertThat(handedOut.get(1)).isSameAs(handedOut.get(0));
    assertThat(elementWriter.values()).containsExactly(1, 2, 3, 4, 5);
    assertThat(elementWriter.repetitionLevels()).containsExactly(0, 1, 1, 0, 1);
  }

  @Test
  void repeatedKeyValueWriterConsumesReusedIteratorAndEntry() {
    RecordingWriter<String> keyWriter = new RecordingWriter<>();
    RecordingWriter<Integer> valueWriter = new RecordingWriter<>();
    ReusingMapWriter mapWriter = new ReusingMapWriter(2, 1, keyWriter, valueWriter);

    mapWriter.write(0, ImmutableMap.of("a", 1, "b", 2));
    mapWriter.write(0, ImmutableMap.of("c", 3));

    List<Iterator<Map.Entry<String, Integer>>> handedOut = mapWriter.handedOutIterators();
    assertThat(handedOut).hasSize(2);
    assertThat(handedOut.get(1)).isSameAs(handedOut.get(0));
    assertThat(keyWriter.values()).containsExactly("a", "b", "c");
    assertThat(valueWriter.values()).containsExactly(1, 2, 3);
    assertThat(keyWriter.repetitionLevels()).containsExactly(0, 1, 0);
    assertThat(valueWriter.repetitionLevels()).containsExactly(0, 1, 0);
  }

  private static class RecordingWriter<T> implements ParquetValueWriter<T> {
    private final List<T> values = Lists.newArrayList();
    private final List<Integer> repetitionLevels = Lists.newArrayList();

    @Override
    public void write(int repetitionLevel, T value) {
      repetitionLevels.add(repetitionLevel);
      values.add(value);
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return ImmutableList.of();
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {}

    private List<T> values() {
      return values;
    }

    private List<Integer> repetitionLevels() {
      return repetitionLevels;
    }
  }

  private static class ReusingListWriter
      extends ParquetValueWriters.RepeatedWriter<List<Integer>, Integer> {
    private final ElementIterator iterator = new ElementIterator();
    private final List<Iterator<Integer>> handedOutIterators = Lists.newArrayList();

    private ReusingListWriter(
        int definitionLevel, int repetitionLevel, ParquetValueWriter<Integer> writer) {
      super(definitionLevel, repetitionLevel, writer);
    }

    @Override
    protected Iterator<Integer> elements(List<Integer> value) {
      iterator.reset(value);
      handedOutIterators.add(iterator);
      return iterator;
    }

    private List<Iterator<Integer>> handedOutIterators() {
      return handedOutIterators;
    }

    private static class ElementIterator implements Iterator<Integer> {
      private List<Integer> values;
      private int index;

      private void reset(List<Integer> newValues) {
        this.values = newValues;
        this.index = 0;
      }

      @Override
      public boolean hasNext() {
        return index < values.size();
      }

      @Override
      public Integer next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        Integer value = values.get(index);
        index += 1;

        return value;
      }
    }
  }

  private static class ReusingMapWriter
      extends ParquetValueWriters.RepeatedKeyValueWriter<Map<String, Integer>, String, Integer> {
    private final EntryIterator iterator = new EntryIterator();
    private final List<Iterator<Map.Entry<String, Integer>>> handedOutIterators =
        Lists.newArrayList();

    private ReusingMapWriter(
        int definitionLevel,
        int repetitionLevel,
        ParquetValueWriter<String> keyWriter,
        ParquetValueWriter<Integer> valueWriter) {
      super(definitionLevel, repetitionLevel, keyWriter, valueWriter);
    }

    @Override
    protected Iterator<Map.Entry<String, Integer>> pairs(Map<String, Integer> value) {
      iterator.reset(value);
      handedOutIterators.add(iterator);
      return iterator;
    }

    private List<Iterator<Map.Entry<String, Integer>>> handedOutIterators() {
      return handedOutIterators;
    }

    private static class EntryIterator implements Iterator<Map.Entry<String, Integer>> {
      private final ParquetValueReaders.ReusableEntry<String, Integer> entry =
          new ParquetValueReaders.ReusableEntry<>();
      private List<Map.Entry<String, Integer>> entries;
      private int index;

      private void reset(Map<String, Integer> map) {
        this.entries = ImmutableList.copyOf(map.entrySet());
        this.index = 0;
      }

      @Override
      public boolean hasNext() {
        return index < entries.size();
      }

      @Override
      public Map.Entry<String, Integer> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        Map.Entry<String, Integer> source = entries.get(index);
        entry.set(source.getKey(), source.getValue());
        index += 1;

        return entry;
      }
    }
  }
}
