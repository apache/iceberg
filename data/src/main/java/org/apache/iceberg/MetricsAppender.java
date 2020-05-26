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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class MetricsAppender<D> implements FileAppender<D> {

  private final FileAppender<D> appender;
  private final Schema schema;
  private Metrics metrics = null;
  private MetricsConfig metricsConfig = MetricsConfig.getDefault();
  private MetricsCollector metricsCollector;

  public MetricsAppender(FileAppender<D> appender, Schema schema) {
    this.appender = appender;
    this.schema = schema;
    this.metricsCollector = TypeUtil.visit(schema, new BuildMetricsCollector());
  }

  @Override
  public void add(D datum) {
    appender.add(datum);
    metricsCollector.add(datum);
  }

  @Override
  public Metrics metrics() {
    Preconditions.checkState(metrics != null, "Cannot produce metrics until closed");
    return metrics;
  }

  @Override
  public long length() {
    return appender.length();
  }

  @Override
  public void close() throws IOException {
    appender.close();
    metrics = metricsCollector.getMetrics();
  }

  private static class MetricsCollectorMap<K, V> extends MetricsCollectorBase<Map<K, V>> {
    private final MetricsCollector collectorKey;
    private final MetricsCollector collectorValue;

    MetricsCollectorMap(MetricsCollector<K> collectorKey, MetricsCollector<V> collectorValue) {
      this.collectorKey = collectorKey;
      this.collectorValue = collectorValue;
    }

    @Override
    public void add(Map<K, V> map) {
      if (map != null) {
        for (K k : map.keySet()) {
          collectorKey.add(k);
          collectorValue.add(map.get(k));
        }
      } else {
        collectorKey.add(null);
        collectorValue.add(null);
      }
    }

    @Override
    public Metrics getMetrics() {
      Map<Integer, Long> valueCounts = new HashMap<>();
      Map<Integer, Long> nullValueCounts = new HashMap<>();
      Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
      Map<Integer, ByteBuffer> upperBounds = new HashMap<>();

      Metrics metricsKey = collectorKey.getMetrics();
      Metrics metricsValue = collectorValue.getMetrics();

      if (metricsKey.valueCounts() != null) {
        valueCounts.putAll(metricsKey.valueCounts());
      }

      if (metricsValue.valueCounts() != null) {
        valueCounts.putAll(metricsValue.valueCounts());
      }

      if (metricsKey.nullValueCounts() != null) {
        nullValueCounts.putAll(metricsKey.nullValueCounts());
      }

      if (metricsValue.nullValueCounts() != null) {
        nullValueCounts.putAll(metricsValue.nullValueCounts());
      }

      if (metricsKey.lowerBounds() != null) {
        lowerBounds.putAll(metricsKey.lowerBounds());
      }

      if (metricsValue.lowerBounds() != null) {
        lowerBounds.putAll(metricsValue.lowerBounds());
      }

      if (metricsKey.upperBounds() != null) {
        upperBounds.putAll(metricsKey.upperBounds());
      }

      if (metricsValue.upperBounds() != null) {
        upperBounds.putAll(metricsValue.upperBounds());
      }

      return new Metrics(0L, null, valueCounts, nullValueCounts, lowerBounds, upperBounds);
    }
  }

  private static class MetricsCollectorList<L> extends MetricsCollectorBase<List<L>> {

    private final MetricsCollector collector;

    MetricsCollectorList(MetricsCollector<L> collector) {
      this.collector = collector;
    }

    @Override
    public void add(List<L> list) {
      if (list != null) {
        for (L e : list) {
          collector.add(e);
        }
      } else {
        collector.add(null);
      }
    }

    @Override
    public Metrics getMetrics() {
      return collector.getMetrics();
    }
  }

  private static class MetricsCollectorRecord extends MetricsCollectorBase<Record> {
    private List<MetricsCollector> collectors;
    private Long count = 0L;

    MetricsCollectorRecord(List<MetricsCollector> collectors) {
      this.collectors = collectors;
    }

    @Override
    public void add(Record record) {
      count++;
      for (int i = 0; i < collectors.size(); i++) {
        if (collectors.get(i) != null) { // TODO: necessary
          collectors.get(i).add(record == null ? null : record.get(i));
        }
      }
    }

    @Override
    public Metrics getMetrics() {
      Map<Integer, Long> valueCounts = new HashMap<>();
      Map<Integer, Long> nullValueCounts = new HashMap<>();
      Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
      Map<Integer, ByteBuffer> upperBounds = new HashMap<>();

      for (MetricsCollector metricsCollector : collectors) {
        if (metricsCollector != null) { // TODO: necessary??
          Metrics metrics = metricsCollector.getMetrics();

          valueCounts.putAll(metrics.valueCounts());
          nullValueCounts.putAll(metrics.nullValueCounts());
          if (metrics.lowerBounds() != null) {
            lowerBounds.putAll(metrics.lowerBounds());
          }

          if (metrics.upperBounds() != null) {
            upperBounds.putAll(metrics.upperBounds());
          }
        }
      }

      return new Metrics(count, null, valueCounts, nullValueCounts, lowerBounds, upperBounds);
    }
  }

  abstract static class MetricsCollectorPrimitive<D> implements MetricsCollector<D> {
    private final Comparator<D> comparator;
    private D max;
    private D min;
    private Long values = 0L;
    private Long nulls = 0L;
    private final Integer id;

    MetricsCollectorPrimitive(Integer id, Comparator<D> comparator) {
      this.id = id;
      this.comparator = comparator;
    }

    @Override
    public void add(D datum) {
      values++;
      if (datum == null) {
        nulls++;
      } else {
        if (max == null || comparator.compare(datum, max) > 0) {
          max = datum;
        }
        if (min == null || comparator.compare(datum, min) < 0) {
          min = datum;
        }
      }
    }

    abstract ByteBuffer encode(D datum);

    @Override
    public Metrics getMetrics() {
      return new Metrics(values,
          null,
          ImmutableMap.of(id, values),
          ImmutableMap.of(id, nulls),
          min == null ? null : ImmutableMap.of(id, encode(min)),
          max == null ? null : ImmutableMap.of(id, encode(max)));
    }
  }

  private static class MetricsCollectorInt extends MetricsCollectorPrimitive<Integer> {
    MetricsCollectorInt(Integer id, Comparator<Integer> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(Integer datum) {
      return Conversions.toByteBuffer(Types.IntegerType.get(), datum);
    }
  }

  private static class MetricsCollectorLong extends MetricsCollectorPrimitive<Long> {
    MetricsCollectorLong(Integer id, Comparator<Long> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(Long datum) {
      return Conversions.toByteBuffer(Types.LongType.get(), datum);
    }
  }

  private static class MetricsCollectorBoolean extends MetricsCollectorPrimitive<Boolean> {
    MetricsCollectorBoolean(Integer id, Comparator<Boolean> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(Boolean datum) {
      return Conversions.toByteBuffer(Types.BooleanType.get(), datum);
    }
  }

  private static class MetricsCollectorString extends MetricsCollectorPrimitive<String> {
    MetricsCollectorString(Integer id, Comparator<String> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(String datum) {
      return Conversions.toByteBuffer(Types.StringType.get(), datum);
    }
  }

  private static class MetricsCollectorFloat extends MetricsCollectorPrimitive<Float> {
    MetricsCollectorFloat(Integer id, Comparator<Float> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(Float datum) {
      return Conversions.toByteBuffer(Types.FloatType.get(), datum);
    }
  }

  private static class MetricsCollectorDouble extends MetricsCollectorPrimitive<Double> {
    MetricsCollectorDouble(Integer id, Comparator<Double> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(Double datum) {
      return Conversions.toByteBuffer(Types.DoubleType.get(), datum);
    }
  }

  private static class MetricsCollectorUUID extends MetricsCollectorPrimitive<UUID> {
    MetricsCollectorUUID(Integer id, Comparator<UUID> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(UUID datum) {
      return Conversions.toByteBuffer(Types.UUIDType.get(), datum);
    }
  }

  private static class MetricsCollectorBytes extends MetricsCollectorPrimitive<ByteBuffer> {
    MetricsCollectorBytes(Integer id, Comparator<ByteBuffer> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(ByteBuffer datum) {
      return Conversions.toByteBuffer(Types.BinaryType.get(), datum);
    }
  }

  private static class MetricsCollectorDate extends MetricsCollectorPrimitive<LocalDate> {
    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

    MetricsCollectorDate(Integer id, Comparator<LocalDate> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(LocalDate datum) {
      return Conversions.toByteBuffer(Types.DateType.get(), (int) ChronoUnit.DAYS.between(EPOCH_DAY, datum));
    }
  }

  private static class MetricsCollectorTime extends MetricsCollectorPrimitive<LocalTime> {
    MetricsCollectorTime(Integer id, Comparator<LocalTime> comparator) {
      super(id, comparator);
    }

    @Override
    ByteBuffer encode(LocalTime datum) {
      return Conversions.toByteBuffer(Types.TimeType.get(), datum.toNanoOfDay() / 1000);
    }
  }

  private static class MetricsCollectorDateTime extends MetricsCollectorPrimitive<LocalDateTime> {
    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private final boolean withoutTimeZone;

    MetricsCollectorDateTime(Integer id, Comparator<LocalDateTime> comparator, boolean withoutTimeZone) {
      super(id, comparator);
      this.withoutTimeZone = withoutTimeZone;
    }

    @Override
    ByteBuffer encode(LocalDateTime datum) {
      // TODO check
      return Conversions.toByteBuffer(
          withoutTimeZone ? Types.TimestampType.withoutZone() : Types.TimestampType.withZone(),
          ChronoUnit.MICROS.between(EPOCH, datum.atOffset(ZoneOffset.UTC)));
    }
  }

  private static class MetricsCollectorFixed extends MetricsCollectorPrimitive<byte[]> {
    private final int length;

    MetricsCollectorFixed(Integer id, Comparator<byte[]> comparator, int length) {
      super(id, comparator);
      this.length = length;
    }

    @Override
    ByteBuffer encode(byte[] datum) {
      return Conversions.toByteBuffer(Types.FixedType.ofLength(length), datum);
    }
  }

  private static class MetricsCollectorDecimal extends MetricsCollectorPrimitive<BigDecimal> {
    private final int precision;
    private final int scale;

    MetricsCollectorDecimal(Integer id, Comparator<BigDecimal> comparator, int precision, int scale) {
      super(id, comparator);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    ByteBuffer encode(BigDecimal datum) {
      return Conversions.toByteBuffer(Types.DecimalType.of(precision, scale), datum);
    }
  }


  private static class BuildMetricsCollector
      extends TypeUtil.SchemaVisitor<MetricsCollector> {

    private Types.NestedField currentField = null;

    private BuildMetricsCollector() {
    }

    @Override
    public void beforeField(Types.NestedField field) {
      currentField = field;
    }

    @Override
    public void afterField(Types.NestedField field) {
      currentField = null;
    }

    @Override
    public MetricsCollector schema(
        Schema schema, MetricsCollector metricsCollector) {
      return metricsCollector;
    }

    @Override
    public MetricsCollector struct(Types.StructType struct, List<MetricsCollector> fieldResults) {
      return new MetricsCollectorRecord(fieldResults);
    }

    @Override
    public MetricsCollector field(Types.NestedField field, MetricsCollector fieldResult) {
      return fieldResult;
    }

    @Override
    public MetricsCollector list(Types.ListType list, MetricsCollector elementResult) {
      return new MetricsCollectorList(elementResult);
    }

    @Override
    public MetricsCollector map(Types.MapType map, MetricsCollector keyResult, MetricsCollector valueResult) {
      return new MetricsCollectorMap(keyResult, valueResult);
    }

    @Override
    public MetricsCollector primitive(Type.PrimitiveType primitive) {
      // TODO check if comparators require previous conversion (i.e. LocalDateTime to long)?
      switch (primitive.typeId()) {
        case BOOLEAN:
          return new MetricsCollectorBoolean(currentField.fieldId(), Comparators.forType(primitive));
        case INTEGER:
          return new MetricsCollectorInt(currentField.fieldId(), Comparators.forType(primitive));
        case LONG:
          return new MetricsCollectorLong(currentField.fieldId(), Comparators.forType(primitive));
        case FLOAT:
          return new MetricsCollectorFloat(currentField.fieldId(), Comparators.forType(primitive));
        case DOUBLE:
          return new MetricsCollectorDouble(currentField.fieldId(), Comparators.forType(primitive));
        case DATE:
          return new MetricsCollectorDate(currentField.fieldId(), Comparators.forType(primitive));
        case TIME:
          return new MetricsCollectorTime(currentField.fieldId(), Comparators.forType(primitive));
        case TIMESTAMP:
          Types.TimestampType timestamp = (Types.TimestampType) primitive;
          return new MetricsCollectorDateTime(currentField.fieldId(),
              Comparators.forType(primitive), timestamp.shouldAdjustToUTC());
        case STRING:
          return new MetricsCollectorString(currentField.fieldId(), Comparators.forType(primitive));
        case UUID:
          return new MetricsCollectorUUID(currentField.fieldId(), Comparators.forType(primitive));
        case FIXED:
          Types.FixedType fixed = (Types.FixedType) primitive;
          return new MetricsCollectorFixed(currentField.fieldId(), Comparators.forType(primitive), fixed.length());
        case BINARY:
          return new MetricsCollectorBytes(currentField.fieldId(), Comparators.forType(primitive));
        case DECIMAL:
          Types.DecimalType decimal = (Types.DecimalType) primitive;
          return new MetricsCollectorDecimal(currentField.fieldId(),
              Comparators.forType(primitive), decimal.precision(), decimal.scale());
        default:
          return null; /* TODO */
      }
    }
  }
}
