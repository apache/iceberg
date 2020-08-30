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

package org.apache.iceberg.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UnicodeUtil;
import org.apache.orc.BooleanColumnStatistics;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DateColumnStatistics;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.Reader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.ColumnStatisticsImpl;

public class OrcMetrics {

  private enum Bound {
    LOWER, UPPER
  }

  private OrcMetrics() {
  }

  public static Metrics fromInputFile(InputFile file) {
    return fromInputFile(file, MetricsConfig.getDefault());
  }

  public static Metrics fromInputFile(InputFile file, MetricsConfig metricsConfig) {
    final Configuration config = (file instanceof HadoopInputFile) ?
        ((HadoopInputFile) file).getConf() : new Configuration();
    return fromInputFile(file, config, metricsConfig);
  }

  static Metrics fromInputFile(InputFile file, Configuration config, MetricsConfig metricsConfig) {
    try (Reader orcReader = ORC.newFileReader(file, config)) {
      return buildOrcMetrics(orcReader.getNumberOfRows(), orcReader.getSchema(), orcReader.getStatistics(),
          metricsConfig);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to open file: %s", file.location());
    }
  }

  static Metrics fromWriter(Writer writer, MetricsConfig metricsConfig) {
    try {
      return buildOrcMetrics(writer.getNumberOfRows(), writer.getSchema(), writer.getStatistics(), metricsConfig);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to get statistics from writer");
    }
  }

  private static Metrics buildOrcMetrics(long numOfRows, TypeDescription orcSchema,
                                         ColumnStatistics[] colStats, MetricsConfig metricsConfig) {
    final Schema schema = ORCSchemaUtil.convert(orcSchema);
    final Set<Integer> statsColumns = statsColumns(orcSchema);
    final MetricsConfig effectiveMetricsConfig = Optional.ofNullable(metricsConfig)
        .orElseGet(MetricsConfig::getDefault);
    Map<Integer, Long> columnSizes = Maps.newHashMapWithExpectedSize(colStats.length);
    Map<Integer, Long> valueCounts = Maps.newHashMapWithExpectedSize(colStats.length);
    Map<Integer, Long> nullCounts = Maps.newHashMapWithExpectedSize(colStats.length);
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();

    for (int i = 0; i < colStats.length; i++) {
      final ColumnStatistics colStat = colStats[i];
      final TypeDescription orcCol = orcSchema.findSubtype(i);
      final Optional<Types.NestedField> icebergColOpt = ORCSchemaUtil.icebergID(orcCol)
          .map(schema::findField);

      if (icebergColOpt.isPresent()) {
        final Types.NestedField icebergCol = icebergColOpt.get();
        final int fieldId = icebergCol.fieldId();
        final MetricsMode metricsMode = effectiveMetricsConfig.columnMode(icebergCol.name());

        columnSizes.put(fieldId, colStat.getBytesOnDisk());

        if (metricsMode == MetricsModes.None.get()) {
          continue;
        }

        if (statsColumns.contains(fieldId)) {
          // Since ORC does not track null values nor repeated ones, the value count for columns in
          // containers (maps, list) may be larger than what it actually is, however these are not
          // used in experssions right now. For such cases, we use the value number of values
          // directly stored in ORC.
          if (colStat.hasNull()) {
            nullCounts.put(fieldId, numOfRows - colStat.getNumberOfValues());
          } else {
            nullCounts.put(fieldId, 0L);
          }
          valueCounts.put(fieldId, colStat.getNumberOfValues() + nullCounts.get(fieldId));

          if (metricsMode != MetricsModes.Counts.get()) {
            Optional<ByteBuffer> orcMin = (colStat.getNumberOfValues() > 0) ?
                fromOrcMin(icebergCol.type(), colStat, metricsMode) : Optional.empty();
            orcMin.ifPresent(byteBuffer -> lowerBounds.put(icebergCol.fieldId(), byteBuffer));
            Optional<ByteBuffer> orcMax = (colStat.getNumberOfValues() > 0) ?
                fromOrcMax(icebergCol.type(), colStat, metricsMode) : Optional.empty();
            orcMax.ifPresent(byteBuffer -> upperBounds.put(icebergCol.fieldId(), byteBuffer));
          }
        }
      }
    }

    return new Metrics(numOfRows,
        columnSizes,
        valueCounts,
        nullCounts,
        lowerBounds,
        upperBounds);
  }

  private static Optional<ByteBuffer> fromOrcMin(Type type, ColumnStatistics columnStats,
                                                 MetricsMode metricsMode) {
    Object min = null;
    if (columnStats instanceof IntegerColumnStatistics) {
      min = ((IntegerColumnStatistics) columnStats).getMinimum();
      if (type.typeId() == Type.TypeID.INTEGER) {
        min = Math.toIntExact((long) min);
      }
    } else if (columnStats instanceof DoubleColumnStatistics) {
      min = ((DoubleColumnStatistics) columnStats).getMinimum();
      if (type.typeId() == Type.TypeID.FLOAT) {
        min = ((Double) min).floatValue();
      }
    } else if (columnStats instanceof StringColumnStatistics) {
      min = ((StringColumnStatistics) columnStats).getMinimum();
    } else if (columnStats instanceof DecimalColumnStatistics) {
      min = Optional
          .ofNullable(((DecimalColumnStatistics) columnStats).getMinimum())
          .map(minStats -> minStats.bigDecimalValue()
              .setScale(((Types.DecimalType) type).scale()))
          .orElse(null);
    } else if (columnStats instanceof DateColumnStatistics) {
      min = Optional.ofNullable(minDayFromEpoch((DateColumnStatistics) columnStats)).orElse(null);
    } else if (columnStats instanceof TimestampColumnStatistics) {
      TimestampColumnStatistics tColStats = (TimestampColumnStatistics) columnStats;
      Timestamp minValue = tColStats.getMinimumUTC();
      min = Optional.ofNullable(minValue)
          .map(v -> TimeUnit.MILLISECONDS.toMicros(v.getTime()))
          .orElse(null);
    } else if (columnStats instanceof BooleanColumnStatistics) {
      BooleanColumnStatistics booleanStats = (BooleanColumnStatistics) columnStats;
      min = booleanStats.getFalseCount() <= 0;
    }

    return Optional.ofNullable(Conversions.toByteBuffer(type, truncateIfNeeded(Bound.LOWER, type, min, metricsMode)));
  }

  private static Optional<ByteBuffer> fromOrcMax(Type type, ColumnStatistics columnStats,
                                                 MetricsMode metricsMode) {
    Object max = null;
    if (columnStats instanceof IntegerColumnStatistics) {
      max = ((IntegerColumnStatistics) columnStats).getMaximum();
      if (type.typeId() == Type.TypeID.INTEGER) {
        max = Math.toIntExact((long) max);
      }
    } else if (columnStats instanceof DoubleColumnStatistics) {
      max = ((DoubleColumnStatistics) columnStats).getMaximum();
      if (type.typeId() == Type.TypeID.FLOAT) {
        max = ((Double) max).floatValue();
      }
    } else if (columnStats instanceof StringColumnStatistics) {
      max = ((StringColumnStatistics) columnStats).getMaximum();
    } else if (columnStats instanceof DecimalColumnStatistics) {
      max = Optional
          .ofNullable(((DecimalColumnStatistics) columnStats).getMaximum())
          .map(maxStats -> maxStats.bigDecimalValue()
              .setScale(((Types.DecimalType) type).scale()))
          .orElse(null);
    } else if (columnStats instanceof DateColumnStatistics) {
      max = Optional.ofNullable(maxDayFromEpoch((DateColumnStatistics) columnStats)).orElse(null);
    } else if (columnStats instanceof TimestampColumnStatistics) {
      TimestampColumnStatistics tColStats = (TimestampColumnStatistics) columnStats;
      Timestamp maxValue = tColStats.getMaximumUTC();
      max = Optional.ofNullable(maxValue)
          .map(v -> TimeUnit.MILLISECONDS.toMicros(v.getTime()))
          .map(v -> v + 1_000) // Add 1 millisecond to handle precision issue due to ORC-611
          .orElse(null);
    } else if (columnStats instanceof BooleanColumnStatistics) {
      BooleanColumnStatistics booleanStats = (BooleanColumnStatistics) columnStats;
      max = booleanStats.getTrueCount() > 0;
    }
    return Optional.ofNullable(Conversions.toByteBuffer(type, truncateIfNeeded(Bound.UPPER, type, max, metricsMode)));
  }

  private static Object truncateIfNeeded(Bound bound, Type type, Object value, MetricsMode metricsMode) {
    // Out of the two types which could be truncated, string or binary, ORC only supports string bounds.
    // Therefore, truncation will be applied if needed only on string type.
    if (!(metricsMode instanceof MetricsModes.Truncate) || type.typeId() != Type.TypeID.STRING || value == null) {
      return value;
    }

    CharSequence charSequence = (CharSequence) value;
    MetricsModes.Truncate truncateMode = (MetricsModes.Truncate) metricsMode;
    int truncateLength = truncateMode.length();

    switch (bound) {
      case UPPER:
        return Optional.ofNullable(UnicodeUtil.truncateStringMax(Literal.of(charSequence), truncateLength))
            .map(Literal::value).orElse(charSequence);
      case LOWER:
        return UnicodeUtil.truncateStringMin(Literal.of(charSequence), truncateLength).value();
      default:
        throw new RuntimeException("No other bound is defined.");
    }
  }

  private static Set<Integer> statsColumns(TypeDescription schema) {
    return OrcSchemaVisitor.visit(schema, new StatsColumnsVisitor());
  }

  private static class StatsColumnsVisitor extends OrcSchemaVisitor<Set<Integer>> {
    @Override
    public Set<Integer> record(TypeDescription record, List<String> names, List<Set<Integer>> fields) {
      ImmutableSet.Builder<Integer> result = ImmutableSet.builder();
      fields.stream().filter(Objects::nonNull).forEach(result::addAll);
      record.getChildren().stream().map(ORCSchemaUtil::fieldId).forEach(result::add);
      return result.build();
    }
  }

  private static final Class<?> DATE_STATS_IMPL = Stream.of(ColumnStatisticsImpl.class.getDeclaredClasses())
      .filter(statsClass -> "DateStatisticsImpl".equals(statsClass.getSimpleName()))
      .findFirst()
      .orElse(null);

  private static final DynFields.UnboundField<Integer> DATE_MINIMUM = DynFields.builder()
      .hiddenImpl(DATE_STATS_IMPL, "minimum")
      .defaultAlwaysNull() // if the minimum field isn't found, don't add a value
      .build();

  private static final DynFields.UnboundField<Integer> DATE_MAXIMUM = DynFields.builder()
      .hiddenImpl(DATE_STATS_IMPL, "maximum")
      .defaultAlwaysNull() // if the minimum field isn't found, don't add a value
      .build();

  private static Integer minDayFromEpoch(DateColumnStatistics stats) {
    return DATE_MINIMUM.get(stats);
  }

  private static Integer maxDayFromEpoch(DateColumnStatistics stats) {
    return DATE_MAXIMUM.get(stats);
  }
}
