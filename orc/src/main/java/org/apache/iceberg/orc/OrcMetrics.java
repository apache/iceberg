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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
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

public class OrcMetrics {

  private OrcMetrics() {
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  public static Metrics fromInputFile(InputFile file) {
    final Configuration config = (file instanceof HadoopInputFile) ?
        ((HadoopInputFile) file).getConf() : new Configuration();
    return fromInputFile(file, config);
  }

  static Metrics fromInputFile(InputFile file, Configuration config) {
    try (Reader orcReader = ORC.newFileReader(file, config)) {
      return buildOrcMetrics(orcReader.getNumberOfRows(),
          orcReader.getSchema(), orcReader.getStatistics());
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to open file: %s", file.location());
    }
  }

  private static Metrics buildOrcMetrics(final long numOfRows, final TypeDescription orcSchema,
                                         final ColumnStatistics[] colStats) {
    final Schema schema = ORCSchemaUtil.convert(orcSchema);
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

        if (colStat.hasNull()) {
          nullCounts.put(fieldId, numOfRows - colStat.getNumberOfValues());
        } else {
          nullCounts.put(fieldId, 0L);
        }

        columnSizes.put(fieldId, colStat.getBytesOnDisk());
        if (nullCounts.containsKey(fieldId)) {
          valueCounts.put(fieldId, colStat.getNumberOfValues() + nullCounts.get(fieldId));
        }

        Optional<ByteBuffer> orcMin = (colStat.getNumberOfValues() > 0) ?
            fromOrcMin(icebergCol, colStat) : Optional.empty();
        orcMin.ifPresent(byteBuffer -> lowerBounds.put(icebergCol.fieldId(), byteBuffer));
        Optional<ByteBuffer> orcMax = (colStat.getNumberOfValues() > 0) ?
            fromOrcMax(icebergCol, colStat) : Optional.empty();
        orcMax.ifPresent(byteBuffer -> upperBounds.put(icebergCol.fieldId(), byteBuffer));
      }
    }

    return new Metrics(numOfRows,
        columnSizes,
        valueCounts,
        nullCounts,
        lowerBounds,
        upperBounds);
  }

  static Metrics fromWriter(Writer writer) {
    try {
      return buildOrcMetrics(writer.getNumberOfRows(),
          writer.getSchema(), writer.getStatistics());
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to get statistics from writer");
    }
  }

  private static long toMicros(Timestamp ts) {
    return ts.getTime() * 1000;
  }

  private static Optional<ByteBuffer> fromOrcMin(Types.NestedField column,
                                                 ColumnStatistics columnStats) {
    Object min = null;
    if (columnStats instanceof IntegerColumnStatistics) {
      min = ((IntegerColumnStatistics) columnStats).getMinimum();
      if (column.type().typeId() == Type.TypeID.INTEGER) {
        min = Math.toIntExact((long) min);
      }
    } else if (columnStats instanceof DoubleColumnStatistics) {
      min = ((DoubleColumnStatistics) columnStats).getMinimum();
      if (column.type().typeId() == Type.TypeID.FLOAT) {
        min = ((Double) min).floatValue();
      }
    } else if (columnStats instanceof StringColumnStatistics) {
      min = ((StringColumnStatistics) columnStats).getMinimum();
    } else if (columnStats instanceof DecimalColumnStatistics) {
      min = Optional
          .ofNullable(((DecimalColumnStatistics) columnStats).getMinimum())
          .map(minStats -> minStats.bigDecimalValue()
              .setScale(((Types.DecimalType) column.type()).scale()))
          .orElse(null);
    } else if (columnStats instanceof DateColumnStatistics) {
      min = Optional.ofNullable(((DateColumnStatistics) columnStats).getMinimum())
          .map(minStats ->
              Math.toIntExact(ChronoUnit.DAYS.between(EPOCH_DAY,
                  EPOCH.plus(minStats.getTime(), ChronoUnit.MILLIS).toLocalDate())))
          .orElse(null);
    } else if (columnStats instanceof TimestampColumnStatistics) {
      TimestampColumnStatistics tColStats = (TimestampColumnStatistics) columnStats;
      Timestamp minValue = tColStats.getMinimumUTC();
      min = Optional.ofNullable(minValue)
          .map(OrcMetrics::toMicros)
          .orElse(null);
    } else if (columnStats instanceof BooleanColumnStatistics) {
      BooleanColumnStatistics booleanStats = (BooleanColumnStatistics) columnStats;
      min = booleanStats.getFalseCount() <= 0;
    }
    return Optional.ofNullable(Conversions.toByteBuffer(column.type(), min));
  }

  private static Optional<ByteBuffer> fromOrcMax(Types.NestedField column,
                                                 ColumnStatistics columnStats) {
    Object max = null;
    if (columnStats instanceof IntegerColumnStatistics) {
      max = ((IntegerColumnStatistics) columnStats).getMaximum();
      if (column.type().typeId() == Type.TypeID.INTEGER) {
        max = Math.toIntExact((long) max);
      }
    } else if (columnStats instanceof DoubleColumnStatistics) {
      max = ((DoubleColumnStatistics) columnStats).getMaximum();
      if (column.type().typeId() == Type.TypeID.FLOAT) {
        max = ((Double) max).floatValue();
      }
    } else if (columnStats instanceof StringColumnStatistics) {
      max = ((StringColumnStatistics) columnStats).getMaximum();
    } else if (columnStats instanceof DecimalColumnStatistics) {
      max = Optional
          .ofNullable(((DecimalColumnStatistics) columnStats).getMaximum())
          .map(maxStats -> maxStats.bigDecimalValue()
              .setScale(((Types.DecimalType) column.type()).scale()))
          .orElse(null);
    } else if (columnStats instanceof DateColumnStatistics) {
      max = Optional.ofNullable(((DateColumnStatistics) columnStats).getMaximum())
          .map(maxStats ->
              (int) ChronoUnit.DAYS.between(EPOCH_DAY,
                  EPOCH.plus(maxStats.getTime(), ChronoUnit.MILLIS).toLocalDate()))
          .orElse(null);
    } else if (columnStats instanceof TimestampColumnStatistics) {
      TimestampColumnStatistics tColStats = (TimestampColumnStatistics) columnStats;
      Timestamp maxValue = tColStats.getMaximumUTC();
      max = Optional.ofNullable(maxValue)
          .map(OrcMetrics::toMicros)
          .orElse(null);
    } else if (columnStats instanceof BooleanColumnStatistics) {
      BooleanColumnStatistics booleanStats = (BooleanColumnStatistics) columnStats;
      max = booleanStats.getTrueCount() > 0;
    }
    return Optional.ofNullable(Conversions.toByteBuffer(column.type(), max));
  }

}
