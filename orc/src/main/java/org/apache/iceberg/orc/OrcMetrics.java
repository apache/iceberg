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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
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
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.common.type.HiveDecimal;

public class OrcMetrics {

  private OrcMetrics() {
  }

  static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  public static Metrics fromInputFile(InputFile file) {
    final Configuration config = (file instanceof HadoopInputFile) ?
        ((HadoopInputFile) file).getConf() : new Configuration();
    return fromInputFile(file, config);
  }

  static Metrics fromInputFile(InputFile file, Configuration config) {
    try {
      final Reader orcReader = OrcFile.createReader(new Path(file.location()),
          OrcFile.readerOptions(config));
      return buildOrcMetrics(orcReader.getNumberOfRows(),
          orcReader.getSchema(), orcReader.getStatistics());
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to read footer of file: %s", file);
    }
  }


  private static Metrics buildOrcMetrics(final long numOfRows, final TypeDescription orcSchema,
                                         final ColumnStatistics[] colStats) {
    final Schema schema = ORCSchemaUtil.convert(orcSchema);
    Map<Integer, Long> columSizes = Maps.newHashMapWithExpectedSize(colStats.length);
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
        columSizes.put(fieldId, colStat.getBytesOnDisk());
        valueCounts.put(fieldId, colStat.getNumberOfValues());

        Optional<ByteBuffer> orcMin = (colStat.getNumberOfValues() > 0)
            ? fromOrcMin(icebergCol, colStat) : Optional.empty();
        orcMin.ifPresent(byteBuffer -> lowerBounds.put(icebergCol.fieldId(), byteBuffer));
        Optional<ByteBuffer> orcMax = (colStat.getNumberOfValues() > 0)
            ? fromOrcMax(icebergCol, colStat) : Optional.empty();
        orcMax.ifPresent(byteBuffer -> upperBounds.put(icebergCol.fieldId(), byteBuffer));
      }
    }

    return new Metrics(numOfRows,
        columSizes,
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
    ByteBuffer min = null;
    if (columnStats instanceof IntegerColumnStatistics) {
      IntegerColumnStatistics intColStats = (IntegerColumnStatistics) columnStats;
      if (column.type().typeId() == Type.TypeID.INTEGER) {
        min = Conversions.toByteBuffer(column.type(), (int) intColStats.getMinimum());
      } else {
        min = Conversions.toByteBuffer(column.type(), intColStats.getMinimum());
      }
    } else if (columnStats instanceof DoubleColumnStatistics) {
      double minVal = ((DoubleColumnStatistics) columnStats).getMinimum();
      if (column.type().typeId() == Type.TypeID.DOUBLE) {
        min = Conversions.toByteBuffer(column.type(), minVal);
      } else {
        min = Conversions.toByteBuffer(column.type(), (float) minVal);
      }
    } else if (columnStats instanceof StringColumnStatistics) {
      String minStats = ((StringColumnStatistics) columnStats).getMinimum();
      if (minStats != null) {
        min = Conversions.toByteBuffer(column.type(), minStats);
      }
    } else if (columnStats instanceof DecimalColumnStatistics) {
      HiveDecimal minStats = ((DecimalColumnStatistics) columnStats).getMinimum();
      if (minStats != null) {
        BigDecimal minValue = minStats.bigDecimalValue()
            .setScale(((Types.DecimalType) column.type()).scale());
        min = Conversions.toByteBuffer(column.type(), minValue);
      }
    } else if (columnStats instanceof DateColumnStatistics) {
      Date minStats = ((DateColumnStatistics) columnStats).getMinimum();
      if (minStats != null) {
        min = Conversions.toByteBuffer(column.type(), (int) ChronoUnit.DAYS.between(
            EPOCH_DAY, EPOCH.plus(minStats.getTime(), ChronoUnit.MILLIS).toLocalDate()));
      }
    } else if (columnStats instanceof TimestampColumnStatistics) {
      Timestamp minStats = ((TimestampColumnStatistics) columnStats).getMinimum();
      if (minStats != null) {
        min = Conversions.toByteBuffer(column.type(), toMicros(minStats));
      }
    } else if (columnStats instanceof BooleanColumnStatistics) {
      BooleanColumnStatistics booleanStats = (BooleanColumnStatistics) columnStats;
      if (booleanStats.getFalseCount() > 0) {
        min = Conversions.toByteBuffer(column.type(), false);
      } else if (booleanStats.getTrueCount() > 0) {
        min = Conversions.toByteBuffer(column.type(), true);
      }
    }
    return Optional.ofNullable(min);
  }

  private static Optional<ByteBuffer> fromOrcMax(Types.NestedField column,
                                                 ColumnStatistics columnStats) {
    ByteBuffer max = null;
    if (columnStats instanceof IntegerColumnStatistics) {
      IntegerColumnStatistics intColStats = (IntegerColumnStatistics) columnStats;
      if (column.type().typeId() == Type.TypeID.INTEGER) {
        max = Conversions.toByteBuffer(column.type(), (int) intColStats.getMaximum());
      } else {
        max = Conversions.toByteBuffer(column.type(), intColStats.getMaximum());
      }
    } else if (columnStats instanceof DoubleColumnStatistics) {
      double maxVal = ((DoubleColumnStatistics) columnStats).getMaximum();
      if (column.type().typeId() == Type.TypeID.DOUBLE) {
        max = Conversions.toByteBuffer(column.type(), maxVal);
      } else {
        max = Conversions.toByteBuffer(column.type(), (float) maxVal);
      }
    } else if (columnStats instanceof StringColumnStatistics) {
      String minStats = ((StringColumnStatistics) columnStats).getMaximum();
      if (minStats != null) {
        max = Conversions.toByteBuffer(column.type(), minStats);
      }
    } else if (columnStats instanceof DecimalColumnStatistics) {
      HiveDecimal maxStats = ((DecimalColumnStatistics) columnStats).getMaximum();
      if (maxStats != null) {
        BigDecimal maxValue = maxStats.bigDecimalValue()
            .setScale(((Types.DecimalType) column.type()).scale());
        max = Conversions.toByteBuffer(column.type(), maxValue);
      }

    } else if (columnStats instanceof DateColumnStatistics) {
      Date maxStats = ((DateColumnStatistics) columnStats).getMaximum();
      if (maxStats != null) {
        max = Conversions.toByteBuffer(column.type(), (int) ChronoUnit.DAYS.between(
            EPOCH_DAY, EPOCH.plus(maxStats.getTime(), ChronoUnit.MILLIS).toLocalDate()));
      }
    } else if (columnStats instanceof TimestampColumnStatistics) {
      Timestamp maxStats = ((TimestampColumnStatistics) columnStats).getMaximum();
      if (maxStats != null) {
        max = Conversions.toByteBuffer(column.type(), toMicros(maxStats));
      }
    } else if (columnStats instanceof BooleanColumnStatistics) {
      BooleanColumnStatistics booleanStats = (BooleanColumnStatistics) columnStats;
      if (booleanStats.getTrueCount() > 0) {
        max = Conversions.toByteBuffer(column.type(), true);
      } else if (booleanStats.getFalseCount() > 0) {
        max = Conversions.toByteBuffer(column.type(), false);
      }
    }
    return Optional.ofNullable(max);
  }

}
