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
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DateColumnStatistics;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.Reader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.Writer;
import org.apache.orc.storage.common.type.HiveDecimal;

import static org.apache.iceberg.types.Conversions.toByteBuffer;

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

  public static Metrics fromInputFile(InputFile file, Configuration config) {
    try (Reader orcReader = ORC.newFileReader(file, config)) {

      final Schema schema = ORCSchemaUtil.convert(orcReader.getSchema());

      ColumnStatistics[] colStats = orcReader.getStatistics();
      Map<Integer, Long> columSizes = Maps.newHashMapWithExpectedSize(colStats.length);
      Map<Integer, Long> valueCounts = Maps.newHashMapWithExpectedSize(colStats.length);
      for (int i = 0; i < colStats.length; i++) {
        columSizes.put(i, colStats[i].getBytesOnDisk());
        valueCounts.put(i, colStats[i].getNumberOfValues());
      }

      Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
      Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();

      for (Types.NestedField col : schema.columns()) {
        final int i = col.fieldId();
        columSizes.put(i, colStats[i].getBytesOnDisk());
        valueCounts.put(i, colStats[i].getNumberOfValues());

        Optional<ByteBuffer> orcMin = fromOrcMin(col, colStats[i]);
        orcMin.ifPresent(byteBuffer -> lowerBounds.put(i, byteBuffer));
        Optional<ByteBuffer> orcMax = fromOrcMax(col, colStats[i]);
        orcMax.ifPresent(byteBuffer -> upperBounds.put(i, byteBuffer));
      }

      return new Metrics(orcReader.getNumberOfRows(),
          columSizes,
          valueCounts,
          Collections.emptyMap(),
          lowerBounds,
          upperBounds);

    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to read footer of file: %s", file);
    }
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
      min = Conversions.toByteBuffer(column.type(), ((DoubleColumnStatistics) columnStats).getMinimum());
    } else if (columnStats instanceof StringColumnStatistics) {
      String minStats = ((StringColumnStatistics) columnStats).getMinimum();
      if (minStats != null) {
        min = Conversions.toByteBuffer(column.type(), minStats);
      }
    } else if (columnStats instanceof DecimalColumnStatistics) {
      HiveDecimal minStats = ((DecimalColumnStatistics) columnStats).getMinimum();
      if (minStats != null) {
        min = Conversions.toByteBuffer(column.type(), minStats.bigDecimalValue());
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
        min = Conversions.toByteBuffer(column.type(), minStats.getTime());
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
      max = Conversions.toByteBuffer(column.type(), ((DoubleColumnStatistics) columnStats).getMaximum());
    } else if (columnStats instanceof StringColumnStatistics) {
      String minStats = ((StringColumnStatistics) columnStats).getMaximum();
      if (minStats != null) {
        max = Conversions.toByteBuffer(column.type(), minStats);
      }
    } else if (columnStats instanceof DecimalColumnStatistics) {
      HiveDecimal maxStats = ((DecimalColumnStatistics) columnStats).getMaximum();
      if (maxStats != null) {
        max = Conversions.toByteBuffer(column.type(), maxStats.bigDecimalValue());
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
        max = Conversions.toByteBuffer(column.type(), maxStats.getTime());
      }
    }
    return Optional.ofNullable(max);
  }

  static Map<Integer, ?> fromBufferMap(Schema schema, Map<Integer, ByteBuffer> map) {
    Map<Integer, ?> values = Maps.newHashMap();
    for (Map.Entry<Integer, ByteBuffer> entry : map.entrySet()) {
      values.put(entry.getKey(),
          Conversions.fromByteBuffer(schema.findType(entry.getKey()), entry.getValue()));
    }
    return values;
  }

  static Map<Integer, ByteBuffer> toBufferMap(Schema schema, Map<Integer, Literal<?>> map) {
    Map<Integer, ByteBuffer> bufferMap = Maps.newHashMap();
    for (Map.Entry<Integer, Literal<?>> entry : map.entrySet()) {
      bufferMap.put(entry.getKey(),
          Conversions.toByteBuffer(schema.findType(entry.getKey()), entry.getValue().value()));
    }
    return bufferMap;
  }

  static Metrics fromWriter(Writer writer) {
    // TODO: implement rest of the methods for ORC metrics in
    // https://github.com/apache/incubator-iceberg/pull/199
    return new Metrics(writer.getNumberOfRows(),
        null,
        null,
        Collections.emptyMap(),
        null,
        null);
  }
}
