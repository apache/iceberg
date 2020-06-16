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
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
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

  public static Metrics fromInputFile(InputFile file) {
    final Configuration config = (file instanceof HadoopInputFile) ?
        ((HadoopInputFile) file).getConf() : new Configuration();
    return fromInputFile(file, config);
  }

  static Metrics fromInputFile(InputFile file, Configuration config) {
    try (Reader orcReader = ORC.newFileReader(file, config)) {
      return buildOrcMetrics(orcReader.getNumberOfRows(), orcReader.getSchema(), orcReader.getStatistics());
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to open file: %s", file.location());
    }
  }

  static Metrics fromWriter(Writer writer) {
    try {
      return buildOrcMetrics(writer.getNumberOfRows(), writer.getSchema(), writer.getStatistics());
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to get statistics from writer");
    }
  }

  private static Metrics buildOrcMetrics(final long numOfRows, final TypeDescription orcSchema,
                                         final ColumnStatistics[] colStats) {
    final Schema schema = ORCSchemaUtil.convert(orcSchema);
    final Set<TypeDescription> columnsInContainers = findColumnsInContainers(schema, orcSchema);
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

        columnSizes.put(fieldId, colStat.getBytesOnDisk());

        if (!columnsInContainers.contains(orcCol)) {
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

          Optional<ByteBuffer> orcMin = (colStat.getNumberOfValues() > 0) ?
              fromOrcMin(icebergCol, colStat) : Optional.empty();
          orcMin.ifPresent(byteBuffer -> lowerBounds.put(icebergCol.fieldId(), byteBuffer));
          Optional<ByteBuffer> orcMax = (colStat.getNumberOfValues() > 0) ?
              fromOrcMax(icebergCol, colStat) : Optional.empty();
          orcMax.ifPresent(byteBuffer -> upperBounds.put(icebergCol.fieldId(), byteBuffer));
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
          .map(minStats -> DateTimeUtil.daysFromDate(
              DateTimeUtil.EPOCH.plus(minStats.getTime(), ChronoUnit.MILLIS).toLocalDate()))
          .orElse(null);
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
          .map(maxStats -> DateTimeUtil.daysFromDate(
              DateTimeUtil.EPOCH.plus(maxStats.getTime(), ChronoUnit.MILLIS).toLocalDate()))
          .orElse(null);
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
    return Optional.ofNullable(Conversions.toByteBuffer(column.type(), max));
  }

  private static Set<TypeDescription> findColumnsInContainers(Schema schema,
                                                              TypeDescription orcSchema) {
    ColumnsInContainersVisitor visitor = new ColumnsInContainersVisitor();
    OrcSchemaWithTypeVisitor.visit(schema, orcSchema, visitor);
    return visitor.getColumnsInContainers();
  }

  private static class ColumnsInContainersVisitor extends OrcSchemaWithTypeVisitor<TypeDescription> {

    private final Set<TypeDescription> columnsInContainers;

    private ColumnsInContainersVisitor() {
      columnsInContainers = Sets.newHashSet();
    }

    public Set<TypeDescription> getColumnsInContainers() {
      return columnsInContainers;
    }

    private Set<TypeDescription> flatten(TypeDescription rootType) {
      if (rootType == null) {
        return ImmutableSet.of();
      }

      final Set<TypeDescription> flatTypes = Sets.newHashSetWithExpectedSize(rootType.getMaximumId());
      final Queue<TypeDescription> queue = Queues.newLinkedBlockingQueue();
      queue.add(rootType);
      while (!queue.isEmpty()) {
        TypeDescription type = queue.remove();
        flatTypes.add(type);
        queue.addAll(Optional.ofNullable(type.getChildren()).orElse(ImmutableList.of()));
      }
      return flatTypes;
    }

    @Override
    public TypeDescription record(Types.StructType iStruct, TypeDescription record,
                                  List<String> names, List<TypeDescription> fields) {
      return record;
    }

    @Override
    public TypeDescription list(Types.ListType iList, TypeDescription array, TypeDescription element) {
      columnsInContainers.addAll(flatten(element));
      return array;
    }

    @Override
    public TypeDescription map(Types.MapType iMap, TypeDescription map,
                    TypeDescription key, TypeDescription value) {
      columnsInContainers.addAll(flatten(key));
      columnsInContainers.addAll(flatten(value));
      return map;
    }

    @Override
    public TypeDescription primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
      return primitive;
    }
  }
}
