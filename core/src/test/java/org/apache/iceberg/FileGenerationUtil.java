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

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.MetricsModes.Counts;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.MetricsModes.None;
import org.apache.iceberg.MetricsModes.Truncate;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.RandomUtil;

public class FileGenerationUtil {

  private FileGenerationUtil() {}

  public static DataFile generateDataFile(Table table, StructLike partition) {
    return generateDataFile(table, partition, ImmutableMap.of(), ImmutableMap.of());
  }

  public static DataFile generateDataFile(
      Table table,
      StructLike partition,
      Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds) {
    Schema schema = table.schema();
    PartitionSpec spec = table.spec();
    LocationProvider locations = table.locationProvider();
    String path = locations.newDataLocation(spec, partition, generateFileName());
    long fileSize = generateFileSize();
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    Metrics metrics = generateRandomMetrics(schema, metricsConfig, lowerBounds, upperBounds);
    return DataFiles.builder(spec)
        .withPath(path)
        .withPartition(partition)
        .withFileSizeInBytes(fileSize)
        .withFormat(FileFormat.PARQUET)
        .withMetrics(metrics)
        .build();
  }

  public static DeleteFile generatePositionDeleteFile(Table table, StructLike partition) {
    PartitionSpec spec = table.spec();
    LocationProvider locations = table.locationProvider();
    String path = locations.newDataLocation(spec, partition, generateFileName());
    long fileSize = generateFileSize();
    Metrics metrics = generatePositionDeleteMetrics();
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withPath(path)
        .withPartition(partition)
        .withFileSizeInBytes(fileSize)
        .withFormat(FileFormat.PARQUET)
        .withMetrics(metrics)
        .build();
  }

  public static DeleteFile generatePositionDeleteFile(Table table, DataFile dataFile) {
    PartitionSpec spec = table.spec();
    StructLike partition = dataFile.partition();
    LocationProvider locations = table.locationProvider();
    String path = locations.newDataLocation(spec, partition, generateFileName());
    long fileSize = generateFileSize();
    Metrics metrics = generatePositionDeleteMetrics(dataFile);
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withPath(path)
        .withPartition(partition)
        .withFileSizeInBytes(fileSize)
        .withFormat(FileFormat.PARQUET)
        .withMetrics(metrics)
        .build();
  }

  // mimics the behavior of OutputFileFactory
  public static String generateFileName() {
    int partitionId = random().nextInt(100_000);
    int taskId = random().nextInt(100);
    UUID operationId = UUID.randomUUID();
    int fileCount = random().nextInt(1_000);
    return String.format("%d-%d-%s-%d.parquet", partitionId, taskId, operationId, fileCount);
  }

  public static Metrics generateRandomMetrics(
      Schema schema,
      MetricsConfig metricsConfig,
      Map<Integer, ByteBuffer> knownLowerBounds,
      Map<Integer, ByteBuffer> knownUpperBounds) {
    long rowCount = generateRowCount();
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Long> nanValueCounts = Maps.newHashMap();
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();

    for (Types.NestedField column : schema.columns()) {
      int fieldId = column.fieldId();
      columnSizes.put(fieldId, generateColumnSize());
      valueCounts.put(fieldId, generateValueCount());
      nullValueCounts.put(fieldId, (long) random().nextInt(5));
      nanValueCounts.put(fieldId, (long) random().nextInt(5));
      if (knownLowerBounds.containsKey(fieldId) && knownUpperBounds.containsKey(fieldId)) {
        lowerBounds.put(fieldId, knownLowerBounds.get(fieldId));
        upperBounds.put(fieldId, knownUpperBounds.get(fieldId));
      } else if (column.type().isPrimitiveType()) {
        PrimitiveType type = column.type().asPrimitiveType();
        MetricsMode metricsMode = metricsConfig.columnMode(column.name());
        Pair<ByteBuffer, ByteBuffer> bounds = generateBounds(type, metricsMode);
        lowerBounds.put(fieldId, bounds.first());
        upperBounds.put(fieldId, bounds.second());
      }
    }

    return new Metrics(
        rowCount,
        columnSizes,
        valueCounts,
        nullValueCounts,
        nanValueCounts,
        lowerBounds,
        upperBounds);
  }

  private static Metrics generatePositionDeleteMetrics(DataFile dataFile) {
    long rowCount = generateRowCount();
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();

    for (Types.NestedField column : DeleteSchemaUtil.pathPosSchema().columns()) {
      int fieldId = column.fieldId();
      columnSizes.put(fieldId, generateColumnSize());
      if (fieldId == MetadataColumns.DELETE_FILE_PATH.fieldId()) {
        ByteBuffer bound = Conversions.toByteBuffer(Types.StringType.get(), dataFile.path());
        lowerBounds.put(fieldId, bound);
        upperBounds.put(fieldId, bound);
      }
    }

    return new Metrics(
        rowCount,
        columnSizes,
        null /* no value counts */,
        null /* no NULL counts */,
        null /* no NaN counts */,
        lowerBounds,
        upperBounds);
  }

  private static Metrics generatePositionDeleteMetrics() {
    long rowCount = generateRowCount();
    Map<Integer, Long> columnSizes = Maps.newHashMap();

    for (Types.NestedField column : DeleteSchemaUtil.pathPosSchema().columns()) {
      int fieldId = column.fieldId();
      columnSizes.put(fieldId, generateColumnSize());
    }

    return new Metrics(
        rowCount,
        columnSizes,
        null /* no value counts */,
        null /* no NULL counts */,
        null /* no NaN counts */,
        null /* no lower bounds */,
        null /* no upper bounds */);
  }

  private static long generateRowCount() {
    return 100_000L + random().nextInt(1000);
  }

  private static long generateColumnSize() {
    return 1_000_000L + random().nextInt(100_000);
  }

  private static long generateValueCount() {
    return 100_000L + random().nextInt(100);
  }

  private static long generateFileSize() {
    return random().nextInt(50_000);
  }

  private static Pair<ByteBuffer, ByteBuffer> generateBounds(PrimitiveType type, MetricsMode mode) {
    Comparator<Object> cmp = Comparators.forType(type);
    Object value1 = generateBound(type, mode);
    Object value2 = generateBound(type, mode);
    if (cmp.compare(value1, value2) > 0) {
      ByteBuffer lowerBuffer = Conversions.toByteBuffer(type, value2);
      ByteBuffer upperBuffer = Conversions.toByteBuffer(type, value1);
      return Pair.of(lowerBuffer, upperBuffer);
    } else {
      ByteBuffer lowerBuffer = Conversions.toByteBuffer(type, value1);
      ByteBuffer upperBuffer = Conversions.toByteBuffer(type, value2);
      return Pair.of(lowerBuffer, upperBuffer);
    }
  }

  private static Object generateBound(PrimitiveType type, MetricsMode mode) {
    if (mode instanceof None || mode instanceof Counts) {
      return null;
    } else if (mode instanceof Truncate) {
      Object value = RandomUtil.generatePrimitive(type, random());
      Transform<Object, Object> truncate = Transforms.truncate(((Truncate) mode).length());
      if (truncate.canTransform(type)) {
        return truncate.bind(type).apply(value);
      } else {
        return value;
      }
    } else {
      return RandomUtil.generatePrimitive(type, random());
    }
  }

  private static Random random() {
    return ThreadLocalRandom.current();
  }
}
