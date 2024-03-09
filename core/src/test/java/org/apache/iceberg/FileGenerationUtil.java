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
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

public class FileGenerationUtil {

  private FileGenerationUtil() {}

  public static DataFile generateDataFile(Table table, StructLike partition) {
    Schema schema = table.schema();
    PartitionSpec spec = table.spec();
    LocationProvider locations = table.locationProvider();
    String path = locations.newDataLocation(spec, partition, generateFileName());
    long fileSize = generateFileSize();
    Metrics metrics = generateRandomMetrics(schema);
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

  public static Metrics generateRandomMetrics(Schema schema) {
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
      byte[] lower = new byte[16];
      random().nextBytes(lower);
      lowerBounds.put(fieldId, ByteBuffer.wrap(lower));
      byte[] upper = new byte[16];
      random().nextBytes(upper);
      upperBounds.put(fieldId, ByteBuffer.wrap(upper));
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

  private static Random random() {
    return ThreadLocalRandom.current();
  }
}
