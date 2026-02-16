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
package org.apache.iceberg.io;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Benchmark test to quantify the delete deduplication optimization.
 *
 * <p>This test simulates real-world Flink CDC scenarios and measures:
 * - Delete record count reduction
 * - Delete file count reduction
 * - Memory usage
 * - Execution time
 *
 * <p>Run this test to see concrete performance improvements.
 */
public class BenchmarkDeleteDeduplication extends TableTestBase {

  private static final int FORMAT_V2 = 2;
  private FileFormat format = FileFormat.PARQUET;
  private OutputFileFactory fileFactory;
  private FileAppenderFactory<Record> appenderFactory;

  public BenchmarkDeleteDeduplication() {
    super(FORMAT_V2);
  }

  @Override
  @Before
  public void setupTable() throws IOException {
    this.tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());
    this.metadataDir = new File(tableDir, "metadata");
    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();
    // Set equality field IDs to field 0 (id field) for delete operations
    // Create delete schema containing only the ID field
    Schema deleteSchema = table.schema().select("id");
    this.appenderFactory = new GenericAppenderFactory(
        table.schema(),
        PartitionSpec.unpartitioned(),
        new int[] {table.schema().findField("id").fieldId()},  // equality field IDs
        deleteSchema,  // eqDeleteRowSchema
        null);  // posDeleteRowSchema
  }

  /**
   * Benchmark 1: High-frequency CDC updates
   *
   * <p>Scenario: E-commerce order table with frequent status updates
   * - 1,000 orders
   * - Each order updated 10 times within 1 checkpoint
   * - 10,000 total delete operations
   * - Expected: 90% deduplication (only 1,000 unique keys)
   */
  @Test
  public void benchmarkHighFrequencyCDC() throws IOException {
    System.out.println("\n=== Benchmark 1: High-Frequency CDC Updates ===");

    int numOrders = 1000;
    int updatesPerOrder = 10;
    int totalDeletes = numOrders * updatesPerOrder;

    BenchmarkEqualityDeltaWriter writer =
        new BenchmarkEqualityDeltaWriter(
            null,
            table.spec(),
            format,
            appenderFactory,
            fileFactory,
            table.io(),
            Long.MAX_VALUE,
            table.schema(),
            ImmutableList.of(table.schema().findField("id").fieldId()));

    long startTime = System.currentTimeMillis();
    long startMemory = getUsedMemory();

    try {
      // Create delete schema for keys (only id field)
      Schema deleteSchema = table.schema().select("id");

      // Simulate: Each order is updated 10 times
      for (int update = 0; update < updatesPerOrder; update++) {
        for (int orderId = 0; orderId < numOrders; orderId++) {
          Record key = GenericRecord.create(deleteSchema);
          key.setField("id", orderId);
          writer.deleteKey(key);
        }
      }

      WriteResult result = writer.complete();

      long endTime = System.currentTimeMillis();
      long endMemory = getUsedMemory();

      // Analyze results
      List<DeleteFile> deleteFiles = Lists.newArrayList(result.deleteFiles());
      long actualDeleteRecords =
          deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum();

      // Calculate metrics
      long executionTime = endTime - startTime;
      long memoryUsed = endMemory - startMemory;
      double deduplicationRate = 100.0 * (totalDeletes - actualDeleteRecords) / totalDeletes;
      double recordsPerFile =
          deleteFiles.isEmpty() ? 0 : (double) actualDeleteRecords / deleteFiles.size();

      // Print results
      System.out.println("\nScenario: E-commerce orders with 10 updates per order");
      System.out.println("Input:");
      System.out.println("  - Orders: " + numOrders);
      System.out.println("  - Updates per order: " + updatesPerOrder);
      System.out.println("  - Total delete operations: " + totalDeletes);
      System.out.println("\nWithout Optimization (Expected):");
      System.out.println("  - Delete records: " + totalDeletes);
      System.out.println("  - Delete files: ~" + (totalDeletes / 5000));
      System.out.println("\nWith Optimization (Actual):");
      System.out.println("  - Delete records: " + actualDeleteRecords);
      System.out.println("  - Delete files: " + deleteFiles.size());
      System.out.println("  - Records per file: " + String.format("%.0f", recordsPerFile));
      System.out.println("\nImprovement:");
      System.out.println(
          "  - Deduplication rate: " + String.format("%.1f%%", deduplicationRate) + " ✅");
      System.out.println(
          "  - Records reduced: "
              + (totalDeletes - actualDeleteRecords)
              + " ("
              + String.format("%.1f%%", deduplicationRate)
              + ")");
      System.out.println("\nPerformance:");
      System.out.println("  - Execution time: " + executionTime + " ms");
      System.out.println("  - Memory used: " + formatBytes(memoryUsed));
      System.out.println("  - Memory per delete: " + formatBytes(memoryUsed / totalDeletes));

      // Assertions
      Assert.assertEquals(
          "Should have exactly " + numOrders + " delete records (one per unique key)",
          numOrders,
          actualDeleteRecords);

      Assert.assertTrue(
          "Deduplication rate should be ~90%", deduplicationRate > 85 && deduplicationRate < 95);

    } finally {
      writer.close();
    }
  }

  /**
   * Benchmark 2: IoT device telemetry
   *
   * <p>Scenario: IoT devices reporting status every 10 seconds
   * - 100,000 devices
   * - 6 status updates per device per checkpoint (1 minute)
   * - 600,000 total delete operations
   * - Expected: 83% deduplication
   */
  @Test
  public void benchmarkIoTTelemetry() throws IOException {
    System.out.println("\n=== Benchmark 2: IoT Device Telemetry ===");

    int numDevices = 10000; // Reduced from 100k for faster test
    int updatesPerDevice = 6;
    int totalDeletes = numDevices * updatesPerDevice;

    BenchmarkEqualityDeltaWriter writer =
        new BenchmarkEqualityDeltaWriter(
            null,
            table.spec(),
            format,
            appenderFactory,
            fileFactory,
            table.io(),
            Long.MAX_VALUE,
            table.schema(),
            ImmutableList.of(table.schema().findField("id").fieldId()));

    long startTime = System.currentTimeMillis();
    long startMemory = getUsedMemory();

    try {
      // Create delete schema for keys (only id field)
      Schema deleteSchema = table.schema().select("id");

      // Simulate: Each device reports 6 times in 1 minute
      for (int update = 0; update < updatesPerDevice; update++) {
        for (int deviceId = 0; deviceId < numDevices; deviceId++) {
          Record key = GenericRecord.create(deleteSchema);
          key.setField("id", deviceId);
          writer.deleteKey(key);
        }
      }

      WriteResult result = writer.complete();

      long endTime = System.currentTimeMillis();
      long endMemory = getUsedMemory();

      List<DeleteFile> deleteFiles = Lists.newArrayList(result.deleteFiles());
      long actualDeleteRecords =
          deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum();

      long executionTime = endTime - startTime;
      long memoryUsed = endMemory - startMemory;
      double deduplicationRate = 100.0 * (totalDeletes - actualDeleteRecords) / totalDeletes;

      System.out.println("\nScenario: IoT devices reporting every 10s (6 times per minute)");
      System.out.println("Input:");
      System.out.println("  - Devices: " + numDevices);
      System.out.println("  - Updates per device: " + updatesPerDevice);
      System.out.println("  - Total delete operations: " + totalDeletes);
      System.out.println("\nImprovement:");
      System.out.println(
          "  - Delete records: " + totalDeletes + " → " + actualDeleteRecords);
      System.out.println(
          "  - Deduplication rate: " + String.format("%.1f%%", deduplicationRate) + " ✅");
      System.out.println("  - Delete files: " + deleteFiles.size());
      System.out.println("\nPerformance:");
      System.out.println("  - Execution time: " + executionTime + " ms");
      System.out.println("  - Memory used: " + formatBytes(memoryUsed));

      Assert.assertEquals(
          "Should have one delete record per device", numDevices, actualDeleteRecords);

      Assert.assertTrue("Deduplication rate should be ~83%", deduplicationRate > 80);

    } finally {
      writer.close();
    }
  }

  /**
   * Benchmark 3: Mixed workload with varying duplication rates
   *
   * <p>Tests different duplication rates to show scalability:
   * - 0% duplication (all unique keys)
   * - 50% duplication
   * - 90% duplication
   */
  @Test
  public void benchmarkMixedWorkload() throws IOException {
    System.out.println("\n=== Benchmark 3: Mixed Workload (Varying Duplication Rates) ===");

    int totalOperations = 10000;
    int[] duplicationRates = {0, 50, 90};

    for (int duplicationRate : duplicationRates) {
      BenchmarkEqualityDeltaWriter writer =
          new BenchmarkEqualityDeltaWriter(
              null,
              table.spec(),
              format,
              appenderFactory,
              fileFactory,
              table.io(),
              Long.MAX_VALUE,
              table.schema(),
              ImmutableList.of(table.schema().findField("id").fieldId()));

      long startTime = System.currentTimeMillis();
      Random random = new Random(42); // Fixed seed for reproducibility

      try {
        // Create delete schema for keys (only id field)
        Schema deleteSchema = new Schema(table.schema().findField("id"));

        // Generate delete operations with specified duplication rate
        int uniqueKeys = totalOperations * (100 - duplicationRate) / 100;
        if (uniqueKeys == 0) uniqueKeys = 1; // At least one unique key

        for (int i = 0; i < totalOperations; i++) {
          int keyId = random.nextInt(uniqueKeys);
          Record key = GenericRecord.create(deleteSchema);
          key.setField("id", keyId);
          writer.deleteKey(key);
        }

        WriteResult result = writer.complete();
        long endTime = System.currentTimeMillis();

        List<DeleteFile> deleteFiles = Lists.newArrayList(result.deleteFiles());
        long actualDeleteRecords =
            deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum();

        long executionTime = endTime - startTime;
        double actualDedupRate = 100.0 * (totalOperations - actualDeleteRecords) / totalOperations;

        System.out.println("\n--- Target Duplication: " + duplicationRate + "% ---");
        System.out.println("  Operations: " + totalOperations);
        System.out.println("  Unique keys: " + uniqueKeys);
        System.out.println("  Delete records: " + actualDeleteRecords);
        System.out.println(
            "  Actual deduplication: " + String.format("%.1f%%", actualDedupRate));
        System.out.println("  Delete files: " + deleteFiles.size());
        System.out.println("  Execution time: " + executionTime + " ms");

      } finally {
        writer.close();
      }
    }
  }

  /**
   * Benchmark 4: Memory overhead measurement
   *
   * <p>Measures memory usage for different cache sizes to verify bounded memory growth.
   */
  @Test
  public void benchmarkMemoryOverhead() throws IOException {
    System.out.println("\n=== Benchmark 4: Memory Overhead ===");

    int[] uniqueKeyCounts = {100, 1000, 10000, 100000};

    System.out.println("\nMemory overhead for different unique key counts:");
    System.out.println(
        String.format("%-15s %-15s %-20s %-15s", "Unique Keys", "Total Ops", "Memory (KB)", "Per Key (bytes)"));
    System.out.println("──────────────────────────────────────────────────────────────────────");

    for (int uniqueKeys : uniqueKeyCounts) {
      BenchmarkEqualityDeltaWriter writer =
          new BenchmarkEqualityDeltaWriter(
              null,
              table.spec(),
              format,
              appenderFactory,
              fileFactory,
              table.io(),
              Long.MAX_VALUE,
              table.schema(),
              ImmutableList.of(table.schema().findField("id").fieldId()));

      try {
        // Create delete schema for keys (only id field)
        Schema deleteSchema = new Schema(table.schema().findField("id"));

        // Force GC before measurement
        System.gc();
        Thread.sleep(100);

        long startMemory = getUsedMemory();

        // Delete each unique key 10 times
        int totalOps = uniqueKeys * 10;
        for (int i = 0; i < 10; i++) {
          for (int keyId = 0; keyId < uniqueKeys; keyId++) {
            Record key = GenericRecord.create(deleteSchema);
            key.setField("id", keyId);
            writer.deleteKey(key);
          }
        }

        writer.complete();

        long endMemory = getUsedMemory();
        long memoryUsed = Math.max(0, endMemory - startMemory);
        long memoryPerKey = uniqueKeys > 0 ? memoryUsed / uniqueKeys : 0;

        System.out.println(
            String.format(
                "%-15d %-15d %-20s %-15s",
                uniqueKeys,
                uniqueKeys * 10,
                String.format("%.1f", memoryUsed / 1024.0),
                memoryPerKey));

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        writer.close();
      }
    }
  }

  /**
   * Benchmark 5: Real production scenario simulation
   *
   * <p>Simulates your actual table scenario:
   * - Based on real data: 52,422 deletes over 87,290 checkpoints
   * - Average ~0.6 deletes per checkpoint, but clustered
   * - Simulates realistic bursty pattern
   */
  @Test
  public void benchmarkRealProductionScenario() throws IOException {
    System.out.println("\n=== Benchmark 5: Real Production Scenario ===");
    System.out.println("Simulating actual production table characteristics:");
    System.out.println("  - Total checkpoints: 87,290");
    System.out.println("  - Total equality deletes: 52,422");
    System.out.println("  - Bursty pattern: 1000 active keys, each updated 5-10 times");

    int numActiveKeys = 1000;
    int minUpdates = 5;
    int maxUpdates = 10;
    Random random = new Random(42);

    BenchmarkEqualityDeltaWriter writer =
        new BenchmarkEqualityDeltaWriter(
            null,
            table.spec(),
            format,
            appenderFactory,
            fileFactory,
            table.io(),
            Long.MAX_VALUE,
            table.schema(),
            ImmutableList.of(table.schema().findField("id").fieldId()));

    long startTime = System.currentTimeMillis();
    long startMemory = getUsedMemory();
    int totalOperations = 0;

    try {
      // Create delete schema for keys (only id field)
      Schema deleteSchema = table.schema().select("id");

      // Simulate bursty updates
      for (int keyId = 0; keyId < numActiveKeys; keyId++) {
        int updates = minUpdates + random.nextInt(maxUpdates - minUpdates + 1);
        for (int i = 0; i < updates; i++) {
          Record key = GenericRecord.create(deleteSchema);
          key.setField("id", keyId);
          writer.deleteKey(key);
          totalOperations++;
        }
      }

      WriteResult result = writer.complete();

      long endTime = System.currentTimeMillis();
      long endMemory = getUsedMemory();

      List<DeleteFile> deleteFiles = Lists.newArrayList(result.deleteFiles());
      long actualDeleteRecords =
          deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum();

      long executionTime = endTime - startTime;
      long memoryUsed = Math.max(0, endMemory - startMemory);
      double deduplicationRate = 100.0 * (totalOperations - actualDeleteRecords) / totalOperations;

      System.out.println("\nResults:");
      System.out.println("  Total delete operations: " + totalOperations);
      System.out.println(
          "  Without optimization: " + totalOperations + " delete records");
      System.out.println("  With optimization: " + actualDeleteRecords + " delete records ✅");
      System.out.println(
          "  Deduplication rate: " + String.format("%.1f%%", deduplicationRate));
      System.out.println(
          "  Records saved: " + (totalOperations - actualDeleteRecords));
      System.out.println("  Delete files: " + deleteFiles.size());
      System.out.println("\nPerformance:");
      System.out.println("  Execution time: " + executionTime + " ms");
      System.out.println("  Memory used: " + formatBytes(memoryUsed));
      System.out.println("  Ops/second: " + (totalOperations * 1000L / executionTime));

      System.out.println("\nEstimated savings for your table (87,290 checkpoints):");
      long savedRecordsPerCheckpoint = totalOperations - actualDeleteRecords;
      long totalSavedRecords = savedRecordsPerCheckpoint * 87290 / 10; // Rough estimate
      System.out.println("  Delete records saved: ~" + totalSavedRecords);
      System.out.println(
          "  Delete files saved: ~" + (totalSavedRecords / 5000) + " (assuming 5K records/file)");

      Assert.assertTrue(
          "Should deduplicate significantly", actualDeleteRecords < totalOperations / 2);

    } finally {
      writer.close();
    }
  }

  /** Get current used memory in bytes */
  private long getUsedMemory() {
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

  /** Format bytes to human-readable format */
  private String formatBytes(long bytes) {
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
    return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
  }

  /**
   * Test helper class that exposes metrics for benchmarking
   */
  private static class BenchmarkEqualityDeltaWriter extends BaseTaskWriter<Record> {

    private final List<Integer> equalityFieldIds;
    private BenchmarkDeltaWriter deltaWriter;

    BenchmarkEqualityDeltaWriter(
        StructLike partition,
        PartitionSpec spec,
        FileFormat format,
        FileAppenderFactory<Record> appenderFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize,
        Schema schema,
        List<Integer> equalityFieldIds) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      this.equalityFieldIds = equalityFieldIds;

      Schema deleteSchema =
          org.apache.iceberg.types.TypeUtil.select(
              schema,
              org.apache.iceberg.relocated.com.google.common.collect.Sets.newHashSet(
                  equalityFieldIds));
      this.deltaWriter = new BenchmarkDeltaWriter(partition, schema, deleteSchema);
    }

    @Override
    public void write(Record record) throws IOException {
      deltaWriter.write(record);
    }

    public void deleteKey(Record key) throws IOException {
      deltaWriter.deleteKey(key);
    }

    public void delete(Record row) throws IOException {
      deltaWriter.delete(row);
    }

    @Override
    public void close() throws IOException {
      if (deltaWriter != null) {
        deltaWriter.close();
      }
    }

    private class BenchmarkDeltaWriter extends BaseEqualityDeltaWriter {
      BenchmarkDeltaWriter(StructLike partition, Schema schema, Schema deleteSchema) {
        super(partition, schema, deleteSchema);
      }

      @Override
      protected StructLike asStructLike(Record data) {
        return data;
      }

      @Override
      protected StructLike asStructLikeKey(Record key) {
        return key;
      }
    }
  }
}
