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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

/**
 * Shared constants and stateless helpers for {@link ManifestBenchmark} and {@link
 * ManifestCompressionBenchmark}.
 */
final class ManifestBenchmarkUtil {

  static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()),
          Types.NestedField.required(3, "customer", Types.StringType.get()));

  static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("id").identity("data").identity("customer").build();

  private ManifestBenchmarkUtil() {}

  /**
   * Returns the number of manifest entries for the given column count. The result is {@code
   * entryBase / cols}.
   *
   * <p>The linear ratio was determined empirically by writing manifests at various column counts
   * and measuring the resulting file sizes. An {@code entryBase} of 300,000 produces ~8 MB
   * manifests (matching the default {@code commit.manifest.target-size-bytes}); 15,000 produces
   * ~400 KB.
   */
  static int entriesForColumnCount(int entryBase, int cols) {
    return entryBase / cols;
  }

  static List<DataFile> generateDataFiles(PartitionSpec spec, int numEntries, int numCols) {
    Random random = new Random(42);
    List<DataFile> files = Lists.newArrayListWithCapacity(numEntries);
    for (int i = 0; i < numEntries; i++) {
      DataFiles.Builder builder =
          DataFiles.builder(spec)
              .withFormat(FileFormat.PARQUET)
              .withPath(String.format(Locale.ROOT, "/path/to/data-%d.parquet", i))
              .withFileSizeInBytes(1024 + i)
              .withRecordCount(1000 + i)
              .withMetrics(randomMetrics(random, numCols));

      if (!spec.isUnpartitioned()) {
        builder.withPartitionPath(
            String.format(
                Locale.ROOT, "id=%d/data=val-%d/customer=cust-%d", i % 100, i % 50, i % 200));
      }

      files.add(builder.build());
    }
    return files;
  }

  static Metrics randomMetrics(Random random, int cols) {
    long rowCount = 100_000L + random.nextInt(1000);
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Long> nanValueCounts = Maps.newHashMap();
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    for (int i = 0; i < cols; i++) {
      columnSizes.put(i, 1_000_000L + random.nextInt(100_000));
      valueCounts.put(i, 100_000L + random.nextInt(100));
      nullValueCounts.put(i, (long) random.nextInt(5));
      nanValueCounts.put(i, (long) random.nextInt(5));
      byte[] lower = new byte[8];
      random.nextBytes(lower);
      lowerBounds.put(i, ByteBuffer.wrap(lower));
      byte[] upper = new byte[8];
      random.nextBytes(upper);
      upperBounds.put(i, ByteBuffer.wrap(upper));
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

  static void cleanDir(String dir) {
    if (dir != null) {
      FileUtils.deleteQuietly(new java.io.File(dir));
    }
  }
}
