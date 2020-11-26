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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestManifestReaderStats extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestManifestReaderStats(int formatVersion) {
    super(formatVersion);
  }

  private static final Map<Integer, Long> VALUE_COUNT = ImmutableMap.of(3, 3L);
  private static final Map<Integer, Long> NULL_VALUE_COUNTS = ImmutableMap.of(3, 0L);
  private static final Map<Integer, Long> NAN_VALUE_COUNTS = ImmutableMap.of(3, 1L);
  private static final Map<Integer, ByteBuffer> LOWER_BOUNDS =
      ImmutableMap.of(3, Conversions.toByteBuffer(Types.IntegerType.get(), 2));
  private static final Map<Integer, ByteBuffer> UPPER_BOUNDS =
      ImmutableMap.of(3, Conversions.toByteBuffer(Types.IntegerType.get(), 4));

  private static final Metrics METRICS = new Metrics(3L, null,
      VALUE_COUNT, NULL_VALUE_COUNTS, NAN_VALUE_COUNTS, LOWER_BOUNDS, UPPER_BOUNDS);

  private static final DataFile FILE = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(3)
      .withMetrics(METRICS)
      .build();

  @Test
  public void testReadIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      ManifestEntry<DataFile> entry = entries.iterator().next();
      assertFullStats(entry.file());
    }
  }

  @Test
  public void testReadWithFilterIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)
        .filterRows(Expressions.equal("id", 3))) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      ManifestEntry<DataFile> entry = entries.iterator().next();
      assertFullStats(entry.file());
    }
  }

  @Test
  public void testReadEntriesWithFilterAndSelectIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)
        .select(ImmutableSet.of("record_count"))
        .filterRows(Expressions.equal("id", 3))) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      ManifestEntry<DataFile> entry = entries.iterator().next();
      assertFullStats(entry.file());
    }
  }

  @Test
  public void testReadIteratorWithFilterAndSelectDropsStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)
        .select(ImmutableSet.of("record_count"))
        .filterRows(Expressions.equal("id", 3))) {
      DataFile entry = reader.iterator().next();
      assertStatsDropped(entry);
    }
  }
  @Test
  public void testReadIteratorWithFilterAndSelectStatsIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)
        .select(ImmutableSet.of("record_count", "value_counts"))
        .filterRows(Expressions.equal("id", 3))) {
      DataFile entry = reader.iterator().next();
      assertFullStats(entry);
    }
  }

  @Test
  public void testReadEntriesWithSelectNotIncludeFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)
        .select(ImmutableSet.of("record_count"))) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      ManifestEntry<DataFile> entry = entries.iterator().next();
      assertStatsDropped(entry.file());
    }
  }
  @Test
  public void testReadEntriesWithSelectCertainStatNotIncludeFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)
        .select(ImmutableSet.of("record_count", "value_counts"))) {
      DataFile dataFile = reader.iterator().next();

      Assert.assertEquals(3, dataFile.recordCount());
      Assert.assertNull(dataFile.columnSizes());
      Assert.assertEquals(VALUE_COUNT, dataFile.valueCounts());
      Assert.assertNull(dataFile.nullValueCounts());
      Assert.assertNull(dataFile.lowerBounds());
      Assert.assertNull(dataFile.upperBounds());
      Assert.assertNull(dataFile.nanValueCounts());
    }
  }

  private void assertFullStats(DataFile dataFile) {
    Assert.assertEquals(3, dataFile.recordCount());
    Assert.assertNull(dataFile.columnSizes());
    Assert.assertEquals(VALUE_COUNT, dataFile.valueCounts());
    Assert.assertEquals(NULL_VALUE_COUNTS, dataFile.nullValueCounts());
    Assert.assertEquals(LOWER_BOUNDS, dataFile.lowerBounds());
    Assert.assertEquals(UPPER_BOUNDS, dataFile.upperBounds());
    Assert.assertEquals(NAN_VALUE_COUNTS, dataFile.nanValueCounts());
  }

  private void assertStatsDropped(DataFile dataFile) {
    Assert.assertEquals(3, dataFile.recordCount()); // always select record count in all test cases
    Assert.assertNull(dataFile.columnSizes());
    Assert.assertNull(dataFile.valueCounts());
    Assert.assertNull(dataFile.nullValueCounts());
    Assert.assertNull(dataFile.lowerBounds());
    Assert.assertNull(dataFile.upperBounds());
    Assert.assertNull(dataFile.nanValueCounts());
  }

}
