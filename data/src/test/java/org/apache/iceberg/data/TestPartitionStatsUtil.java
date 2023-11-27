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
package org.apache.iceberg.data;

import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionEntry;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPartitionStatsUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TestPartitionStatsUtil.class);

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "binary", Types.BinaryType.get()));

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testPartitionStats() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").identity("binary").build();
    Table testTable =
        TestTables.create(
            temp.newFolder("test_partition_stats"),
            "test_partition_stats",
            SCHEMA,
            spec,
            SortOrder.unsorted(),
            2);

    Schema schema =
        PartitionEntry.icebergSchema(Partitioning.partitionType(testTable.specs().values()));

    ImmutableList.Builder<PartitionEntry> partitionListBuilder = ImmutableList.builder();

    long seed = System.currentTimeMillis();
    LOG.info("Seed used for random generator is {}", seed);
    Random random = new Random(seed);

    for (int i = 0; i < 42; i++) {
      PartitionData partitionData =
          new PartitionData(
              schema.findField(PartitionEntry.Column.PARTITION_DATA.name()).type().asStructType());
      partitionData.set(0, random.nextLong());

      PartitionEntry partition =
          PartitionEntry.builder()
              .withPartitionData(partitionData)
              .withSpecId(random.nextInt(10))
              .withDataRecordCount(random.nextLong())
              .withDataFileCount(random.nextInt())
              .withDataFileSizeInBytes(1024L * random.nextInt(20))
              .withPosDeleteRecordCount(random.nextLong())
              .withPosDeleteFileCount(random.nextInt())
              .withEqDeleteRecordCount(random.nextLong())
              .withEqDeleteFileCount(random.nextInt())
              .withTotalRecordCount(random.nextLong())
              .withLastUpdatedAt(random.nextLong())
              .withLastUpdatedSnapshotId(random.nextLong())
              .build();

      partitionListBuilder.add(partition);
    }
    List<PartitionEntry> records = partitionListBuilder.build();

    String dataFileFormatName =
        testTable
            .properties()
            .getOrDefault(
                TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);

    OutputFile outputFile =
        Files.localOutput(
            testTable.location() + "/metadata/" + UUID.randomUUID() + "." + dataFileFormatName);
    PartitionStatsUtil.writePartitionStatsFile(
        records.iterator(), outputFile, testTable.specs().values());

    Assertions.assertThat(Paths.get(outputFile.location())).exists();

    List<PartitionEntry> rows;
    try (CloseableIterable<PartitionEntry> recordIterator =
        PartitionStatsUtil.readPartitionStatsFile(
            schema, Files.localInput(outputFile.location()))) {
      rows = Lists.newArrayList(recordIterator);
    }

    Assertions.assertThat(rows).hasSize(records.size());
    for (int i = 0; i < records.size(); i++) {
      Assertions.assertThat(rows.get(i)).isEqualTo(records.get(i));
    }
  }
}
