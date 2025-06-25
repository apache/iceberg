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
package org.apache.iceberg.flink.source;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFlinkSplitPlanner {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final FileFormat FILE_FORMAT = FileFormat.PARQUET;
  private ExecutorService workerPool;
  private HadoopCatalog catalog;
  private Table table;
  private Schema tableSchema;

  @Before
  public void before() throws Exception {
    workerPool = Executors.newFixedThreadPool(4);

    // Create a temporary catalog
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    String warehouse = "file:" + warehouseFile;
    Configuration hadoopConf = new Configuration();
    catalog = new HadoopCatalog(hadoopConf, warehouse);

    // Create a table with identity partitioning on 'data' field
    this.tableSchema =
        new Schema(
            required(1, "id", org.apache.iceberg.types.Types.LongType.get()),
            required(2, "data", org.apache.iceberg.types.Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(tableSchema).bucket("id", 4).build();

    ImmutableMap<String, String> properties = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");

    table = catalog.createTable(TestFixtures.TABLE_IDENTIFIER, tableSchema, spec, null, properties);
  }

  @After
  public void after() throws Exception {
    workerPool.shutdown();
    catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
    catalog.close();
  }

  @Test
  public void testPlanPartitionAwareSplitsWithBucketPartitioning() throws Exception {
    try {
      // Create records for each bucket (0, 1, 2, 3)
      // bucket(id, 4) = id % 4, so we use ids: 100, 101, 102, 103
      GenericAppenderHelper dataAppender =
          new GenericAppenderHelper(table, FILE_FORMAT, TEMPORARY_FOLDER);

      List<Record> bucket0Records = RandomGenericData.generate(tableSchema, 2, 0L);
      bucket0Records.get(0).setField("id", 100L); // bucket(100, 4) = 0
      bucket0Records.get(1).setField("id", 104L); // bucket(104, 4) = 0

      List<Record> bucket1Records = RandomGenericData.generate(tableSchema, 2, 1L);
      bucket1Records.get(0).setField("id", 101L); // bucket(101, 4) = 1
      bucket1Records.get(1).setField("id", 105L); // bucket(105, 4) = 1

      List<Record> bucket2Records = RandomGenericData.generate(tableSchema, 2, 2L);
      bucket2Records.get(0).setField("id", 102L); // bucket(102, 4) = 2
      bucket2Records.get(1).setField("id", 106L); // bucket(106, 4) = 2

      List<Record> bucket3Records = RandomGenericData.generate(tableSchema, 2, 3L);
      bucket3Records.get(0).setField("id", 103L); // bucket(103, 4) = 3
      bucket3Records.get(1).setField("id", 107L); // bucket(107, 4) = 3

      // Write files with explicit bucket values
      dataAppender.appendToTable(org.apache.iceberg.TestHelpers.Row.of(0), bucket0Records);
      dataAppender.appendToTable(org.apache.iceberg.TestHelpers.Row.of(1), bucket1Records);
      dataAppender.appendToTable(org.apache.iceberg.TestHelpers.Row.of(2), bucket2Records);
      dataAppender.appendToTable(org.apache.iceberg.TestHelpers.Row.of(3), bucket3Records);

      ScanContext scanContext = ScanContext.builder().build();
      List<IcebergSourceSplit> splits =
          FlinkSplitPlanner.planIcebergPartitionAwareSourceSplits(table, scanContext, workerPool);

      Assert.assertEquals("Should have one split per bucket", 4, splits.size());
      for (IcebergSourceSplit split : splits) {
        long totalRecords =
            split.task().files().stream()
                .mapToLong(fileScanTask -> fileScanTask.file().recordCount())
                .sum();
        Assert.assertEquals("Each split should contain 2 records", 2, totalRecords);
      }
    } finally {
      catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
    }
  }

  @Test
  public void testPlanPartitionAwareSplitsWithUnpartitionedTable() throws Exception {
    // Create an unpartitioned table (no partition spec)
    Schema unpartitionedSchema =
        new Schema(
            required(1, "id", org.apache.iceberg.types.Types.LongType.get()),
            required(2, "data", org.apache.iceberg.types.Types.StringType.get()));

    // Create table with unpartitioned spec
    PartitionSpec unpartitionedSpec = PartitionSpec.unpartitioned();

    Table unpartitionedTable =
        catalog.createTable(
            TableIdentifier.of("default", "unpartitioned_test"),
            unpartitionedSchema,
            unpartitionedSpec,
            null,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));

    try {
      GenericAppenderHelper dataAppender =
          new GenericAppenderHelper(unpartitionedTable, FILE_FORMAT, TEMPORARY_FOLDER);
      List<Record> records = RandomGenericData.generate(unpartitionedSchema, 5, 0L);
      dataAppender.appendToTable(records);

      ScanContext scanContext = ScanContext.builder().build();

      // This should throw IllegalArgumentException
      RuntimeException exception =
          Assert.assertThrows(
              RuntimeException.class,
              () ->
                  FlinkSplitPlanner.planIcebergPartitionAwareSourceSplits(
                      unpartitionedTable, scanContext, workerPool));

      Assert.assertTrue(
          "Error message should mention grouping fields",
          exception
              .getMessage()
              .contains("Currently only Partitions that are able to be grouped are supported"));

    } finally {
      catalog.dropTable(TableIdentifier.of("default", "unpartitioned_test"));
    }
  }

  @Test
  public void testPlanPartitionAwareSplitsWithConflictingPartitionSpecs() throws Exception {
    // Table with conflicting/incompatible partition specs schema(id, data, category)
    Schema schema =
        new Schema(
            required(1, "id", org.apache.iceberg.types.Types.LongType.get()),
            required(2, "data", org.apache.iceberg.types.Types.StringType.get()),
            required(3, "category", org.apache.iceberg.types.Types.StringType.get()));

    // PARTITION BY data
    PartitionSpec initialSpec = PartitionSpec.builderFor(schema).identity("data").build();
    Table conflictingTable =
        catalog.createTable(
            TableIdentifier.of("default", "conflicting_specs"),
            schema,
            initialSpec,
            null,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));

    try {
      // Add some data with the initial spec
      GenericAppenderHelper dataAppender =
          new GenericAppenderHelper(conflictingTable, FILE_FORMAT, TEMPORARY_FOLDER);
      List<Record> records = RandomGenericData.generate(schema, 2, 0L);
      records.get(0).setField("data", "test1");
      records.get(0).setField("category", "cat1");
      records.get(1).setField("data", "test2");
      records.get(1).setField("category", "cat2");
      dataAppender.appendToTable(
          org.apache.iceberg.TestHelpers.Row.of("test1"), records.subList(0, 1));
      dataAppender.appendToTable(
          org.apache.iceberg.TestHelpers.Row.of("test2"), records.subList(1, 2));

      // Create a new spec that conflicts with the existing one
      // This creates incompatible specs that can't be grouped together
      // PARTITIONED BY category
      PartitionSpec conflictingSpec = PartitionSpec.builderFor(schema).identity("category").build();

      // Manually add the conflicting spec to create incompatible partition evolution
      TableOperations ops = ((HasTableOperations) conflictingTable).operations();
      TableMetadata current = ops.current();
      ops.commit(current, current.updatePartitionSpec(conflictingSpec));

      Assert.assertEquals("Should have 2 specs", 2, conflictingTable.specs().size());

      ScanContext scanContext = ScanContext.builder().build();

      // This should throw IllegalArgumentException due to conflicting specs
      RuntimeException exception =
          Assert.assertThrows(
              RuntimeException.class,
              () ->
                  FlinkSplitPlanner.planIcebergPartitionAwareSourceSplits(
                      conflictingTable, scanContext, workerPool));

      Assert.assertTrue(
          "Error message should mention grouping fields",
          exception
              .getMessage()
              .contains("Currently only Partitions that are able to be grouped are supported"));

    } finally {
      catalog.dropTable(TableIdentifier.of("default", "conflicting_specs"));
    }
  }
}
