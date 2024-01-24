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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.DataOperations.DELETE;
import static org.apache.iceberg.DataOperations.OVERWRITE;
import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;
import static org.apache.iceberg.SnapshotSummary.ADDED_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.ADDED_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.CHANGED_PARTITION_COUNT_PROP;
import static org.apache.iceberg.SnapshotSummary.DELETED_FILES_PROP;
import static org.apache.iceberg.TableProperties.DATA_PLANNING_MODE;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DELETE_PLANNING_MODE;
import static org.apache.iceberg.TableProperties.ORC_VECTORIZATION_ENABLED;
import static org.apache.iceberg.TableProperties.PARQUET_VECTORIZATION_ENABLED;
import static org.apache.iceberg.TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_HASH;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_RANGE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.execution.SparkPlan;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class SparkRowLevelOperationsTestBase extends SparkExtensionsTestBase {

  private static final Random RANDOM = ThreadLocalRandom.current();

  protected final String fileFormat;
  protected final boolean vectorized;
  protected final String distributionMode;
  protected final boolean fanoutEnabled;
  protected final String branch;
  protected final PlanningMode planningMode;

  public SparkRowLevelOperationsTestBase(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      boolean vectorized,
      String distributionMode,
      boolean fanoutEnabled,
      String branch,
      PlanningMode planningMode) {
    super(catalogName, implementation, config);
    this.fileFormat = fileFormat;
    this.vectorized = vectorized;
    this.distributionMode = distributionMode;
    this.fanoutEnabled = fanoutEnabled;
    this.branch = branch;
    this.planningMode = planningMode;
  }

  @Parameters(
      name =
          "catalogName = {0}, implementation = {1}, config = {2},"
              + " format = {3}, vectorized = {4}, distributionMode = {5},"
              + " fanout = {6}, branch = {7}, planningMode = {8}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default"),
        "orc",
        true,
        WRITE_DISTRIBUTION_MODE_NONE,
        true,
        SnapshotRef.MAIN_BRANCH,
        LOCAL
      },
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default"),
        "parquet",
        true,
        WRITE_DISTRIBUTION_MODE_NONE,
        false,
        "test",
        DISTRIBUTED
      },
      {
        "testhadoop",
        SparkCatalog.class.getName(),
        ImmutableMap.of("type", "hadoop"),
        "parquet",
        RANDOM.nextBoolean(),
        WRITE_DISTRIBUTION_MODE_HASH,
        true,
        null,
        LOCAL
      },
      {
        "spark_catalog",
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "clients", "1",
            "parquet-enabled", "false",
            "cache-enabled",
                "false" // Spark will delete tables using v1, leaving the cache out of sync
            ),
        "avro",
        false,
        WRITE_DISTRIBUTION_MODE_RANGE,
        false,
        "test",
        DISTRIBUTED
      },
      {
        "spark_catalog",
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            "default-namespace",
            "default",
            "clients",
            "1",
            "parquet-enabled",
            "false",
            "cache-enabled",
            "false" // Spark will delete tables using v1, leaving the cache out of sync
            ),
        "parquet",
        false,
        WRITE_DISTRIBUTION_MODE_RANGE,
        false,
        "test",
        DISTRIBUTED
      }
    };
  }

  protected abstract Map<String, String> extraTableProperties();

  protected void initTable() {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s', '%s' '%s', '%s' '%s', '%s' '%s', '%s' '%s')",
        tableName,
        DEFAULT_FILE_FORMAT,
        fileFormat,
        WRITE_DISTRIBUTION_MODE,
        distributionMode,
        SPARK_WRITE_PARTITIONED_FANOUT_ENABLED,
        String.valueOf(fanoutEnabled),
        DATA_PLANNING_MODE,
        planningMode.modeName(),
        DELETE_PLANNING_MODE,
        planningMode.modeName());

    switch (fileFormat) {
      case "parquet":
        sql(
            "ALTER TABLE %s SET TBLPROPERTIES('%s' '%b')",
            tableName, PARQUET_VECTORIZATION_ENABLED, vectorized);
        break;
      case "orc":
        sql(
            "ALTER TABLE %s SET TBLPROPERTIES('%s' '%b')",
            tableName, ORC_VECTORIZATION_ENABLED, vectorized);
        break;
      case "avro":
        Assert.assertFalse(vectorized);
        break;
    }

    Map<String, String> props = extraTableProperties();
    props.forEach(
        (prop, value) -> {
          sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')", tableName, prop, value);
        });
  }

  protected void createAndInitTable(String schema) {
    createAndInitTable(schema, null);
  }

  protected void createAndInitTable(String schema, String jsonData) {
    createAndInitTable(schema, "", jsonData);
  }

  protected void createAndInitTable(String schema, String partitioning, String jsonData) {
    sql("CREATE TABLE %s (%s) USING iceberg %s", tableName, schema, partitioning);
    initTable();

    if (jsonData != null) {
      try {
        Dataset<Row> ds = toDS(schema, jsonData);
        ds.coalesce(1).writeTo(tableName).append();
        createBranchIfNeeded();
      } catch (NoSuchTableException e) {
        throw new RuntimeException("Failed to write data", e);
      }
    }
  }

  protected void append(String table, String jsonData) {
    append(table, null, jsonData);
  }

  protected void append(String table, String schema, String jsonData) {
    try {
      Dataset<Row> ds = toDS(schema, jsonData);
      ds.coalesce(1).writeTo(table).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException("Failed to write data", e);
    }
  }

  protected void createOrReplaceView(String name, String jsonData) {
    createOrReplaceView(name, null, jsonData);
  }

  protected void createOrReplaceView(String name, String schema, String jsonData) {
    Dataset<Row> ds = toDS(schema, jsonData);
    ds.createOrReplaceTempView(name);
  }

  protected <T> void createOrReplaceView(String name, List<T> data, Encoder<T> encoder) {
    spark.createDataset(data, encoder).createOrReplaceTempView(name);
  }

  private Dataset<Row> toDS(String schema, String jsonData) {
    List<String> jsonRows =
        Arrays.stream(jsonData.split("\n"))
            .filter(str -> str.trim().length() > 0)
            .collect(Collectors.toList());
    Dataset<String> jsonDS = spark.createDataset(jsonRows, Encoders.STRING());

    if (schema != null) {
      return spark.read().schema(schema).json(jsonDS);
    } else {
      return spark.read().json(jsonDS);
    }
  }

  protected void validateDelete(
      Snapshot snapshot, String changedPartitionCount, String deletedDataFiles) {
    validateSnapshot(snapshot, DELETE, changedPartitionCount, deletedDataFiles, null, null);
  }

  protected void validateCopyOnWrite(
      Snapshot snapshot,
      String changedPartitionCount,
      String deletedDataFiles,
      String addedDataFiles) {
    validateSnapshot(
        snapshot, OVERWRITE, changedPartitionCount, deletedDataFiles, null, addedDataFiles);
  }

  protected void validateMergeOnRead(
      Snapshot snapshot,
      String changedPartitionCount,
      String addedDeleteFiles,
      String addedDataFiles) {
    validateSnapshot(
        snapshot, OVERWRITE, changedPartitionCount, null, addedDeleteFiles, addedDataFiles);
  }

  protected void validateSnapshot(
      Snapshot snapshot,
      String operation,
      String changedPartitionCount,
      String deletedDataFiles,
      String addedDeleteFiles,
      String addedDataFiles) {
    Assert.assertEquals("Operation must match", operation, snapshot.operation());
    validateProperty(snapshot, CHANGED_PARTITION_COUNT_PROP, changedPartitionCount);
    validateProperty(snapshot, DELETED_FILES_PROP, deletedDataFiles);
    validateProperty(snapshot, ADDED_DELETE_FILES_PROP, addedDeleteFiles);
    validateProperty(snapshot, ADDED_FILES_PROP, addedDataFiles);
  }

  protected void validateProperty(Snapshot snapshot, String property, Set<String> expectedValues) {
    String actual = snapshot.summary().get(property);
    Assert.assertTrue(
        "Snapshot property "
            + property
            + " has unexpected value, actual = "
            + actual
            + ", expected one of : "
            + String.join(",", expectedValues),
        expectedValues.contains(actual));
  }

  protected void validateProperty(Snapshot snapshot, String property, String expectedValue) {
    String actual = snapshot.summary().get(property);
    Assert.assertEquals(
        "Snapshot property " + property + " has unexpected value.", expectedValue, actual);
  }

  protected void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected DataFile writeDataFile(Table table, List<GenericRecord> records) {
    try {
      OutputFile file = Files.localOutput(temp.newFile());

      DataWriter<GenericRecord> dataWriter =
          Parquet.writeData(file)
              .forTable(table)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .overwrite()
              .build();

      try {
        for (GenericRecord record : records) {
          dataWriter.write(record);
        }
      } finally {
        dataWriter.close();
      }

      return dataWriter.toDataFile();

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected String commitTarget() {
    return branch == null ? tableName : String.format("%s.branch_%s", tableName, branch);
  }

  @Override
  protected String selectTarget() {
    return branch == null ? tableName : String.format("%s VERSION AS OF '%s'", tableName, branch);
  }

  protected void createBranchIfNeeded() {
    if (branch != null && !branch.equals(SnapshotRef.MAIN_BRANCH)) {
      sql("ALTER TABLE %s CREATE BRANCH %s", tableName, branch);
    }
  }

  // ORC currently does not support vectorized reads with deletes
  protected boolean supportsVectorization() {
    return vectorized && (isParquet() || isCopyOnWrite());
  }

  private boolean isParquet() {
    return fileFormat.equalsIgnoreCase(FileFormat.PARQUET.name());
  }

  private boolean isCopyOnWrite() {
    return extraTableProperties().containsValue(RowLevelOperationMode.COPY_ON_WRITE.modeName());
  }

  protected void assertAllBatchScansVectorized(SparkPlan plan) {
    List<SparkPlan> batchScans = SparkPlanUtil.collectBatchScans(plan);
    assertThat(batchScans).hasSizeGreaterThan(0).allMatch(SparkPlan::supportsColumnar);
  }
}
