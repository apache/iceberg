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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestConflictValidation extends ExtensionsTestBase {

  @BeforeEach
  public void createTables() {
    sql(
        "CREATE TABLE %s (id int, data string) USING iceberg "
            + "PARTITIONED BY (id)"
            + "TBLPROPERTIES"
            + "('format-version'='2',"
            + "'write.delete.mode'='merge-on-read')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testOverwriteFilterSerializableIsolation() throws Exception {
    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "a"));
    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();

    // Validating from previous snapshot finds conflicts
    Dataset<Row> conflictingDf = spark.createDataFrame(records, SimpleRecord.class);
    assertThatThrownBy(
            () ->
                conflictingDf
                    .writeTo(tableName)
                    .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
                    .option(
                        SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
                    .overwrite(functions.col("id").equalTo(1)))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Found conflicting files that can contain records matching ref(name=\"id\") == 1:");

    // Validating from latest snapshot should succeed
    table.refresh();
    long newSnapshotId = table.currentSnapshot().snapshotId();
    conflictingDf
        .writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(newSnapshotId))
        .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
        .overwrite(functions.col("id").equalTo(1));
  }

  @TestTemplate
  public void testOverwriteFilterSerializableIsolation2() throws Exception {
    List<SimpleRecord> records =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(1, "b"));
    spark.createDataFrame(records, SimpleRecord.class).coalesce(1).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    // This should add a delete file
    sql("DELETE FROM %s WHERE id='1' and data='b'", tableName);
    table.refresh();

    // Validating from previous snapshot finds conflicts
    List<SimpleRecord> conflictingRecords = Lists.newArrayList(new SimpleRecord(1, "a"));
    Dataset<Row> conflictingDf = spark.createDataFrame(conflictingRecords, SimpleRecord.class);
    assertThatThrownBy(
            () ->
                conflictingDf
                    .writeTo(tableName)
                    .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
                    .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SNAPSHOT.toString())
                    .overwrite(functions.col("id").equalTo(1)))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Found new conflicting delete files that can apply to records matching ref(name=\"id\") == 1:");

    // Validating from latest snapshot should succeed
    table.refresh();
    long newSnapshotId = table.currentSnapshot().snapshotId();
    conflictingDf
        .writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(newSnapshotId))
        .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
        .overwrite(functions.col("id").equalTo(1));
  }

  @TestTemplate
  public void testOverwriteFilterSerializableIsolation3() throws Exception {
    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    // This should delete a data file
    sql("DELETE FROM %s WHERE id='1'", tableName);
    table.refresh();

    // Validating from previous snapshot finds conflicts
    List<SimpleRecord> conflictingRecords = Lists.newArrayList(new SimpleRecord(1, "a"));
    Dataset<Row> conflictingDf = spark.createDataFrame(conflictingRecords, SimpleRecord.class);
    assertThatThrownBy(
            () ->
                conflictingDf
                    .writeTo(tableName)
                    .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
                    .option(
                        SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
                    .overwrite(functions.col("id").equalTo(1)))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Found conflicting deleted files that can contain records matching ref(name=\"id\") == 1:");

    // Validating from latest snapshot should succeed
    table.refresh();
    long newSnapshotId = table.currentSnapshot().snapshotId();
    conflictingDf
        .writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(newSnapshotId))
        .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
        .overwrite(functions.col("id").equalTo(1));
  }

  @TestTemplate
  public void testOverwriteFilterNoSnapshotIdValidation() throws Exception {
    Table table = validationCatalog.loadTable(tableIdent);

    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "a"));
    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();

    // Validating from no snapshot id defaults to beginning snapshot id and finds conflicts
    Dataset<Row> conflictingDf = spark.createDataFrame(records, SimpleRecord.class);

    assertThatThrownBy(
            () ->
                conflictingDf
                    .writeTo(tableName)
                    .option(
                        SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
                    .overwrite(functions.col("id").equalTo(1)))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Found conflicting files that can contain records matching ref(name=\"id\") == 1:");

    // Validating from latest snapshot should succeed
    table.refresh();
    long newSnapshotId = table.currentSnapshot().snapshotId();
    conflictingDf
        .writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(newSnapshotId))
        .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
        .overwrite(functions.col("id").equalTo(1));
  }

  @TestTemplate
  public void testOverwriteFilterSnapshotIsolation() throws Exception {
    List<SimpleRecord> records =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(1, "b"));
    spark.createDataFrame(records, SimpleRecord.class).coalesce(1).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    // This should add a delete file
    sql("DELETE FROM %s WHERE id='1' and data='b'", tableName);
    table.refresh();

    // Validating from previous snapshot finds conflicts
    List<SimpleRecord> conflictingRecords = Lists.newArrayList(new SimpleRecord(1, "a"));
    Dataset<Row> conflictingDf = spark.createDataFrame(conflictingRecords, SimpleRecord.class);
    assertThatThrownBy(
            () ->
                conflictingDf
                    .writeTo(tableName)
                    .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
                    .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SNAPSHOT.toString())
                    .overwrite(functions.col("id").equalTo(1)))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Found new conflicting delete files that can apply to records matching ref(name=\"id\") == 1:");

    // Validating from latest snapshot should succeed
    table.refresh();
    long newSnapshotId = table.currentSnapshot().snapshotId();
    conflictingDf
        .writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(newSnapshotId))
        .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SNAPSHOT.toString())
        .overwrite(functions.col("id").equalTo(1));
  }

  @TestTemplate
  public void testOverwriteFilterSnapshotIsolation2() throws Exception {
    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "a"));
    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();

    // Validation should not fail due to conflicting data file in snapshot isolation mode
    Dataset<Row> conflictingDf = spark.createDataFrame(records, SimpleRecord.class);
    conflictingDf
        .writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
        .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SNAPSHOT.toString())
        .overwrite(functions.col("id").equalTo(1));
  }

  @TestTemplate
  public void testOverwritePartitionSerializableIsolation() throws Exception {
    Table table = validationCatalog.loadTable(tableIdent);
    final long snapshotId = table.currentSnapshot().snapshotId();

    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "a"));
    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();

    // Validating from previous snapshot finds conflicts
    Dataset<Row> conflictingDf = spark.createDataFrame(records, SimpleRecord.class);
    assertThatThrownBy(
            () ->
                conflictingDf
                    .writeTo(tableName)
                    .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
                    .option(
                        SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
                    .overwritePartitions())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Found conflicting files that can contain records matching partitions [id=1]");

    // Validating from latest snapshot should succeed
    table.refresh();
    long newSnapshotId = table.currentSnapshot().snapshotId();
    conflictingDf
        .writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(newSnapshotId))
        .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
        .overwritePartitions();
  }

  @TestTemplate
  public void testOverwritePartitionSnapshotIsolation() throws Exception {
    List<SimpleRecord> records =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(1, "b"));
    spark.createDataFrame(records, SimpleRecord.class).coalesce(1).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    final long snapshotId = table.currentSnapshot().snapshotId();

    // This should generate a delete file
    sql("DELETE FROM %s WHERE data='a'", tableName);

    // Validating from previous snapshot finds conflicts
    Dataset<Row> conflictingDf = spark.createDataFrame(records, SimpleRecord.class);
    assertThatThrownBy(
            () ->
                conflictingDf
                    .writeTo(tableName)
                    .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
                    .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SNAPSHOT.toString())
                    .overwritePartitions())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Found new conflicting delete files that can apply to records matching [id=1]");

    // Validating from latest snapshot should succeed
    table.refresh();
    long newSnapshotId = table.currentSnapshot().snapshotId();
    conflictingDf
        .writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(newSnapshotId))
        .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SNAPSHOT.toString())
        .overwritePartitions();
  }

  @TestTemplate
  public void testOverwritePartitionSnapshotIsolation2() throws Exception {
    Table table = validationCatalog.loadTable(tableIdent);
    final long snapshotId = table.currentSnapshot().snapshotId();

    // This should delete a data file
    sql("DELETE FROM %s WHERE id='1'", tableName);

    // Validating from previous snapshot finds conflicts
    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "a"));
    spark.createDataFrame(records, SimpleRecord.class).coalesce(1).writeTo(tableName).append();
    Dataset<Row> conflictingDf = spark.createDataFrame(records, SimpleRecord.class);

    assertThatThrownBy(
            () ->
                conflictingDf
                    .writeTo(tableName)
                    .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
                    .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SNAPSHOT.toString())
                    .overwritePartitions())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Found conflicting deleted files that can apply to records matching [id=1]");

    // Validating from latest snapshot should succeed
    table.refresh();
    long newSnapshotId = table.currentSnapshot().snapshotId();
    conflictingDf
        .writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(newSnapshotId))
        .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SNAPSHOT.toString())
        .overwritePartitions();
  }

  @TestTemplate
  public void testOverwritePartitionSnapshotIsolation3() throws Exception {
    Table table = validationCatalog.loadTable(tableIdent);
    final long snapshotId = table.currentSnapshot().snapshotId();

    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "a"));
    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();

    // Validation should not find conflicting data file in snapshot isolation mode
    Dataset<Row> conflictingDf = spark.createDataFrame(records, SimpleRecord.class);
    conflictingDf
        .writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
        .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SNAPSHOT.toString())
        .overwritePartitions();
  }

  @TestTemplate
  public void testOverwritePartitionNoSnapshotIdValidation() throws Exception {
    Table table = validationCatalog.loadTable(tableIdent);

    List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "a"));
    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();

    // Validating from null snapshot is equivalent to validating from beginning
    Dataset<Row> conflictingDf = spark.createDataFrame(records, SimpleRecord.class);
    assertThatThrownBy(
            () ->
                conflictingDf
                    .writeTo(tableName)
                    .option(
                        SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
                    .overwritePartitions())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Found conflicting files that can contain records matching partitions [id=1]");

    // Validating from latest snapshot should succeed
    table.refresh();
    long snapshotId = table.currentSnapshot().snapshotId();
    conflictingDf
        .writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
        .option(SparkWriteOptions.ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
        .overwritePartitions();
  }
}
