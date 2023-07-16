/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.TableProperties.WRITE_AUDIT_PUBLISH_ENABLED;
import static org.apache.iceberg.spark.SparkSQLProperties.WAP_ID;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Test;

public class TestFastForwardBranchProcedure extends SparkExtensionsTestBase {
  public TestFastForwardBranchProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testFastForwardBranchUsingPositionalArgs() {
    sql("CREATE TABLE %s (id int NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);
    spark.conf().set(WAP_ID, "111122223333");

    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    assertEquals(
        "Should not see rows from staged snapshot",
        ImmutableList.of(row(1, "a")),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot wapSnapshot = Iterables.getLast(table.snapshots());

    String newBranch = "new-branch-at-staged-snapshot";
    table.manageSnapshots().createBranch(newBranch, wapSnapshot.snapshotId()).commit();

    List<Object[]> output =
        sql(
            "CALL %s.system.fast_forward('%s', '%s', '%s')",
            catalogName, tableIdent, SnapshotRef.MAIN_BRANCH, newBranch);

    table.refresh();

    assertEquals(
        "Procedure output must match", ImmutableList.of(row(wapSnapshot.snapshotId())), output);

    assertEquals(
        "Fast-Forward must be successful",
        ImmutableList.of(row(1, "a"), row(2, "b")),
        sql("SELECT * FROM %s order by id", tableName));
  }

  @Test
  public void testFastForwardBranchUsingNamedArgs() {
    sql("CREATE TABLE %s (id int NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);
    spark.conf().set(WAP_ID, "111122223333");

    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    assertEquals(
        "Should not see rows from staged snapshot",
        ImmutableList.of(row(1, "a")),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot wapSnapshot = Iterables.getLast(table.snapshots());

    String newBranch = "new-branch-at-staged-snapshot";
    table.manageSnapshots().createBranch(newBranch, wapSnapshot.snapshotId()).commit();

    List<Object[]> output =
        sql(
            "CALL %s.system.fast_forward(table => '%s', branch => '%s', to => '%s')",
            catalogName, tableIdent, SnapshotRef.MAIN_BRANCH, newBranch);

    table.refresh();

    assertEquals(
        "Procedure output must match", ImmutableList.of(row(wapSnapshot.snapshotId())), output);

    assertEquals(
        "Fast-Forward must be successful",
        ImmutableList.of(row(1, "a"), row(2, "b")),
        sql("SELECT * FROM %s order by id", tableName));
  }

  @Test
  public void testFastForwardWhenTargetIsNotAncestorFails() {
    sql("CREATE TABLE %s (id int NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);
    spark.conf().set(WAP_ID, "111122223333");

    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    assertEquals(
        "Should not see rows from staged snapshot",
        ImmutableList.of(row(1, "a")),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    long wapSnapshotId = Iterables.getLast(table.snapshots()).snapshotId();

    GenericRecord record = GenericRecord.create(table.schema());
    record.set(0, 3);
    record.set(1, "c");

    // Commit a snapshot on main to deviate the branches
    DataFile dataFile = writeDataFile(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    String newBranch = "testBranch";
    table.manageSnapshots().createBranch(newBranch, wapSnapshotId).commit();

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.fast_forward(table => '%s', branch => '%s', to => '%s')",
                    catalogName, tableIdent, SnapshotRef.MAIN_BRANCH, newBranch))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot fast-forward: main is not an ancestor of testBranch");
  }

  @Test
  public void testInvalidFastForwardBranchCases() {

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.fast_forward('test_table', branch => 'main', to => 'newBranch')",
                    catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Named and positional arguments cannot be mixed");

    Assertions.assertThatThrownBy(
            () ->
                sql("CALL %s.custom.fast_forward('test_table', 'main', 'newBranch')", catalogName))
        .isInstanceOf(NoSuchProcedureException.class)
        .hasMessage("Procedure custom.fast_forward not found");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.fast_forward('test_table', 'main')", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Missing required parameters: [to]");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.fast_forward('', 'main', 'newBranch')", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot handle an empty identifier for argument table");
  }

  private DataFile writeDataFile(Table table, List<GenericRecord> records) {
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
}
