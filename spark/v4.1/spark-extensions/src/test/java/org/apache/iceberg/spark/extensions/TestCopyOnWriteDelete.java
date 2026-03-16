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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestCopyOnWriteDelete extends TestDelete {

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
  }

  @TestTemplate
  public void testRuntimeFilteringWithPreservedDataGrouping() throws NoSuchTableException {
    createAndInitPartitionedTable();

    append(tableName, new Employee(1, "hr"), new Employee(3, "hr"));
    createBranchIfNeeded();
    append(new Employee(1, "hardware"), new Employee(2, "hardware"));

    Map<String, String> sqlConf =
        ImmutableMap.of(
            SQLConf.V2_BUCKETING_ENABLED().key(),
            "true",
            SparkSQLProperties.PRESERVE_DATA_GROUPING,
            "true");

    withSQLConf(sqlConf, () -> sql("DELETE FROM %s WHERE id = 2", commitTarget()));

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).as("Should have 3 snapshots").hasSize(3);

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    validateCopyOnWrite(currentSnapshot, "1", "1", "1");

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hardware"), row(1, "hr"), row(3, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", selectTarget()));
  }

  @TestTemplate
  public void testEqualityDeletePreservation() throws NoSuchTableException, IOException {
    createAndInitPartitionedTable();
    append(tableName, new Employee(1, "hr"), new Employee(2, "hr"), new Employee(3, "hr"));

    Table table = validationCatalog.loadTable(tableIdent);
    OutputFile out = Files.localOutput(File.createTempFile("junit", null, temp.toFile()));
    Schema deleteSchema = table.schema().select("id");
    GenericRecord deleteRecord = GenericRecord.create(deleteSchema);
    DeleteFile eqDelete =
        FileHelpers.writeDeleteFile(
            table,
            out,
            TestHelpers.Row.of("hr"),
            List.of(deleteRecord.copy("id", 2)),
            deleteSchema);

    table.newRowDelta().addDeletes(eqDelete).commit();

    sql("REFRESH TABLE %s", tableName);

    assertEquals(
        "Equality delete should remove row with id 2",
        ImmutableList.of(row(1, "hr"), row(3, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));

    sql("DELETE FROM %s WHERE id = 3", tableName);

    assertEquals(
        "COW Delete should remove row with id 3",
        ImmutableList.of(row(1, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));
  }
}
