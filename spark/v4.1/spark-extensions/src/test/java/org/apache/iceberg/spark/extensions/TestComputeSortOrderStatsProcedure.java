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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestComputeSortOrderStatsProcedure extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void procedureOnEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);
    List<Object[]> result =
        sql("CALL %s.system.compute_sort_order_stats('%s')", catalogName, tableIdent);
    assertThat(result).isEmpty();
  }

  @TestTemplate
  public void procedureOnUnsortedTableFails() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    assertThatThrownBy(
            () -> sql("CALL %s.system.compute_sort_order_stats('%s')", catalogName, tableIdent))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("does not declare a sort order");
  }

  @TestTemplate
  public void overlappingUnpartitionedTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    // three appends with the same key: every file's [lower, upper] range contains id=50, so all
    // three files overlap regardless of how the rows of each insert are split into files
    sql("INSERT INTO TABLE %s VALUES (50, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (50, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (50, 'c')", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);

    List<Object[]> result =
        sql("CALL %s.system.compute_sort_order_stats('%s')", catalogName, tableIdent);
    assertThat(result).hasSize(1);
    Object[] row = result.get(0);
    assertThat(row[0]).isNull(); // unpartitioned
    assertThat(row[2]).isEqualTo(3); // file_count
    assertThat(row[3]).isEqualTo(0); // files_missing_bounds
    assertThat(row[4]).isEqualTo(3); // max_overlap_depth
  }

  @TestTemplate
  public void disjointFilesReportDepthOne() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (10, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (20, 'b')", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);

    List<Object[]> result =
        sql("CALL %s.system.compute_sort_order_stats('%s')", catalogName, tableIdent);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[4]).isEqualTo(1);
  }

  @TestTemplate
  public void partitionedTableReportsPerPartition() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (7, 'a'), (7, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (7, 'a')", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);

    List<Object[]> result =
        sql("CALL %s.system.compute_sort_order_stats(table => '%s')", catalogName, tableIdent);
    assertThat(result).hasSize(2);
    for (Object[] row : result) {
      String partition = row[0].toString();
      if (partition.contains("data=a")) {
        assertThat(row[2]).isEqualTo(2);
        assertThat(row[4]).isEqualTo(2); // both files contain id=7 -> ranges overlap
      } else {
        assertThat(row[2]).isEqualTo(1);
        assertThat(row[4]).isEqualTo(1);
      }
    }
  }

  @TestTemplate
  public void snapshotIdArgSelectsOlderState() throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (50, 'a')", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    sql("INSERT INTO TABLE %s VALUES (50, 'b')", tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);

    List<Object[]> current =
        sql("CALL %s.system.compute_sort_order_stats('%s')", catalogName, tableIdent);
    assertThat(current.get(0)[4]).isEqualTo(2);

    List<Object[]> old =
        sql(
            "CALL %s.system.compute_sort_order_stats(table => '%s', snapshot_id => %d)",
            catalogName, tableIdent, firstSnapshotId);
    assertThat(old.get(0)[4]).isEqualTo(1);
  }
}
