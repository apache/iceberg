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
package org.apache.iceberg.spark.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestDeleteFrom extends CatalogTestBase {
  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testDeleteFromUnpartitionedTable() throws NoSuchTableException {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.coalesce(1).writeTo(tableName).append();

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DELETE FROM %s WHERE id < 2", tableName);

    assertEquals(
        "Should have no rows after successful delete",
        ImmutableList.of(row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DELETE FROM %s WHERE id < 4", tableName);

    assertEquals(
        "Should have no rows after successful delete",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testDeleteFromTableAtSnapshot() throws NoSuchTableException {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.coalesce(1).writeTo(tableName).append();

    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    String prefix = "snapshot_id_";
    assertThatThrownBy(() -> sql("DELETE FROM %s.%s WHERE id < 4", tableName, prefix + snapshotId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot delete from table at a specific snapshot");
  }

  @TestTemplate
  public void testDeleteFromPartitionedTable() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id bigint, data string) "
            + "USING iceberg "
            + "PARTITIONED BY (truncate(id, 2))",
        tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.coalesce(1).writeTo(tableName).append();

    assertEquals(
        "Should have 3 rows in 2 partitions",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DELETE FROM %s WHERE id > 2", tableName);
    assertEquals(
        "Should have two rows in the second partition",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DELETE FROM %s WHERE id < 2", tableName);

    assertEquals(
        "Should have two rows in the second partition",
        ImmutableList.of(row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void metadataDeleteRetainsUnknownRows() {
    assertMetadataDeleteWithNulls("", "data <> 'a1'");
  }

  @TestTemplate
  public void partitionDeleteRetainsUnknownRows() {
    assertMetadataDeleteWithNulls("PARTITIONED BY (data)", "data NOT IN ('a1')");
  }

  @TestTemplate
  public void metadataDeleteWithNegatedPrefix() {
    assertMetadataDeleteWithNulls("", "data NOT LIKE 'a%'");
  }

  @TestTemplate
  public void metadataDeleteWithNegatedNullSafeEquality() {
    assertMetadataDeleteWithNulls("", "NOT (data <=> 'a1')");
  }

  @TestTemplate
  public void metadataDeleteWithNegatedNullCheck() {
    assertMetadataDeleteWithNulls("", "NOT (data IS NOT NULL)");
  }

  @TestTemplate
  public void rowLevelDeleteRetainsUnknownRowsInMixedFile() throws NoSuchTableException {
    String predicate = "data <> 'a1'";
    List<Object[]> expected =
        sql(
            "SELECT id FROM VALUES (1L, 'a1'), (2L, 'b'), (3L, CAST(NULL AS STRING)) "
                + "AS source(id, data) WHERE (%s) IS NOT TRUE ORDER BY id",
            predicate);

    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('%s'='2', '%s'='%s')",
        tableName,
        TableProperties.FORMAT_VERSION,
        TableProperties.DELETE_MODE,
        RowLevelOperationMode.COPY_ON_WRITE.modeName());
    List<SimpleRecord> records =
        ImmutableList.of(
            new SimpleRecord(1, "a1"), new SimpleRecord(2, "b"), new SimpleRecord(3, null));
    spark.createDataFrame(records, SimpleRecord.class).coalesce(1).writeTo(tableName).append();
    assertThat(scalarSql("SELECT count(*) FROM %s.files", tableName)).isEqualTo(1L);

    sql("DELETE FROM %s WHERE %s", tableName, predicate);

    assertEquals(
        "Row-level delete should retain rows where the predicate is false or unknown",
        expected,
        sql("SELECT id FROM %s ORDER BY id", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.OVERWRITE);
    assertThat(table.currentSnapshot().summary())
        .containsEntry(SnapshotSummary.DELETED_FILES_PROP, "1");
  }

  private void assertMetadataDeleteWithNulls(String partitioning, String predicate) {
    List<Object[]> expected =
        sql(
            "SELECT id FROM VALUES (1L, 'a1'), (2L, 'b'), (3L, CAST(NULL AS STRING)) "
                + "AS source(id, data) WHERE (%s) IS NOT TRUE ORDER BY id",
            predicate);

    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg %s TBLPROPERTIES ('%s'='2')",
        tableName, partitioning, TableProperties.FORMAT_VERSION);
    sql("INSERT INTO %s VALUES (1L, 'a1')", tableName);
    sql("INSERT INTO %s VALUES (2L, 'b')", tableName);
    sql("INSERT INTO %s VALUES (3L, CAST(NULL AS STRING))", tableName);

    long originalFiles = (Long) scalarSql("SELECT count(*) FROM %s.files", tableName);
    assertThat(originalFiles).isEqualTo(3L);

    sql("DELETE FROM %s WHERE %s", tableName, predicate);

    assertEquals(
        "Delete should retain rows where the predicate is false or unknown",
        expected,
        sql("SELECT id FROM %s ORDER BY id", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);
    assertThat(table.currentSnapshot().summary())
        .containsEntry(
            SnapshotSummary.DELETED_FILES_PROP, String.valueOf(originalFiles - expected.size()));
  }

  @TestTemplate
  public void testDeleteFromWhereFalse() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).as("Should have 1 snapshot").hasSize(1);

    sql("DELETE FROM %s WHERE false", tableName);

    table.refresh();

    assertThat(table.snapshots()).as("Delete should not produce a new snapshot").hasSize(1);
  }

  @TestTemplate
  public void testTruncate() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).as("Should have 1 snapshot").hasSize(1);

    sql("TRUNCATE TABLE %s", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testDeleteFromTablePartitionedByVarbinary() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data binary) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES(1, X'e3bcd1'), (2, X'bcd1')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, new byte[] {-29, -68, -47}), row(2L, new byte[] {-68, -47})),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DELETE FROM %s WHERE data = X'bcd1'", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, new byte[] {-29, -68, -47})),
        sql("SELECT * FROM %s", tableName));
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, new byte[] {-29, -68, -47})),
        sql("SELECT * FROM %s where data = X'e3bcd1'", tableName));
  }
}
