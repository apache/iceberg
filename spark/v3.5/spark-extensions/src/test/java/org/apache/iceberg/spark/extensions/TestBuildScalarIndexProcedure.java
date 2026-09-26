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
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexIdentifier;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkIndexCatalogs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBuildScalarIndexProcedure extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testBuildHashIndexOnStringColumn() throws Exception {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, 'aaa'), (2, 'bbb'), (3, 'ccc'), (4, 'ddd'), (5, 'eee')",
        tableName);

    List<Object[]> output =
        sql(
            "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
                + " transform => 'HASH')",
            catalogName, tableIdent);

    assertThat(output).hasSize(1);
    Object[] row = output.get(0);
    // index_location, leaf_file_count, record_count
    assertThat((String) row[0]).isNotBlank();
    assertThat((int) row[1]).isGreaterThan(0);
    assertThat((long) row[2]).isEqualTo(5L);

    // Loaded through the Spark catalog (matching how BuildScalarIndexProcedure and
    // SparkScanBuilder both load it), not validationCatalog -- validationCatalog is a separate
    // Catalog handle configured with its own catalog name, which table.name() embeds, so a table
    // loaded through it produces a different TableIdentifier than one loaded through Spark for
    // the same physical table.
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    IndexCatalog indexCatalog = SparkIndexCatalogs.get().catalogFor(table);
    IndexIdentifier indexIdent =
        IndexIdentifier.of(
            org.apache.iceberg.catalog.TableIdentifier.parse(table.name()), "data_idx");
    assertThat(indexCatalog.indexExists(indexIdent)).isTrue();

    IndexMetadata metadata = indexCatalog.loadIndex(indexIdent);
    assertThat(metadata.type()).isEqualTo("SCALAR");
    assertThat(metadata.transformFunction()).isEqualTo("HASH");
    assertThat(metadata.snapshots()).hasSize(1);
  }

  @TestTemplate
  public void testBuildIdentityIndexOnLongColumn() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    List<Object[]> output =
        sql(
            "CALL %s.system.build_scalar_index(table => '%s', columns => array('id'),"
                + " transform => 'IDENTITY')",
            catalogName, tableIdent);

    assertThat(output).hasSize(1);
    assertThat((long) output.get(0)[2]).isEqualTo(3L);
  }

  @TestTemplate
  public void testIdentityRejectsStringColumn() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
                        + " transform => 'IDENTITY')",
                    catalogName, tableIdent))
        .hasMessageContaining("IDENTITY transform requires a numeric");
  }

  @TestTemplate
  public void testRejectsMultipleColumns() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_scalar_index(table => '%s', columns => array('id',"
                        + " 'data'), transform => 'HASH')",
                    catalogName, tableIdent))
        .hasMessageContaining("supports exactly one key column");
  }

  @TestTemplate
  public void testCustomBucketCountOption() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
        tableName);

    List<Object[]> output =
        sql(
            "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
                + " transform => 'HASH', options => map('hash.num-buckets', '8',"
                + " 'target-leaf-files', '2'))",
            catalogName, tableIdent);

    assertThat((long) output.get(0)[2]).isEqualTo(4L);
  }

  @TestTemplate
  public void testBuildOnPartitionColumnWarnsButDoesNotReject() {
    // Redundant (partitioning already prunes this column) but not wrong -- the index build must
    // still succeed, matching the "always advisory, never required, never rejected" design.
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b')", tableName);

    List<Object[]> output =
        sql(
            "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
                + " transform => 'HASH')",
            catalogName, tableIdent);

    assertThat((long) output.get(0)[2]).isEqualTo(2L);
  }

  @TestTemplate
  public void testIncrementalBuildIndexesOnlyNewRows() throws Exception {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'aaa'), (2, 'bbb')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
            + " transform => 'HASH')",
        catalogName, tableIdent);

    // A new commit after the full build -- the incremental build should only process this row,
    // not re-scan the whole table.
    sql("INSERT INTO TABLE %s VALUES (3, 'ccc')", tableName);

    List<Object[]> output =
        sql(
            "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
                + " transform => 'HASH', options => map('mode', 'incremental'))",
            catalogName, tableIdent);

    assertThat(output).hasSize(1);
    // record_count reported by an incremental build is the running total (existing + new), not
    // just the newly indexed rows, matching what a full rebuild would report for the same table.
    assertThat((long) output.get(0)[2]).isEqualTo(3L);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    IndexCatalog indexCatalog = SparkIndexCatalogs.get().catalogFor(table);
    IndexIdentifier indexIdent =
        IndexIdentifier.of(
            org.apache.iceberg.catalog.TableIdentifier.parse(table.name()), "data_idx");
    IndexMetadata metadata = indexCatalog.loadIndex(indexIdent);
    // One snapshot from the full build, one from the incremental build.
    assertThat(metadata.snapshots()).hasSize(2);
    // The incremental commit refreshed the index to exactly the current table snapshot, so the
    // read side resolves this without needing the covered/uncovered-files staleness fallback.
    assertThat(metadata.currentSnapshot().sourceTableSnapshotId())
        .isEqualTo(table.currentSnapshot().snapshotId());

    // Both the original and the newly-indexed row resolve correctly.
    List<Object[]> original = sql("SELECT id FROM %s WHERE data = 'aaa'", tableName);
    assertThat(original).hasSize(1);
    assertThat(original.get(0)[0]).isEqualTo(1L);

    List<Object[]> added = sql("SELECT id FROM %s WHERE data = 'ccc'", tableName);
    assertThat(added).hasSize(1);
    assertThat(added.get(0)[0]).isEqualTo(3L);
  }

  @TestTemplate
  public void testIncrementalBuildFallsBackToFullWhenNoExistingIndex() {
    // No prior build_scalar_index call for this column -- "incremental" mode has nothing to
    // build on, so this must silently behave like a full build rather than fail.
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'aaa'), (2, 'bbb')", tableName);

    List<Object[]> output =
        sql(
            "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
                + " transform => 'HASH', options => map('mode', 'incremental'))",
            catalogName, tableIdent);

    assertThat((long) output.get(0)[2]).isEqualTo(2L);
  }

  @TestTemplate
  public void testIncrementalBuildNoOpWhenAlreadyFresh() throws Exception {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'aaa'), (2, 'bbb')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
            + " transform => 'HASH')",
        catalogName, tableIdent);

    // No new commits since the full build -- the index is already fresh, so this should be a
    // no-op rather than create a redundant new index snapshot.
    List<Object[]> output =
        sql(
            "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
                + " transform => 'HASH', options => map('mode', 'incremental'))",
            catalogName, tableIdent);

    assertThat((long) output.get(0)[2]).isEqualTo(2L);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    IndexCatalog indexCatalog = SparkIndexCatalogs.get().catalogFor(table);
    IndexIdentifier indexIdent =
        IndexIdentifier.of(
            org.apache.iceberg.catalog.TableIdentifier.parse(table.name()), "data_idx");
    assertThat(indexCatalog.loadIndex(indexIdent).snapshots()).hasSize(1);
  }
}
