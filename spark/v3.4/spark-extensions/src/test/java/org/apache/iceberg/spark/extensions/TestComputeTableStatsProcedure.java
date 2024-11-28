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
import java.util.Map;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.NDVSketchUtil;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.After;
import org.junit.Test;

public class TestComputeTableStatsProcedure extends SparkExtensionsTestBase {

  public TestComputeTableStatsProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testProcedureOnEmptyTable() throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    List<Object[]> result =
        sql("CALL %s.system.compute_table_stats('%s')", catalogName, tableIdent);
    assertThat(result).isEmpty();
  }

  @Test
  public void testProcedureWithNamedArgs() throws NoSuchTableException, ParseException {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);
    List<Object[]> output =
        sql(
            "CALL %s.system.compute_table_stats(table => '%s', columns => array('id'))",
            catalogName, tableIdent);
    assertThat(output.get(0)).isNotEmpty();
    Object obj = output.get(0)[0];
    assertThat(obj.toString()).endsWith(".stats");
    verifyTableStats(tableName);
  }

  @Test
  public void testProcedureWithPositionalArgs() throws NoSuchTableException, ParseException {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Snapshot snapshot = table.currentSnapshot();
    List<Object[]> output =
        sql(
            "CALL %s.system.compute_table_stats('%s', %dL)",
            catalogName, tableIdent, snapshot.snapshotId());
    assertThat(output.get(0)).isNotEmpty();
    Object obj = output.get(0)[0];
    assertThat(obj.toString()).endsWith(".stats");
    verifyTableStats(tableName);
  }

  @Test
  public void testProcedureWithInvalidColumns() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.compute_table_stats(table => '%s', columns => array('id1'))",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Can't find column id1");
  }

  @Test
  public void testProcedureWithInvalidSnapshot() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.compute_table_stats(table => '%s', snapshot_id => %dL)",
                    catalogName, tableIdent, 1234L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Snapshot not found");
  }

  @Test
  public void testProcedureWithInvalidTable() {
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.compute_table_stats(table => '%s', snapshot_id => %dL)",
                    catalogName, TableIdentifier.of(Namespace.of("default"), "abcd"), 1234L))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Couldn't load table");
  }

  void verifyTableStats(String tableName) throws NoSuchTableException, ParseException {
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    StatisticsFile statisticsFile = table.statisticsFiles().get(0);
    BlobMetadata blobMetadata = statisticsFile.blobMetadata().get(0);
    assertThat(blobMetadata.properties())
        .containsKey(NDVSketchUtil.APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY);
  }
}
