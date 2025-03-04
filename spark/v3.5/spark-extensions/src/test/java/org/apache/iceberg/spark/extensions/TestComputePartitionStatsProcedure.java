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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestComputePartitionStatsProcedure extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testProcedureOnEmptyTable() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    List<Object[]> result =
        sql("CALL %s.system.compute_partition_stats('%s')", catalogName, tableIdent);
    assertThat(result).isEmpty();
  }

  @TestTemplate
  public void testProcedureWithPositionalArgs() throws NoSuchTableException, ParseException {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);
    List<Object[]> output =
        sql("CALL %s.system.compute_partition_stats('%s')", catalogName, tableIdent);
    assertThat(output.get(0)).isNotEmpty();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    assertThat(table.partitionStatisticsFiles()).hasSize(1);
    assertThat(table.partitionStatisticsFiles().get(0).path())
        .isEqualTo(output.get(0)[0].toString());
    assertThat(table.partitionStatisticsFiles().get(0).snapshotId())
        .isEqualTo(table.currentSnapshot().snapshotId());
  }

  @TestTemplate
  public void testProcedureWithNamedArgs() throws NoSuchTableException, ParseException {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);
    sql("ALTER TABLE %s CREATE BRANCH `b1`", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Snapshot snapshotBranch = table.currentSnapshot();
    sql("INSERT INTO TABLE %s VALUES (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h')", tableName);

    List<Object[]> output =
        sql(
            "CALL %s.system.compute_partition_stats(table => '%s', branch => 'b1')",
            catalogName, tableIdent);
    table.refresh();
    assertThat(output.get(0)).isNotEmpty();
    assertThat(table.partitionStatisticsFiles()).hasSize(1);
    assertThat(table.partitionStatisticsFiles().get(0).path())
        .isEqualTo(output.get(0)[0].toString());
    // should be from the branch instead of latest snapshot of the table
    assertThat(table.partitionStatisticsFiles().get(0).snapshotId())
        .isEqualTo(snapshotBranch.snapshotId());
  }

  @TestTemplate
  public void testProcedureWithInvalidBranch() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.compute_partition_stats(table => '%s', branch => 'invalid')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Couldn't find the snapshot for the branch invalid");
  }

  @TestTemplate
  public void testProcedureWithInvalidTable() {
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.compute_partition_stats(table => '%s')",
                    catalogName, TableIdentifier.of(Namespace.of("default"), "abcd")))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Couldn't load table");
  }
}
