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

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestComputeTableStatsProcedure extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testProcedureOnEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    List<Object[]> result =
        sql("CALL %s.system.compute_table_stats('%s')", catalogName, tableIdent);
    Assertions.assertTrue(result.isEmpty());
  }

  @TestTemplate
  public void testProcedureWithNamedArgs() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);
    List<Object[]> output =
        sql(
            "CALL %s.system.compute_table_stats(table => '%s', columns => array('id'))",
            catalogName, tableIdent);
    Assertions.assertNotNull(output.get(0));
  }

  @TestTemplate
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
    Assertions.assertNotNull(output.get(0));
  }

  @TestTemplate
  public void testProcedureWithInvalidColumns() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                sql(
                    "CALL %s.system.compute_table_stats(table => '%s', columns => array('id1'))",
                    catalogName, tableIdent));
    Assertions.assertTrue(illegalArgumentException.getMessage().contains("Can't find column id1"));
  }

  @TestTemplate
  public void testProcedureWithInvalidSnapshot() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                sql(
                    "CALL %s.system.compute_table_stats(table => '%s', snapshot_id => %dL)",
                    catalogName, tableIdent, 1234L));
    Assertions.assertTrue(illegalArgumentException.getMessage().contains("Snapshot not found"));
  }
}
