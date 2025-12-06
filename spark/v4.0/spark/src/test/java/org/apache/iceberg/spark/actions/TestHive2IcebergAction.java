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
package org.apache.iceberg.spark.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Hive2Iceberg;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestHive2IcebergAction extends CatalogTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testHive2IcebergPartitionTable() throws NoSuchTableException, ParseException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    createPartitionedTable();
    sql("INSERT INTO TABLE %s VALUES (1, 'a', 'p1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b', 'p2')", tableName);

    Hive2Iceberg.Result result = SparkActions.get().hive2Iceberg(tableName).execute();
    assertThat(result).as("Hive2Iceberg action result should not be null").isNotNull();
    assertThat(result.successful()).as("Hive2Iceberg action should be successful").isTrue();
    assertThat(result.failureMessage()).as("Failure message should be 'N/A'").isEqualTo("N/A");
    assertThat(result.latestVersion())
        .as("Latest version of Iceberg metadata should not be null")
        .isNotNull();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    String metadataLocation =
        ((HiveTableOperations) ((BaseTable) table).operations()).currentMetadataLocation();
    String metadataLocationFileName = RewriteTablePathUtil.fileName(metadataLocation);
    assertThat(metadataLocationFileName)
        .as("Iceberg metadata file name should match the latest version from Hive2Iceberg result")
        .isEqualTo(result.latestVersion());

    List<Object[]> expected = sql("select * from %s.partitions", tableName);
    assertThat(expected).as("Migrated Iceberg table should have exactly 2 partitions").hasSize(2);
  }

  private void createPartitionedTable() {
    sql("CREATE TABLE %s (c1 string, c2 string, c3 string) PARTITIONED BY (c1)", tableName);
  }
}
