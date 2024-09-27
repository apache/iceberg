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

import java.util.List;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRegisterTableProcedure extends ExtensionsTestBase {

  private String targetName;

  @BeforeEach
  public void setTargetName() {
    targetName = tableName("register_table");
  }

  @AfterEach
  public void dropTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", targetName);
  }

  @TestTemplate
  public void testRegisterTable() throws NoSuchTableException, ParseException {
    long numRows = 1000;

    sql("CREATE TABLE %s (id int, data string) using ICEBERG", tableName);
    spark
        .range(0, numRows)
        .withColumn("data", functions.col("id").cast(DataTypes.StringType))
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    long originalFileCount = (long) scalarSql("SELECT COUNT(*) from %s.files", tableName);
    long currentSnapshotId = table.currentSnapshot().snapshotId();
    String metadataJson =
        (((HasTableOperations) table).operations()).current().metadataFileLocation();

    List<Object[]> result =
        sql("CALL %s.system.register_table('%s', '%s')", catalogName, targetName, metadataJson);
    assertThat(result.get(0)[0]).as("Current Snapshot is not correct").isEqualTo(currentSnapshotId);

    List<Object[]> original = sql("SELECT * FROM %s", tableName);
    List<Object[]> registered = sql("SELECT * FROM %s", targetName);
    assertEquals("Registered table rows should match original table rows", original, registered);
    assertThat(result.get(0)[1])
        .as("Should have the right row count in the procedure result")
        .isEqualTo(numRows);
    assertThat(result.get(0)[2])
        .as("Should have the right datafile count in the procedure result")
        .isEqualTo(originalFileCount);
  }
}
