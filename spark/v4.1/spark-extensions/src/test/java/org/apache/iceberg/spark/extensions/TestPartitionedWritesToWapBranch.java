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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.sql.PartitionedWritesTestBase;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestPartitionedWritesToWapBranch extends PartitionedWritesTestBase {

  private static final String BRANCH = "test";

  @BeforeAll
  public static void startMetastoreAndSpark() {
    TestBase.metastore = new TestHiveMetastore();
    metastore.start();
    TestBase.hiveConf = metastore.hiveConf();

    TestBase.spark.stop();

    TestBase.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.testing", "true")
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.hive.metastorePartitionPruningFallbackOnException", "true")
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .enableHiveSupport()
            .getOrCreate();

    TestBase.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

    String className = HiveCatalog.class.getName();
    Map<String, String> options = ImmutableMap.of();
    TestBase.catalog = (HiveCatalog) CatalogUtil.loadCatalog(className, "hive", options, hiveConf);
  }

  @BeforeEach
  @Override
  public void createTables() {
    spark.conf().set(SparkSQLProperties.WAP_BRANCH, BRANCH);
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg PARTITIONED BY (truncate(id, 3)) OPTIONS (%s = 'true')",
        tableName, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
  }

  @AfterEach
  @Override
  public void removeTables() {
    super.removeTables();
    spark.conf().unset(SparkSQLProperties.WAP_BRANCH);
    spark.conf().unset(SparkSQLProperties.WAP_ID);
  }

  @Override
  protected String selectTarget() {
    return String.format("%s VERSION AS OF '%s'", tableName, BRANCH);
  }

  @TestTemplate
  public void testWriteToBranchWithWapBranchSet() {
    Table table = validationCatalog.loadTable(tableIdent);
    table.manageSnapshots().createBranch("test2", table.refs().get(BRANCH).snapshotId()).commit();
    sql("REFRESH TABLE " + tableName);

    // writing to explicit branch should succeed even with WAP branch set
    sql("INSERT INTO TABLE %s.branch_test2 VALUES (4, 'd')", tableName);

    // verify write went to branch test2
    assertEquals(
        "Data should be written to branch test2",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d")),
        sql("SELECT * FROM %s VERSION AS OF 'test2' ORDER BY id", tableName));

    // verify current state is not affected
    assertEquals(
        "Data should be written to branch test2",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testWapIdAndWapBranchCannotBothBeSetForWrite() {
    String wapId = UUID.randomUUID().toString();
    spark.conf().set(SparkSQLProperties.WAP_ID, wapId);
    assertThatThrownBy(() -> sql("INSERT INTO %s VALUES (4, 'd')", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot set both WAP ID and branch, but got ID [%s] and branch [%s]", wapId, BRANCH);
  }

  @Override
  protected void assertPartitionMetadata(
      String tableName, List<Object[]> expected, String... selectPartitionColumns) {
    // Cannot read from the .partitions table newly written data into the WAP branch. See
    // https://github.com/apache/iceberg/issues/7297 for more details.
  }
}
