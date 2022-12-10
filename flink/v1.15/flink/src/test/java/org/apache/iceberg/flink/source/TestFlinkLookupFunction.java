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
package org.apache.iceberg.flink.source;

import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCatalogTestBase;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkLookupFunction extends FlinkCatalogTestBase {
  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final String TABLE_NAME = "test_table";
  private static final String DTABLE_NAME = "d_table";
  private TableEnvironment tEnv;
  private final FileFormat format;
  private final boolean isStreamingJob;
  private Table icebergtable;

  @Parameterized.Parameters(
      name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    Object[] catalogParams = FlinkCatalogTestBase.parameters().iterator().next();
    String catalogName = (String) catalogParams[0];
    Namespace baseNamespace = (Namespace) catalogParams[1];
    FileFormat format = FileFormat.ORC;
    Boolean isStreaming = true;
    parameters.add(new Object[] {catalogName, baseNamespace, format, isStreaming});
    return parameters;
  }

  public TestFlinkLookupFunction(
      String catalogName, Namespace baseNamespace, FileFormat format, Boolean isStreamingJob) {
    super(catalogName, baseNamespace);
    this.format = format;
    this.isStreamingJob = isStreamingJob;
  }

  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @Override
  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, DTABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    BoundedTableFactory.clearDataSets();
    super.clean();
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance();
        if (isStreamingJob) {
          settingsBuilder.inStreamingMode();
          StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment(
                  MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
          env.enableCheckpointing(400);
          env.setMaxParallelism(2);
          env.setParallelism(2);
          tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
        } else {
          settingsBuilder.inBatchMode();
          tEnv = TableEnvironment.create(settingsBuilder.build());
        }
      }
    }
    return tEnv;
  }

  @Test
  public void testLookupOptions() throws Exception {
    sql(
        "CREATE TABLE %s ("
            + "id int, "
            + "data string"
            + ") with ("
            + "'reload-interval'='1000' "
            + ")",
        DTABLE_NAME);

    icebergtable = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, DTABLE_NAME));

    String reloadInterval = icebergtable.properties().get("reload-interval");

    Assert.assertEquals("1000", reloadInterval);
  }

  @Test
  public void testLookupJoin() throws Exception {
    sql("CREATE TABLE %s (id int, data string)", TABLE_NAME);
    sql("CREATE TABLE %s (id int, data string)", DTABLE_NAME);
    sql(
        "INSERT INTO %s values" + "(1, 'a'), " + "(2, 'b'), " + "(3, 'c'), " + "(4, 'd')",
        DTABLE_NAME);

    sql(
        "INSERT INTO %s values"
            + "(1, 'hello'), "
            + "(2, 'world'), "
            + "(7, 'ddd'), "
            + "(9, 'dsss')",
        TABLE_NAME);

    List<Row> result =
        sql(
            "SELECT so.id, so.data"
                + " from "
                + "("
                + "SELECT  id, data, proctime() as proctime "
                + "from %s"
                + ") so "
                + "join %s FOR SYSTEM_TIME AS OF so.proctime as si on so.id=si.id",
            TABLE_NAME, DTABLE_NAME);

    List<Row> expected = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"));
    assertSameElements(expected, result);
  }

  @Test
  public void testLookupjoinPartitionedTable() {
    sql("CREATE TABLE %s (id int, data string)", TABLE_NAME);
    sql(
        "CREATE TABLE %s ("
            + "id int, "
            + "data string, "
            + "pt_d string, "
            + "pt_h string"
            + ") partitioned by ("
            + "pt_d, pt_h"
            + ")",
        DTABLE_NAME);
    sql(
        "INSERT INTO %s values"
            + "(1, 'a', '20230216', '08'), "
            + "(2, 'b', '20230215', '07'), "
            + "(3, 'c', '20230214', '06'), "
            + "(4, 'd', '20230214', '05')",
        DTABLE_NAME);

    sql(
        "INSERT INTO %s values"
            + "(1, 'hello'), "
            + "(2, 'world'), "
            + "(7, 'ddd'), "
            + "(9, 'dsss')",
        TABLE_NAME);

    List<Row> result =
        sql(
            "SELECT so.id, so.data, si.id, si.data, si.pt_d, si.pt_h"
                + " from "
                + "("
                + "SELECT  id, data, proctime() as proctime "
                + "from %s"
                + ") so "
                + "join %s FOR SYSTEM_TIME AS OF so.proctime as si on so.id=si.id",
            TABLE_NAME, DTABLE_NAME);

    List<Row> expected =
        Lists.newArrayList(
            Row.of(1, "hello", 1, "a", "20230216", "08"),
            Row.of(2, "world", 2, "b", "20230215", "07"));
    assertSameElements(expected, result);
  }
}
