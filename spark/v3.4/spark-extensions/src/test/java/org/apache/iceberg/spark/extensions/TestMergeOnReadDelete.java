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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.spark.source.TestSparkCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestMergeOnReadDelete extends TestDelete {

  public TestMergeOnReadDelete(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      Boolean vectorized,
      String distributionMode,
      boolean fanoutEnabled,
      String branch) {
    super(
        catalogName,
        implementation,
        config,
        fileFormat,
        vectorized,
        distributionMode,
        fanoutEnabled,
        branch);
  }

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.FORMAT_VERSION,
        "2",
        TableProperties.DELETE_MODE,
        RowLevelOperationMode.MERGE_ON_READ.modeName());
  }

  @Parameterized.AfterParam
  public static void clearTestSparkCatalogCache() {
    TestSparkCatalog.clearTables();
  }

  @Test
  public void testCommitUnknownException() {
    createAndInitTable("id INT, dep STRING, category STRING");

    // write unpartitioned files
    append(tableName, "{ \"id\": 1, \"dep\": \"hr\", \"category\": \"c1\"}");
    createBranchIfNeeded();
    append(
        commitTarget(),
        "{ \"id\": 2, \"dep\": \"hr\", \"category\": \"c1\" }\n"
            + "{ \"id\": 3, \"dep\": \"hr\", \"category\": \"c1\" }");

    Table table = validationCatalog.loadTable(tableIdent);

    RowDelta newRowDelta = table.newRowDelta();
    if (branch != null) {
      newRowDelta.toBranch(branch);
    }

    RowDelta spyNewRowDelta = spy(newRowDelta);
    doAnswer(
            invocation -> {
              newRowDelta.commit();
              throw new CommitStateUnknownException(new RuntimeException("Datacenter on Fire"));
            })
        .when(spyNewRowDelta)
        .commit();

    Table spyTable = spy(table);
    when(spyTable.newRowDelta()).thenReturn(spyNewRowDelta);
    SparkTable sparkTable =
        branch == null ? new SparkTable(spyTable, false) : new SparkTable(spyTable, branch, false);

    ImmutableMap<String, String> config =
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default");
    spark
        .conf()
        .set("spark.sql.catalog.dummy_catalog", "org.apache.iceberg.spark.source.TestSparkCatalog");
    config.forEach(
        (key, value) -> spark.conf().set("spark.sql.catalog.dummy_catalog." + key, value));
    Identifier ident = Identifier.of(new String[] {"default"}, "table");
    TestSparkCatalog.setTable(ident, sparkTable);

    // Although an exception is thrown here, write and commit have succeeded
    Assertions.assertThatThrownBy(
            () -> sql("DELETE FROM %s WHERE id = 2", "dummy_catalog.default.table"))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("Datacenter on Fire");

    // Since write and commit succeeded, the rows should be readable
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "hr", "c1"), row(3, "hr", "c1")),
        sql("SELECT * FROM %s ORDER BY id", "dummy_catalog.default.table"));
  }

  @Test
  public void testAggregatePushDownInMergeOnReadDelete() {
    createAndInitTable("id LONG, data INT");
    sql(
        "INSERT INTO TABLE %s VALUES (1, 1111), (1, 2222), (2, 3333), (2, 4444), (3, 5555), (3, 6666) ",
        tableName);
    createBranchIfNeeded();

    sql("DELETE FROM %s WHERE data = 1111", commitTarget());
    String select = "SELECT max(data), min(data), count(data) FROM %s";

    List<Object[]> explain = sql("EXPLAIN " + select, selectTarget());
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
    boolean explainContainsPushDownAggregates = false;
    if (explainString.contains("max(data)")
        || explainString.contains("min(data)")
        || explainString.contains("count(data)")) {
      explainContainsPushDownAggregates = true;
    }

    Assert.assertFalse(
        "min/max/count not pushed down for deleted", explainContainsPushDownAggregates);

    List<Object[]> actual = sql(select, selectTarget());
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {6666, 2222, 5L});
    assertEquals("min/max/count push down", expected, actual);
  }
}
