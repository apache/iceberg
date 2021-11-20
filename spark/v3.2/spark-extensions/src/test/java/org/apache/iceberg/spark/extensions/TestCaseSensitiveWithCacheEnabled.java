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

import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestCaseSensitiveWithCacheEnabled extends SparkExtensionsTestBase {

  public TestCaseSensitiveWithCacheEnabled(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][]{{"spark_catalog",
            SparkSessionCatalog.class.getName(),
            ImmutableMap.of(
                    "type", "hive",
                    "default-namespace", "default",
                    "parquet-enabled", "true",
                    "cache-enabled", "true"
            )}
    };
  }

  @Test
  public void testAddIdentityPartitionWithCaseInsensitive() {
    sql("CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    spark.conf().set("spark.sql.caseSensitive", "false");
    sql("ALTER TABLE %s ADD PARTITION FIELD CATEGORY", tableName);

    table.refresh();

    PartitionSpec expected = PartitionSpec.builderFor(table.schema())
        .withSpecId(1)
        .identity("category")
        .build();

    Assert.assertEquals("Should have new spec field", expected, table.spec());
  }

  @Test
  public void testAddIdentityPartitionWithCaseSensitive() {
    sql("CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    spark.conf().set("spark.sql.caseSensitive", "true");
    sql("ALTER TABLE %s ADD PARTITION FIELD CATEGORY", tableName);

    AssertHelpers.assertThrows("Should reject invalid `write ordered` columns",
        ValidationException.class, "Cannot find field 'CATEGORY' in struct",
        () -> sql("ALTER TABLE %s ADD PARTITION FIELD CATEGORY", tableName));
  }

  @Test
  public void testReplacePartitionWithSwitchCaseSensitive() {
    sql("CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unpartitioned", table.spec().isUnpartitioned());

    spark.conf().set("spark.sql.caseSensitive", "true");
    sql("ALTER TABLE %s ADD PARTITION FIELD days(ts)", tableName);
    table.refresh();
    PartitionSpec expected = PartitionSpec.builderFor(table.schema())
        .withSpecId(1)
        .day("ts")
        .build();
    Assert.assertEquals("Should have new spec field", expected, table.spec());

    spark.conf().set("spark.sql.caseSensitive", "false");
    sql("ALTER TABLE %s REPLACE PARTITION FIELD days(ts) WITH hours(TS)", tableName);
    table.refresh();
    expected = PartitionSpec.builderFor(table.schema())
        .withSpecId(2)
        .alwaysNull("ts", "ts_day")
        .hour("ts")
        .build();
    Assert.assertEquals("Should changed from daily to hourly partitioned field", expected, table.spec());
  }
}
