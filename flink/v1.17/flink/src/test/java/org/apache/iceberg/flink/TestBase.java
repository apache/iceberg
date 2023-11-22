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
package org.apache.iceberg.flink;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public abstract class TestBase extends TestBaseUtils {

  @RegisterExtension
  public static MiniClusterExtension miniClusterResource =
      MiniFlinkClusterResource.createWithClassloaderCheckDisabled();

  @TempDir Path temporaryDirectory;

  private static TestHiveMetastore metastore = null;
  protected static HiveConf hiveConf = null;
  protected static HiveCatalog catalog = null;

  private volatile TableEnvironment tEnv = null;

  @BeforeAll
  public static void startMetastore() {
    TestBase.metastore = new TestHiveMetastore();
    metastore.start();
    TestBase.hiveConf = metastore.hiveConf();
    TestBase.catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);
  }

  @AfterAll
  public static void stopMetastore() throws Exception {
    metastore.stop();
    TestBase.catalog = null;
  }

  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();

          TableEnvironment env = TableEnvironment.create(settings);
          env.getConfig()
              .getConfiguration()
              .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
          tEnv = env;
        }
      }
    }
    return tEnv;
  }

  protected static TableResult exec(TableEnvironment env, String query, Object... args) {
    return env.executeSql(String.format(query, args));
  }

  protected TableResult exec(String query, Object... args) {
    return exec(getTableEnv(), query, args);
  }

  protected List<Row> sql(String query, Object... args) {
    TableResult tableResult = exec(query, args);
    try (CloseableIterator<Row> iter = tableResult.collect()) {
      return Lists.newArrayList(iter);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }

  protected void assertSameElements(Iterable<Row> expected, Iterable<Row> actual) {
    Assertions.assertThat(actual).isNotNull().containsExactlyInAnyOrderElementsOf(expected);
  }

  protected void assertSameElements(String message, Iterable<Row> expected, Iterable<Row> actual) {
    Assertions.assertThat(actual)
        .isNotNull()
        .as(message)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  /**
   * We can not drop currently used catalog after FLINK-29677, so we have make sure that we do not
   * use the current catalog before dropping it. This method switches to the 'default_catalog' and
   * drops the one requested.
   *
   * @param catalogName The catalog to drop
   * @param ifExists If we should use the 'IF EXISTS' when dropping the catalog
   */
  protected void dropCatalog(String catalogName, boolean ifExists) {
    sql("USE CATALOG default_catalog");
    sql("DROP CATALOG %s %s", ifExists ? "IF EXISTS" : "", catalogName);
  }
}
