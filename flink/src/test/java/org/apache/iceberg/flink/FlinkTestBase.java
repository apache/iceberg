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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class FlinkTestBase extends AbstractTestBase {

  private static TestHiveMetastore metastore = null;
  protected static HiveConf hiveConf = null;
  protected static HiveCatalog catalog = null;

  private volatile TableEnvironment tEnv = null;

  @BeforeClass
  public static void startMetastore() {
    FlinkTestBase.metastore = new TestHiveMetastore();
    metastore.start();
    FlinkTestBase.hiveConf = metastore.hiveConf();
    FlinkTestBase.catalog = new HiveCatalog(metastore.hiveConf());
  }

  @AfterClass
  public static void stopMetastore() {
    metastore.stop();
    catalog.close();
    FlinkTestBase.catalog = null;
  }

  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          this.tEnv = TableEnvironment.create(EnvironmentSettings
              .newInstance()
              .useBlinkPlanner()
              .inBatchMode().build());
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

  protected List<Object[]> sql(String query, Object... args) {
    TableResult tableResult = exec(String.format(query, args));

    tableResult.getJobClient().ifPresent(c -> {
      try {
        c.getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });

    List<Object[]> results = Lists.newArrayList();
    try (CloseableIterator<Row> iter = tableResult.collect()) {
      while (iter.hasNext()) {
        Row row = iter.next();
        results.add(IntStream.range(0, row.getArity()).mapToObj(row::getField).toArray(Object[]::new));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return results;
  }
}
