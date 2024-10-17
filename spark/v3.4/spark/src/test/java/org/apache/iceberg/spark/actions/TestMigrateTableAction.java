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

import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestMigrateTableAction extends SparkCatalogTestBase {

  public TestMigrateTableAction(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s_BACKUP_", tableName);
  }

  @Test
  public void testMigrateWithParallelTasks() throws IOException {
    String location = temp.newFolder().toURI().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    AtomicInteger migrationThreadsIndex = new AtomicInteger(0);
    SparkActions.get()
        .migrateTable(tableName)
        .executeWith(
            Executors.newFixedThreadPool(
                4,
                runnable -> {
                  Thread thread = new Thread(runnable);
                  thread.setName("table-migration-" + migrationThreadsIndex.getAndIncrement());
                  thread.setDaemon(true);
                  return thread;
                }))
        .execute();
    Assert.assertEquals(migrationThreadsIndex.get(), 2);
  }
}
