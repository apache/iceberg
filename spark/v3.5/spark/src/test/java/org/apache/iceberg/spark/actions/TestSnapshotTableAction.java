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

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.spark.CatalogTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSnapshotTableAction extends CatalogTestBase {
  private static final String sourceName = "spark_catalog.default.source";

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s PURGE", sourceName);
  }

  @TestTemplate
  public void testSnapshotWithParallelTasks() throws IOException {
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        sourceName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", sourceName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", sourceName);

    AtomicInteger snapshotThreadsIndex = new AtomicInteger(0);
    SparkActions.get()
        .snapshotTable(sourceName)
        .as(tableName)
        .executeWith(
            Executors.newFixedThreadPool(
                4,
                runnable -> {
                  Thread thread = new Thread(runnable);
                  thread.setName("table-snapshot-" + snapshotThreadsIndex.getAndIncrement());
                  thread.setDaemon(true);
                  return thread;
                }))
        .execute();
    assertThat(snapshotThreadsIndex.get()).isEqualTo(2);
  }
}
