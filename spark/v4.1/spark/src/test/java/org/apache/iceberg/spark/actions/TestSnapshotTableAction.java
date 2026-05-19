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

import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

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
  private static final String SOURCE_NAME = "spark_catalog.default.source";
  private static final String SOURCE = "source";

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s PURGE", SOURCE_NAME);
  }

  @TestTemplate
  public void testSnapshotWithParallelTasks() throws IOException {
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", SOURCE_NAME);

    AtomicInteger snapshotThreadsIndex = new AtomicInteger(0);
    SparkActions.get()
        .snapshotTable(SOURCE_NAME)
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

  @TestTemplate
  public void testSnapshotWithOverlappingLocation() throws IOException {
    //  Hadoop Catalogs do not Support Custom Table Locations
    String catalogType = catalogConfig.get(ICEBERG_CATALOG_TYPE);
    assumeThat(catalogType).isNotEqualTo(ICEBERG_CATALOG_TYPE_HADOOP);

    String sourceLocation =
        Files.createTempDirectory(temp, "junit").resolve(SOURCE).toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, sourceLocation);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", SOURCE_NAME);
    String actualSourceLocation =
        spark
            .sql(String.format("DESCRIBE EXTENDED %s", SOURCE_NAME))
            .filter("col_name = 'Location'")
            .select("data_type")
            .first()
            .getString(0);

    assertThatThrownBy(
            () ->
                SparkActions.get()
                    .snapshotTable(SOURCE_NAME)
                    .as(tableName)
                    .tableLocation(actualSourceLocation)
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "The snapshot table location cannot be same as the source table location.");

    String destAsSubdirectory = actualSourceLocation + "/nested";
    assertThatThrownBy(
            () ->
                SparkActions.get()
                    .snapshotTable(SOURCE_NAME)
                    .as(tableName)
                    .tableLocation(destAsSubdirectory)
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot create a snapshot at location");

    String parentLocation =
        actualSourceLocation.substring(0, actualSourceLocation.length() - ("/" + SOURCE).length());
    assertThatThrownBy(
            () ->
                SparkActions.get()
                    .snapshotTable(SOURCE_NAME)
                    .as(tableName)
                    .tableLocation(parentLocation)
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot create a snapshot at location");
  }

  @TestTemplate
  public void testSnapshotWithNonOverlappingLocation() throws IOException {
    //  Hadoop Catalogs do not Support Custom Table Locations
    String catalogType = catalogConfig.get(ICEBERG_CATALOG_TYPE);
    assumeThat(catalogType).isNotEqualTo(ICEBERG_CATALOG_TYPE_HADOOP);

    String sourceLocation =
        Files.createTempDirectory(temp, "junit").resolve(SOURCE).toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, sourceLocation);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", SOURCE_NAME);
    String actualSourceLocation =
        spark
            .sql(String.format("DESCRIBE EXTENDED %s", SOURCE_NAME))
            .filter("col_name = 'Location'")
            .select("data_type")
            .first()
            .getString(0);

    String validDestLocation =
        actualSourceLocation.substring(0, actualSourceLocation.length() - SOURCE.length())
            + "newDestination";
    SparkActions.get()
        .snapshotTable(SOURCE_NAME)
        .as(tableName)
        .tableLocation(validDestLocation)
        .execute();
    assertThat(sql("SELECT * FROM %s", tableName)).hasSize(2);
  }
}
