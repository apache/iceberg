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

import java.io.File;
import java.util.stream.StreamSupport;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestRemoveOrphanFilesAction3 extends TestRemoveOrphanFilesAction {
  @TestTemplate
  public void testSparkCatalogTable() throws Exception {
    spark.conf().set("spark.sql.catalog.mycat", "org.apache.iceberg.spark.SparkCatalog");
    spark.conf().set("spark.sql.catalog.mycat.type", "hadoop");
    spark.conf().set("spark.sql.catalog.mycat.warehouse", tableLocation);
    SparkCatalog cat = (SparkCatalog) spark.sessionState().catalogManager().catalog("mycat");

    String[] database = {"default"};
    Identifier id = Identifier.of(database, randomName("table"));
    Transform[] transforms = {};
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, properties);
    SparkTable table = (SparkTable) cat.loadTable(id);

    sql("INSERT INTO mycat.default.%s VALUES (1,1,1)", id.name());

    String location = table.table().location().replaceFirst("file:", "");
    String trashFile = randomName("/data/trashfile");
    new File(location + trashFile).createNewFile();

    DeleteOrphanFiles.Result results =
        SparkActions.get()
            .deleteOrphanFiles(table.table())
            .olderThan(System.currentTimeMillis() + 1000)
            .execute();
    assertThat(results.orphanFileLocations())
        .as("trash file should be removed")
        .contains("file:" + location + trashFile);
  }

  @TestTemplate
  public void testSparkCatalogNamedHadoopTable() throws Exception {
    spark.conf().set("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog");
    spark.conf().set("spark.sql.catalog.hadoop.type", "hadoop");
    spark.conf().set("spark.sql.catalog.hadoop.warehouse", tableLocation);
    SparkCatalog cat = (SparkCatalog) spark.sessionState().catalogManager().catalog("hadoop");

    String[] database = {"default"};
    Identifier id = Identifier.of(database, randomName("table"));
    Transform[] transforms = {};
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, properties);
    SparkTable table = (SparkTable) cat.loadTable(id);

    sql("INSERT INTO hadoop.default.%s VALUES (1,1,1)", id.name());

    String location = table.table().location().replaceFirst("file:", "");
    String trashFile = randomName("/data/trashfile");
    new File(location + trashFile).createNewFile();

    DeleteOrphanFiles.Result results =
        SparkActions.get()
            .deleteOrphanFiles(table.table())
            .olderThan(System.currentTimeMillis() + 1000)
            .execute();
    assertThat(results.orphanFileLocations())
        .as("trash file should be removed")
        .contains("file:" + location + trashFile);
  }

  @TestTemplate
  public void testSparkCatalogNamedHiveTable() throws Exception {
    spark.conf().set("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog");
    spark.conf().set("spark.sql.catalog.hive.type", "hadoop");
    spark.conf().set("spark.sql.catalog.hive.warehouse", tableLocation);
    SparkCatalog cat = (SparkCatalog) spark.sessionState().catalogManager().catalog("hive");

    String[] database = {"default"};
    Identifier id = Identifier.of(database, randomName("table"));
    Transform[] transforms = {};
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, properties);
    SparkTable table = (SparkTable) cat.loadTable(id);

    sql("INSERT INTO hive.default.%s VALUES (1,1,1)", id.name());

    String location = table.table().location().replaceFirst("file:", "");
    String trashFile = randomName("/data/trashfile");
    new File(location + trashFile).createNewFile();

    DeleteOrphanFiles.Result results =
        SparkActions.get()
            .deleteOrphanFiles(table.table())
            .olderThan(System.currentTimeMillis() + 1000)
            .execute();

    assertThat(StreamSupport.stream(results.orphanFileLocations().spliterator(), false))
        .as("trash file should be removed")
        .anyMatch(file -> file.contains("file:" + location + trashFile));
  }

  @TestTemplate
  public void testSparkSessionCatalogHadoopTable() throws Exception {
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hadoop");
    spark.conf().set("spark.sql.catalog.spark_catalog.warehouse", tableLocation);
    SparkSessionCatalog cat =
        (SparkSessionCatalog) spark.sessionState().catalogManager().v2SessionCatalog();

    String[] database = {"default"};
    Identifier id = Identifier.of(database, randomName("table"));
    Transform[] transforms = {};
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, properties);
    SparkTable table = (SparkTable) cat.loadTable(id);

    sql("INSERT INTO default.%s VALUES (1,1,1)", id.name());

    String location = table.table().location().replaceFirst("file:", "");
    String trashFile = randomName("/data/trashfile");
    new File(location + trashFile).createNewFile();

    DeleteOrphanFiles.Result results =
        SparkActions.get()
            .deleteOrphanFiles(table.table())
            .olderThan(System.currentTimeMillis() + 1000)
            .execute();
    assertThat(results.orphanFileLocations())
        .as("trash file should be removed")
        .contains("file:" + location + trashFile);
  }

  @TestTemplate
  public void testSparkSessionCatalogHiveTable() throws Exception {
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
    SparkSessionCatalog cat =
        (SparkSessionCatalog) spark.sessionState().catalogManager().v2SessionCatalog();

    String[] database = {"default"};
    Identifier id = Identifier.of(database, "sessioncattest");
    Transform[] transforms = {};
    cat.dropTable(id);
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, properties);
    SparkTable table = (SparkTable) cat.loadTable(id);

    sql("INSERT INTO default.sessioncattest VALUES (1,1,1)");

    String location = table.table().location().replaceFirst("file:", "");
    String trashFile = randomName("/data/trashfile");
    new File(location + trashFile).createNewFile();

    DeleteOrphanFiles.Result results =
        SparkActions.get()
            .deleteOrphanFiles(table.table())
            .olderThan(System.currentTimeMillis() + 1000)
            .execute();
    assertThat(results.orphanFileLocations())
        .as("trash file should be removed")
        .contains("file:" + location + trashFile);
  }

  @AfterEach
  public void resetSparkSessionCatalog() {
    spark.conf().unset("spark.sql.catalog.spark_catalog");
    spark.conf().unset("spark.sql.catalog.spark_catalog.type");
    spark.conf().unset("spark.sql.catalog.spark_catalog.warehouse");
  }
}
