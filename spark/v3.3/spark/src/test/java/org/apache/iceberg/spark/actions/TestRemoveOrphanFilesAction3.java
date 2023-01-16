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

import java.io.File;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestRemoveOrphanFilesAction3 extends TestRemoveOrphanFilesAction {
  @Test
  public void testSparkCatalogTable() throws Exception {
    spark.conf().set("spark.sql.catalog.mycat", "org.apache.iceberg.spark.SparkCatalog");
    spark.conf().set("spark.sql.catalog.mycat.type", "hadoop");
    spark.conf().set("spark.sql.catalog.mycat.warehouse", tableLocation);
    SparkCatalog cat = (SparkCatalog) spark.sessionState().catalogManager().catalog("mycat");

    String[] database = {"default"};
    Identifier id = Identifier.of(database, "table");
    Map<String, String> options = Maps.newHashMap();
    Transform[] transforms = {};
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, options);
    SparkTable table = (SparkTable) cat.loadTable(id);

    spark.sql("INSERT INTO mycat.default.table VALUES (1,1,1)");

    String location = table.table().location().replaceFirst("file:", "");
    new File(location + "/data/trashfile").createNewFile();

    DeleteOrphanFiles.Result results =
        SparkActions.get()
            .deleteOrphanFiles(table.table())
            .olderThan(System.currentTimeMillis() + 1000)
            .execute();
    Assert.assertTrue(
        "trash file should be removed",
        StreamSupport.stream(results.orphanFileLocations().spliterator(), false)
            .anyMatch(file -> file.contains("file:" + location + "/data/trashfile")));
  }

  @Test
  public void testSparkCatalogNamedHadoopTable() throws Exception {
    spark.conf().set("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog");
    spark.conf().set("spark.sql.catalog.hadoop.type", "hadoop");
    spark.conf().set("spark.sql.catalog.hadoop.warehouse", tableLocation);
    SparkCatalog cat = (SparkCatalog) spark.sessionState().catalogManager().catalog("hadoop");

    String[] database = {"default"};
    Identifier id = Identifier.of(database, "table");
    Map<String, String> options = Maps.newHashMap();
    Transform[] transforms = {};
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, options);
    SparkTable table = (SparkTable) cat.loadTable(id);

    spark.sql("INSERT INTO hadoop.default.table VALUES (1,1,1)");

    String location = table.table().location().replaceFirst("file:", "");
    new File(location + "/data/trashfile").createNewFile();

    DeleteOrphanFiles.Result results =
        SparkActions.get()
            .deleteOrphanFiles(table.table())
            .olderThan(System.currentTimeMillis() + 1000)
            .execute();
    Assert.assertTrue(
        "trash file should be removed",
        StreamSupport.stream(results.orphanFileLocations().spliterator(), false)
            .anyMatch(file -> file.contains("file:" + location + "/data/trashfile")));
  }

  @Test
  public void testSparkCatalogNamedHiveTable() throws Exception {
    spark.conf().set("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog");
    spark.conf().set("spark.sql.catalog.hive.type", "hadoop");
    spark.conf().set("spark.sql.catalog.hive.warehouse", tableLocation);
    SparkCatalog cat = (SparkCatalog) spark.sessionState().catalogManager().catalog("hive");

    String[] database = {"default"};
    Identifier id = Identifier.of(database, "table");
    Map<String, String> options = Maps.newHashMap();
    Transform[] transforms = {};
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, options);
    SparkTable table = (SparkTable) cat.loadTable(id);

    spark.sql("INSERT INTO hive.default.table VALUES (1,1,1)");

    String location = table.table().location().replaceFirst("file:", "");
    new File(location + "/data/trashfile").createNewFile();

    DeleteOrphanFiles.Result results =
        SparkActions.get()
            .deleteOrphanFiles(table.table())
            .olderThan(System.currentTimeMillis() + 1000)
            .execute();
    Assert.assertTrue(
        "trash file should be removed",
        StreamSupport.stream(results.orphanFileLocations().spliterator(), false)
            .anyMatch(file -> file.contains("file:" + location + "/data/trashfile")));
  }

  @Test
  public void testSparkSessionCatalogHadoopTable() throws Exception {
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hadoop");
    spark.conf().set("spark.sql.catalog.spark_catalog.warehouse", tableLocation);
    SparkSessionCatalog cat =
        (SparkSessionCatalog) spark.sessionState().catalogManager().v2SessionCatalog();

    String[] database = {"default"};
    Identifier id = Identifier.of(database, "table");
    Map<String, String> options = Maps.newHashMap();
    Transform[] transforms = {};
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, options);
    SparkTable table = (SparkTable) cat.loadTable(id);

    spark.sql("INSERT INTO default.table VALUES (1,1,1)");

    String location = table.table().location().replaceFirst("file:", "");
    new File(location + "/data/trashfile").createNewFile();

    DeleteOrphanFiles.Result results =
        SparkActions.get()
            .deleteOrphanFiles(table.table())
            .olderThan(System.currentTimeMillis() + 1000)
            .execute();
    Assert.assertTrue(
        "trash file should be removed",
        StreamSupport.stream(results.orphanFileLocations().spliterator(), false)
            .anyMatch(file -> file.contains("file:" + location + "/data/trashfile")));
  }

  @Test
  public void testSparkSessionCatalogHiveTable() throws Exception {
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
    SparkSessionCatalog cat =
        (SparkSessionCatalog) spark.sessionState().catalogManager().v2SessionCatalog();

    String[] database = {"default"};
    Identifier id = Identifier.of(database, "sessioncattest");
    Map<String, String> options = Maps.newHashMap();
    Transform[] transforms = {};
    cat.dropTable(id);
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, options);
    SparkTable table = (SparkTable) cat.loadTable(id);

    spark.sql("INSERT INTO default.sessioncattest VALUES (1,1,1)");

    String location = table.table().location().replaceFirst("file:", "");
    new File(location + "/data/trashfile").createNewFile();

    DeleteOrphanFiles.Result results =
        SparkActions.get()
            .deleteOrphanFiles(table.table())
            .olderThan(System.currentTimeMillis() + 1000)
            .execute();
    Assert.assertTrue(
        "trash file should be removed",
        StreamSupport.stream(results.orphanFileLocations().spliterator(), false)
            .anyMatch(file -> file.contains("file:" + location + "/data/trashfile")));
  }

  @After
  public void resetSparkSessionCatalog() throws Exception {
    spark.conf().unset("spark.sql.catalog.spark_catalog");
    spark.conf().unset("spark.sql.catalog.spark_catalog.type");
    spark.conf().unset("spark.sql.catalog.spark_catalog.warehouse");
  }
}
