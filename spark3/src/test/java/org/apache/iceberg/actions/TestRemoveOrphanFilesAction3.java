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

package org.apache.iceberg.actions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestRemoveOrphanFilesAction3 extends TestRemoveOrphanFilesAction {

  public TestRemoveOrphanFilesAction3(boolean mockSchema, boolean mockAuthority) {
    super(mockSchema, mockAuthority);
  }

  @Test
  public void testSparkCatalogTable() throws Exception {
    // Hadoop catalog can't reset warehouse location, so we use new SparkSession
    SparkSession newSession = spark.newSession();
    String tableLocation = getTableLocation(false);
    newSession.conf().set("spark.sql.catalog.mycat", "org.apache.iceberg.spark.SparkCatalog");
    newSession.conf().set("spark.sql.catalog.mycat.type", "hadoop");
    newSession.conf().set("spark.sql.catalog.mycat.warehouse", tableLocation);
    SparkCatalog cat = (SparkCatalog) newSession.sessionState().catalogManager().catalog("mycat");

    String[] database = {"default"};
    Identifier id = Identifier.of(database, "table");
    Map<String, String> options = Maps.newHashMap();
    Transform[] transforms = {};
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, options);
    SparkTable table = cat.loadTable(id);

    newSession.sql("INSERT INTO mycat.default.table VALUES (1,1,1)");

    String location = createNewFile(table.table().location(), "data/trashfile");

    List<String> results = Actions.forTable(newSession, table.table()).removeOrphanFiles()
        .olderThan(System.currentTimeMillis() + 1000)
        .execute()
        .stream().map(this::getQualifiedPath).collect(Collectors.toList());
    Assert.assertTrue("trash file should be removed\n" + String.join(", ", results) + "\n" + location,
        results.contains(location));
    cat.dropTable(id);
  }

  @Test
  public void testSparkCatalogNamedHadoopTable() throws Exception {
    // Hadoop catalog can't reset warehouse location, so we use new SparkSession
    SparkSession newSession = spark.newSession();
    String tableLocation = getTableLocation(false);
    newSession.conf().set("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog");
    newSession.conf().set("spark.sql.catalog.hadoop.type", "hadoop");
    newSession.conf().set("spark.sql.catalog.hadoop.warehouse", tableLocation);
    SparkCatalog cat = (SparkCatalog) newSession.sessionState().catalogManager().catalog("hadoop");

    String[] database = {"default"};
    Identifier id = Identifier.of(database, "table");
    Map<String, String> options = Maps.newHashMap();
    Transform[] transforms = {};
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, options);
    SparkTable table = cat.loadTable(id);

    newSession.sql("INSERT INTO hadoop.default.table VALUES (1,1,1)");

    String location = createNewFile(table.table().location(), "data/trashfile");

    List<String> results = Actions.forTable(newSession, table.table()).removeOrphanFiles()
        .olderThan(System.currentTimeMillis() + 1000)
        .execute()
        .stream().map(this::getQualifiedPath).collect(Collectors.toList());
    Assert.assertTrue("trash file should be removed",
        results.contains(location));
    cat.dropTable(id);
  }

  @Test
  public void testSparkCatalogNamedHiveTable() throws Exception {
    // Hadoop catalog can't reset warehouse location, so we use new SparkSession
    SparkSession newSession = spark.newSession();
    String tableLocation = getTableLocation(false);
    newSession.conf().set("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog");
    newSession.conf().set("spark.sql.catalog.hive.type", "hadoop");
    newSession.conf().set("spark.sql.catalog.hive.warehouse", tableLocation);
    SparkCatalog cat = (SparkCatalog) newSession.sessionState().catalogManager().catalog("hive");

    String[] database = {"default"};
    Identifier id = Identifier.of(database, "table");
    Map<String, String> options = Maps.newHashMap();
    Transform[] transforms = {};
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, options);
    SparkTable table = cat.loadTable(id);

    newSession.sql("INSERT INTO hive.default.table VALUES (1,1,1)");

    String location = createNewFile(table.table().location(), "data/trashfile");

    List<String> results = Actions.forTable(newSession, table.table()).removeOrphanFiles()
        .olderThan(System.currentTimeMillis() + 1000)
        .execute()
        .stream().map(this::getQualifiedPath).collect(Collectors.toList());
    Assert.assertTrue("trash file should be removed",
        results.contains(location));
    cat.dropTable(id);
  }

  @Test
  public void testSparkSessionCatalogHadoopTable() throws Exception {
    // Hadoop catalog can't reset warehouse location, so we use new SparkSession
    SparkSession newSession = spark.newSession();
    String tableLocation = getTableLocation(false);
    newSession.conf().set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    newSession.conf().set("spark.sql.catalog.spark_catalog.type", "hadoop");
    newSession.conf().set("spark.sql.catalog.spark_catalog.warehouse", tableLocation);
    SparkSessionCatalog cat = (SparkSessionCatalog) newSession.sessionState().catalogManager().v2SessionCatalog();

    String[] database = {"default"};
    Identifier id = Identifier.of(database, "table");
    Map<String, String> options = Maps.newHashMap();
    Transform[] transforms = {};
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, options);
    SparkTable table = (SparkTable) cat.loadTable(id);

    newSession.sql("INSERT INTO default.table VALUES (1,1,1)");

    String location = createNewFile(table.table().location(), "data/trashfile");

    List<String> results = Actions.forTable(newSession, table.table()).removeOrphanFiles()
        .olderThan(System.currentTimeMillis() + 1000)
        .execute()
        .stream().map(this::getQualifiedPath).collect(Collectors.toList());
    Assert.assertTrue("trash file should be removed",
        results.contains(location));
    cat.dropTable(id);
  }

  @Test
  public void testSparkSessionCatalogHiveTable() throws Exception {
    String tableLocation = getTableLocation(false);
    spark.conf().set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
    spark.conf().set("spark.sql.catalog.spark_catalog.warehouse", tableLocation);
    SparkSessionCatalog cat = (SparkSessionCatalog) spark.sessionState().catalogManager().v2SessionCatalog();

    String[] database = {"default"};
    Identifier id = Identifier.of(database, "sessioncattest");
    Map<String, String> options = Maps.newHashMap();
    Transform[] transforms = {};
    cat.dropTable(id);
    cat.createTable(id, SparkSchemaUtil.convert(SCHEMA), transforms, options);
    SparkTable table = (SparkTable) cat.loadTable(id);

    spark.sql("INSERT INTO default.sessioncattest VALUES (1,1,1)");

    String location = createNewFile(table.table().location(), "data/trashfile");

    List<String> results = Actions.forTable(table.table()).removeOrphanFiles()
        .olderThan(System.currentTimeMillis() + 1000)
        .execute()
        .stream().map(this::getQualifiedPath).collect(Collectors.toList());
    Assert.assertTrue("trash file should be removed",
        results.contains(location));
    cat.dropTable(id);
  }

  @After
  public void resetSparkSessionCatalog() throws Exception {
    spark.conf().unset("spark.sql.catalog.spark_catalog");
    spark.conf().unset("spark.sql.catalog.spark_catalog.type");
    spark.conf().unset("spark.sql.catalog.spark_catalog.warehouse");
  }

}
