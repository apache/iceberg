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

package org.apache.iceberg.hive;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataPathUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.nio.file.attribute.PosixFilePermissions.fromString;


public class TestRelativePaths extends HiveTableBaseTest {
  static final String NON_DEFAULT_DATABASE =  "nondefault";

  @After
  public void cleanup() throws Exception {
    // Drop the database and purge the files
    metastoreClient.dropDatabase(NON_DEFAULT_DATABASE, true, true, true);
  }

  @Test
  public void testMoveHiveTable() throws Exception {

    // Drop the previously created table to make place for the new one
    dropTestTable();
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    String tableLocation = getTableLocation(TABLE_NAME);
    String tableLocationPrefix = tableLocation.substring(0, tableLocation.indexOf(TABLE_NAME));

    createTable(TABLE_IDENTIFIER, tableLocationPrefix, tableLocation);
    Assert.assertTrue("Table should exist", catalog.tableExists(TABLE_IDENTIFIER));
    Table original = catalog.loadTable(TABLE_IDENTIFIER);
    Assert.assertEquals(original.locationPrefix(), tableLocationPrefix);
    org.apache.hadoop.hive.metastore.api.Table oldHmsTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);

    List<String> metadataVersionFiles = metadataVersionFiles(TABLE_NAME);
    Assert.assertEquals(3, metadataVersionFiles.size());

    // 2. Move files to a different location
    // Create a new location to move
    String newPrefix = createTempDirectory(NON_DEFAULT_DATABASE,
        asFileAttribute(fromString("rwxrwxrwx"))).toFile().getAbsolutePath() + "/" +  DB_NAME;
    moveTableFiles(tableLocation, newPrefix + "/" + TABLE_NAME);

    // 3. Update prefix
    String oldMetadataFilePath = Paths.get(newPrefix,
        MetadataPathUtils.toRelativePath(
            metadataVersionFiles.get(metadataVersionFiles.size() - 1),
            removePrefix(tableLocationPrefix),
        true)).toString();

    Table newTable = catalog.updatePrefix(TABLE_IDENTIFIER, oldMetadataFilePath, newPrefix);

    org.apache.hadoop.hive.metastore.api.Table newHmsTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);
    Assert.assertNotEquals("Table location in HMS should have changed", oldHmsTable.getSd().getLocation(),
        newHmsTable.getSd().getLocation());
  }

  public Table createTable(TableIdentifier identifier, String tableLocationPrefix, String tableLocation) throws
      IOException {

    Table table = catalog.buildTable(identifier, schema)
        .withLocationPrefix(tableLocationPrefix)
        .withLocation(tableLocation)
        .withProperties(ImmutableMap.of(
            TableProperties.WRITE_METADATA_USE_RELATIVE_PATH, "true",
            TableProperties.FORMAT_VERSION, "2"))
        .create();

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(schema, TABLE_NAME));
    List<GenericData.Record> records = Lists.newArrayList(
        recordBuilder.set("id", 1L).build(),
        recordBuilder.set("id", 2L).build(),
        recordBuilder.set("id", 3L).build()
    );

    String location1 = table.locationPrefix().replace("file:", "") + "/" + TABLE_NAME +
        "/data/file1.avro";
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(location1))
        .schema(schema)
        .named("test")
        .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    String location2 = table.locationPrefix().replace("file:", "") + "/" + TABLE_NAME +
        "/data/file2.avro";
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(location2))
        .schema(schema)
        .named("test")
        .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    DataFile file1 = DataFiles.builder(table.spec())
        .withRecordCount(3)
        .withPath(location1)
        .withFileSizeInBytes(Files.localInput(location2).getLength())
        .build();

    DataFile file2 = DataFiles.builder(table.spec())
        .withRecordCount(3)
        .withPath(location2)
        .withFileSizeInBytes(Files.localInput(location1).getLength())
        .build();

    // add both data files
    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // delete file2
    table.newDelete().deleteFile(file2.path()).commit();

    return table;

  }

  private void moveTableFiles(String sourceDir, String targetDir) throws Exception {
    FileUtils.copyDirectory(new File(removePrefix(sourceDir) + "/data/"),
        new File(removePrefix(targetDir) + "/data/"));
    FileUtils.copyDirectory(new File(removePrefix(sourceDir) + "/metadata/"),
        new File(removePrefix(targetDir) + "/metadata/"));
  }

  private String removePrefix(String path) {
    return path.substring(path.lastIndexOf(":") + 1);
  }
}
