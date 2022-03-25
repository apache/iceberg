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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestGenerateSymlinkFormatManifestsProcedure extends SparkExtensionsTestBase {

  public TestGenerateSymlinkFormatManifestsProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() throws Exception {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testGenerateSymlinkFormatManifestsEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    AssertHelpers.assertThrows("Should not support generate symlink manifest for empty table",
        ValidationException.class,
        "Cannot generate symlink manifests for an empty table",
        () -> sql("CALL %s.system.generate_symlink_format_manifest('%s')", catalogName, tableIdent));
  }

  @Test
  public void testGenerateSymlinkFormatManifestsUnpartitioned() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);
    List<Object[]> result = sql("CALL %s.system.generate_symlink_format_manifest('%s')", catalogName, tableIdent);
    Table table = validationCatalog.loadTable(tableIdent);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(row(table.currentSnapshot().snapshotId(), 2L));
    assertEquals("Should find 2 files", expected, result);
    checkDirectoryExists(table.location(), "_symlink_format_manifest");
    checkDirectoryExists(table.location(), "_symlink_format_manifest",
        Long.toString(table.currentSnapshot().snapshotId()));
  }

  @Test
  public void testGenerateSymlinkFormatManifestsPartitioned() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (1, 'b'), (1, 'c')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b'), (2, 'c'), (2, 'd')", tableName);
    List<Object[]> result = sql("CALL %s.system.generate_symlink_format_manifest('%s')", catalogName, tableIdent);
    Table table = validationCatalog.loadTable(tableIdent);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(row(table.currentSnapshot().snapshotId(), 6L));
    assertEquals("Should find 6 files", expected, result);
    checkDirectoryExists(table.location(), "_symlink_format_manifest");
    checkDirectoryExists(table.location(), "_symlink_format_manifest",
        Long.toString(table.currentSnapshot().snapshotId()));
    checkDirectoryExists(table.location(), "_symlink_format_manifest",
        Long.toString(table.currentSnapshot().snapshotId()), "data=a");
    checkDirectoryExists(table.location(), "_symlink_format_manifest",
        Long.toString(table.currentSnapshot().snapshotId()), "data=b");
    checkDirectoryExists(table.location(), "_symlink_format_manifest",
        Long.toString(table.currentSnapshot().snapshotId()), "data=c");
    checkDirectoryExists(table.location(), "_symlink_format_manifest",
        Long.toString(table.currentSnapshot().snapshotId()), "data=d");
  }

  @Test
  public void testGenerateSymlinkFormatManifestsHiddenPartitioned() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg " +
        "PARTITIONED BY (bucket(16, data))", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (1, 'b'), (1, 'c')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b'), (2, 'c'), (2, 'd')", tableName);
    List<Object[]> result = sql("CALL %s.system.generate_symlink_format_manifest('%s')", catalogName, tableIdent);
    Table table = validationCatalog.loadTable(tableIdent);
    String location = table.location();
    List<Object[]> expected = Lists.newArrayList();
    expected.add(row(table.currentSnapshot().snapshotId(), 6L));
    assertEquals("Should find 6 files", expected, result);
    checkDirectoryExists(table.location(), "_symlink_format_manifest");
    checkDirectoryExists(table.location(), "_symlink_format_manifest",
        Long.toString(table.currentSnapshot().snapshotId()));
    checkDirectoryExists(table.location(), "_symlink_format_manifest",
        Long.toString(table.currentSnapshot().snapshotId()), "data_bucket=15");
    checkDirectoryExists(table.location(), "_symlink_format_manifest",
        Long.toString(table.currentSnapshot().snapshotId()), "data_bucket=2");
    checkDirectoryExists(table.location(), "_symlink_format_manifest",
        Long.toString(table.currentSnapshot().snapshotId()), "data_bucket=3");
  }

  @Test
  public void testGenerateSymlinkFormatManifestsCustomLocationPositionArgument() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    String customLocation = table.location() + "/" + UUID.randomUUID();
    List<Object[]> result = sql("CALL %s.system.generate_symlink_format_manifest('%s', '%s')",
        catalogName, tableIdent, customLocation);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(row(table.currentSnapshot().snapshotId(), 2L));
    assertEquals("Should find 2 files", expected, result);
    checkDirectoryExists(customLocation);
  }

  @Test
  public void testGenerateSymlinkFormatManifestsCustomLocationNamedArgument() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    String customLocation = table.location() + "/" + UUID.randomUUID();
    List<Object[]> result = sql(
        "CALL %s.system.generate_symlink_format_manifest(symlink_root_location => '%s', table => '%s')",
        catalogName, customLocation, tableIdent);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(row(table.currentSnapshot().snapshotId(), 2L));
    assertEquals("Should find 2 files", expected, result);
    checkDirectoryExists(customLocation);
  }

  private void checkDirectoryExists(String... paths) {
    String path = String.join("/", paths);
    Assert.assertTrue("Directory should exist: " + path, Files.isDirectory(Paths.get(URI.create(path))));
  }
}
