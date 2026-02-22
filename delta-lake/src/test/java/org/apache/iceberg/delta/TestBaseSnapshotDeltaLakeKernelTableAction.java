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
package org.apache.iceberg.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestBaseSnapshotDeltaLakeKernelTableAction {
  private static final String DELTA_GOLDEN_TABLES_ROOT = "delta/golden/";

  @TempDir private File sourceDeltaFolder;
  @TempDir private File icebergWarehouseFolder;
  private String sourceTableLocation;
  private final Configuration testHadoopConf = new Configuration();
  private String newTableLocation;
  private Catalog testCatalog;

  @BeforeEach
  public void before() throws IOException {
    sourceTableLocation = sourceDeltaFolder.toURI().toString();

    Path icebergTablePath = icebergWarehouseFolder.toPath().resolve("iceberg_table");
    newTableLocation = icebergTablePath.toString();
    Files.createDirectories(icebergTablePath);

    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, icebergWarehouseFolder.toString());

    HadoopCatalog icebergCatalog = new HadoopCatalog();
    icebergCatalog.setConf(new Configuration());
    icebergCatalog.initialize("hadoop_catalog", properties);
    testCatalog = icebergCatalog;
  }

  @Test
  public void testRequiredTableIdentifier() {
    SnapshotDeltaLakeTable testAction =
        new BaseSnapshotDeltaLakeKernelTableAction(sourceTableLocation)
            .icebergCatalog(testCatalog)
            .deltaLakeConfiguration(testHadoopConf)
            .tableLocation(newTableLocation);
    assertThatThrownBy(testAction::execute)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Iceberg catalog and identifier cannot be null. Make sure to configure the action with a valid Iceberg catalog and identifier.");
  }

  @Test
  public void testRequiredIcebergCatalog() {
    SnapshotDeltaLakeTable testAction =
        new BaseSnapshotDeltaLakeKernelTableAction(sourceTableLocation)
            .as(TableIdentifier.of("test", "test"))
            .deltaLakeConfiguration(testHadoopConf)
            .tableLocation(newTableLocation);
    assertThatThrownBy(testAction::execute)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Iceberg catalog and identifier cannot be null. Make sure to configure the action with a valid Iceberg catalog and identifier.");
  }

  @Test
  public void testRequiredDeltaLakeConfiguration() {
    SnapshotDeltaLakeTable testAction =
        new BaseSnapshotDeltaLakeKernelTableAction(sourceTableLocation)
            .as(TableIdentifier.of("test", "test"))
            .icebergCatalog(testCatalog)
            .tableLocation(newTableLocation);
    assertThatThrownBy(testAction::execute)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Make sure to configure the action with a valid deltaLakeConfiguration");
  }

  @Test
  public void testTableCreated() throws Exception {
    loadDeltaLakeGoldenTable("basic-decimal-table");

    TableIdentifier icebergTable = TableIdentifier.of("iceberg_table");
    SnapshotDeltaLakeTable testAction =
        new BaseSnapshotDeltaLakeKernelTableAction(sourceTableLocation)
            .as(icebergTable)
            .deltaLakeConfiguration(testHadoopConf)
            .icebergCatalog(testCatalog)
            .tableLocation(newTableLocation);

    assertThat(testCatalog.tableExists(icebergTable)).isFalse();

    testAction.execute();

    assertThat(testCatalog.tableExists(icebergTable)).isTrue();
  }

  @Test
  public void testDefaultTableProperties() throws Exception {
    loadDeltaLakeGoldenTable("basic-decimal-table");

    TableIdentifier icebergTableIdentifier = TableIdentifier.of("iceberg_table");
    SnapshotDeltaLakeTable testAction =
        new BaseSnapshotDeltaLakeKernelTableAction(sourceTableLocation)
            .as(icebergTableIdentifier)
            .deltaLakeConfiguration(testHadoopConf)
            .icebergCatalog(testCatalog)
            .tableLocation(newTableLocation);

    assertThat(testCatalog.tableExists(icebergTableIdentifier)).isFalse();

    testAction.execute();

    assertThat(testCatalog.tableExists(icebergTableIdentifier)).isTrue();

    Map<String, String> properties = testCatalog.loadTable(icebergTableIdentifier).properties();
    assertThat(properties.get("original_location")).isEqualTo(sourceTableLocation);
    assertThat(properties.get("snapshot_source")).isEqualTo("delta");
    assertThat(properties.containsKey(TableProperties.DEFAULT_NAME_MAPPING)).isTrue();
  }

  @Test
  public void testCustomTableProperties() throws Exception {
    loadDeltaLakeGoldenTable("basic-decimal-table");

    TableIdentifier icebergTableIdentifier = TableIdentifier.of("iceberg_table");
    SnapshotDeltaLakeTable testAction =
        new BaseSnapshotDeltaLakeKernelTableAction(sourceTableLocation)
            .as(icebergTableIdentifier)
            .deltaLakeConfiguration(testHadoopConf)
            .icebergCatalog(testCatalog)
            .tableLocation(newTableLocation)
            .tableProperty("custom_prop_1", "custom val 1")
            .tableProperty("custom_prop_2", "custom val 2")
            .tableProperties(
                ImmutableMap.of(
                    "custom_map_prop1", "val 1",
                    "custom_map_prop2", "val 2"));

    assertThat(testCatalog.tableExists(icebergTableIdentifier)).isFalse();

    testAction.execute();

    assertThat(testCatalog.tableExists(icebergTableIdentifier)).isTrue();

    Map<String, String> properties = testCatalog.loadTable(icebergTableIdentifier).properties();
    assertThat(properties.get("custom_prop_1")).isEqualTo("custom val 1");
    assertThat(properties.get("custom_prop_2")).isEqualTo("custom val 2");
    assertThat(properties.get("custom_map_prop1")).isEqualTo("val 1");
    assertThat(properties.get("custom_map_prop2")).isEqualTo("val 2");
  }

  @Test
  void testDeltaTableNotExist() {
    SnapshotDeltaLakeTable testAction =
        new BaseSnapshotDeltaLakeKernelTableAction(sourceTableLocation)
            .as(TableIdentifier.of("test", "test"))
            .deltaLakeConfiguration(testHadoopConf)
            .icebergCatalog(testCatalog)
            .tableLocation(newTableLocation);

    assertThatThrownBy(testAction::execute)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Delta Lake table does not exist at the given location: %s", sourceTableLocation);
  }

  static List<String> deltaGoldenTableNames() throws Exception {
    Path rootPath =
        Paths.get(
            TestBaseSnapshotDeltaLakeKernelTableAction.class
                .getClassLoader()
                .getResource(DELTA_GOLDEN_TABLES_ROOT)
                .toURI());

    try (Stream<Path> stream = Files.list(rootPath)) {
      return stream
          .filter(Files::isDirectory)
          .map(path -> path.getFileName().toString())
          .collect(Collectors.toList());
    }
  }

  @ParameterizedTest
  @MethodSource("deltaGoldenTableNames")
  void goldenDeltaTableConversion(String deltaTableName) throws Exception {
    loadDeltaLakeGoldenTable(deltaTableName);

    SnapshotDeltaLakeTable testAction =
        new BaseSnapshotDeltaLakeKernelTableAction(sourceTableLocation)
            .as(TableIdentifier.of("iceberg_table"))
            .deltaLakeConfiguration(testHadoopConf)
            .icebergCatalog(testCatalog)
            .tableLocation(newTableLocation);

    testAction.execute();
  }

  private void loadDeltaLakeGoldenTable(String goldenTableName) throws Exception {
    String goldenTableFolderPath = DELTA_GOLDEN_TABLES_ROOT + goldenTableName;
    URI resourceUri =
        Objects.requireNonNull(
                getClass().getClassLoader().getResource(goldenTableFolderPath),
                "Could not find Delta table test table path: " + goldenTableFolderPath)
            .toURI();

    Path source = Paths.get(resourceUri);
    Path target = sourceDeltaFolder.toPath();
    copyDirectory(source, target);
  }

  private void copyDirectory(Path source, Path target) throws IOException {
    Files.walkFileTree(
        source,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            Path targetDir = target.resolve(source.relativize(dir));
            if (!Files.exists(targetDir)) {
              Files.createDirectory(targetDir);
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.copy(
                file, target.resolve(source.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
            return FileVisitResult.CONTINUE;
          }
        });
  }
}
