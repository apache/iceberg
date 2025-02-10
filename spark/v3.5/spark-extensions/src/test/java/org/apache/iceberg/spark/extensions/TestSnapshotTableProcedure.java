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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSnapshotTableProcedure extends ExtensionsTestBase {
  private static final String SOURCE_NAME = "spark_catalog.default.source";

  // Currently we can only Snapshot only out of the Spark Session Catalog

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s PURGE", SOURCE_NAME);
  }

  @TestTemplate
  public void testSnapshot() throws IOException {
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);
    Object result =
        scalarSql("CALL %s.system.snapshot('%s', '%s')", catalogName, SOURCE_NAME, tableName);

    assertThat(result).as("Should have added one file").isEqualTo(1L);

    Table createdTable = validationCatalog.loadTable(tableIdent);
    String tableLocation = createdTable.location();
    assertThat(tableLocation)
        .as("Table should not have the original location")
        .isNotEqualTo(location);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testSnapshotWithProperties() throws IOException {
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);
    Object result =
        scalarSql(
            "CALL %s.system.snapshot(source_table => '%s', table => '%s', properties => map('foo','bar'))",
            catalogName, SOURCE_NAME, tableName);

    assertThat(result).as("Should have added one file").isEqualTo(1L);

    Table createdTable = validationCatalog.loadTable(tableIdent);

    String tableLocation = createdTable.location();
    assertThat(tableLocation)
        .as("Table should not have the original location")
        .isNotEqualTo(location);

    Map<String, String> props = createdTable.properties();
    assertThat(props).as("Should have extra property set").containsEntry("foo", "bar");

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testSnapshotWithAlternateLocation() throws IOException {
    assumeThat(catalogName)
        .as("No Snapshoting with Alternate locations with Hadoop Catalogs")
        .doesNotContain("hadoop");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    String snapshotLocation = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);
    Object[] result =
        sql(
                "CALL %s.system.snapshot(source_table => '%s', table => '%s', location => '%s')",
                catalogName, SOURCE_NAME, tableName, snapshotLocation)
            .get(0);

    assertThat(result[0]).as("Should have added one file").isEqualTo(1L);

    String storageLocation = validationCatalog.loadTable(tableIdent).location();
    assertThat(storageLocation)
        .as("Snapshot should be made at specified location")
        .isEqualTo(snapshotLocation);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testDropTable() throws IOException {
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);

    Object result =
        scalarSql("CALL %s.system.snapshot('%s', '%s')", catalogName, SOURCE_NAME, tableName);
    assertThat(result).as("Should have added one file").isEqualTo(1L);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", tableName));

    sql("DROP TABLE %s", tableName);

    assertEquals(
        "Source table should be intact",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", SOURCE_NAME));
  }

  @TestTemplate
  public void testSnapshotWithConflictingProps() throws IOException {
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);

    Object result =
        scalarSql(
            "CALL %s.system.snapshot("
                + "source_table => '%s',"
                + "table => '%s',"
                + "properties => map('%s', 'true', 'snapshot', 'false'))",
            catalogName, SOURCE_NAME, tableName, TableProperties.GC_ENABLED);
    assertThat(result).as("Should have added one file").isEqualTo(1L);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Map<String, String> props = table.properties();
    assertThat(props).as("Should override user value").containsEntry("snapshot", "true");
    assertThat(props)
        .as("Should override user value")
        .containsEntry(TableProperties.GC_ENABLED, "false");
  }

  @TestTemplate
  public void testInvalidSnapshotsCases() throws IOException {
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, location);

    assertThatThrownBy(() -> sql("CALL %s.system.snapshot('foo')", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Missing required parameters: [table]");

    assertThatThrownBy(
            () -> sql("CALL %s.system.snapshot('n', 't', map('foo', 'bar'))", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith("Wrong arg type for location");

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.snapshot('%s', 'fable', 'loc', map(2, 1, 1))",
                    catalogName, SOURCE_NAME))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "The `map` requires 2n (n > 0) parameters but the actual number is 3");

    assertThatThrownBy(() -> sql("CALL %s.system.snapshot('', 'dest')", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot handle an empty identifier for argument source_table");

    assertThatThrownBy(() -> sql("CALL %s.system.snapshot('src', '')", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot handle an empty identifier for argument table");
  }

  @TestTemplate
  public void testSnapshotWithParallelism() throws IOException {
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", SOURCE_NAME);

    List<Object[]> result =
        sql(
            "CALL %s.system.snapshot(source_table => '%s', table => '%s', parallelism => %d)",
            catalogName, SOURCE_NAME, tableName, 2);
    assertEquals("Procedure output must match", ImmutableList.of(row(2L)), result);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testSnapshotWithInvalidParallelism() throws IOException {
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", SOURCE_NAME);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.snapshot(source_table => '%s', table => '%s', parallelism => %d)",
                    catalogName, SOURCE_NAME, tableName, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Parallelism should be larger than 0");
  }

  @TestTemplate
  public void testSnapshotPartitionedWithParallelism() throws IOException {
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet PARTITIONED BY (id) LOCATION '%s'",
        SOURCE_NAME, location);
    sql("INSERT INTO TABLE %s (id, data) VALUES (1, 'a'), (2, 'b')", SOURCE_NAME);
    List<Object[]> result =
        sql(
            "CALL %s.system.snapshot(source_table => '%s', table => '%s', parallelism => %d)",
            catalogName, SOURCE_NAME, tableName, 2);
    assertEquals("Procedure output must match", ImmutableList.of(row(2L)), result);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row("a", 1L), row("b", 2L)),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }
}
