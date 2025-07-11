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
import static org.assertj.core.api.Assertions.atIndex;

import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteTablePathProcedure extends ExtensionsTestBase {
  @TempDir private Path staging;
  @TempDir private Path targetTableDir;

  @BeforeEach
  public void setupTableLocation() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testRewriteTablePathWithPositionalArgument() {
    String location = targetTableDir.toFile().toURI().toString();
    Table table = validationCatalog.loadTable(tableIdent);
    String metadataJson = TableUtil.metadataFileLocation(table);

    List<Object[]> result =
        sql(
            "CALL %s.system.rewrite_table_path('%s', '%s', '%s')",
            catalogName, tableIdent, table.location(), location);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0])
        .as("Should return correct latest version")
        .isEqualTo(RewriteTablePathUtil.fileName(metadataJson));
    assertThat(result.get(0)[1])
        .as("Should return file_list_location")
        .asString()
        .startsWith(table.location())
        .endsWith("file-list");
    checkFileListLocationCount((String) result.get(0)[1], 1);
  }

  @TestTemplate
  public void testRewriteTablePathWithNamedArgument() {
    Table table = validationCatalog.loadTable(tableIdent);
    String v0Metadata = RewriteTablePathUtil.fileName(TableUtil.metadataFileLocation(table));
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    String v1Metadata =
        RewriteTablePathUtil.fileName(
            (((HasTableOperations) table).operations()).refresh().metadataFileLocation());

    String targetLocation = targetTableDir.toFile().toURI().toString();
    String stagingLocation = staging.toFile().toURI().toString();
    String expectedFileListLocation = stagingLocation + "file-list";

    List<Object[]> result =
        sql(
            "CALL %s.system.rewrite_table_path("
                + "table => '%s', "
                + "target_prefix => '%s', "
                + "source_prefix => '%s', "
                + "end_version => '%s', "
                + "start_version => '%s', "
                + "staging_location => '%s')",
            catalogName,
            tableIdent,
            targetLocation,
            table.location(),
            v1Metadata,
            v0Metadata,
            stagingLocation);
    assertThat(result)
        .singleElement()
        .satisfies(
            objects -> {
              assertThat(objects).contains(v1Metadata, atIndex(0));
              assertThat(objects).contains(expectedFileListLocation, atIndex(1));
            });
    checkFileListLocationCount((String) result.get(0)[1], 4);
  }

  @TestTemplate
  public void testProcedureWithInvalidInput() {
    String targetLocation = targetTableDir.toFile().toURI().toString();

    assertThatThrownBy(
            () -> sql("CALL %s.system.rewrite_table_path('%s')", catalogName, tableIdent))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "[REQUIRED_PARAMETER_NOT_FOUND] Cannot invoke routine `rewrite_table_path` because the parameter named `source_prefix` is required, but the routine call did not supply a value. Please update the routine call to supply an argument value (either positionally at index 0 or by name) and retry the query again. SQLSTATE: 4274K");
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_table_path('%s','%s')",
                    catalogName, tableIdent, targetLocation))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "[REQUIRED_PARAMETER_NOT_FOUND] Cannot invoke routine `rewrite_table_path` because the parameter named `target_prefix` is required, but the routine call did not supply a value. Please update the routine call to supply an argument value (either positionally at index 0 or by name) and retry the query again. SQLSTATE: 4274K");
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_table_path('%s', '%s','%s')",
                    catalogName, "notExists", targetLocation, targetLocation))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Couldn't load table");

    Table table = validationCatalog.loadTable(tableIdent);
    String v0Metadata = RewriteTablePathUtil.fileName(TableUtil.metadataFileLocation(table));
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_table_path("
                        + "table => '%s', "
                        + "source_prefix => '%s', "
                        + "target_prefix => '%s', "
                        + "start_version => '%s')",
                    catalogName, tableIdent, table.location(), targetLocation, "v20.metadata.json"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot find provided version file %s in metadata log.", "v20.metadata.json");
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_table_path("
                        + "table => '%s', "
                        + "source_prefix => '%s', "
                        + "target_prefix => '%s', "
                        + "start_version => '%s',"
                        + "end_version => '%s')",
                    catalogName,
                    tableIdent,
                    table.location(),
                    targetLocation,
                    v0Metadata,
                    "v11.metadata.json"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot find provided version file %s in metadata log.", "v11.metadata.json");
  }

  private void checkFileListLocationCount(String fileListLocation, long expectedFileCount) {
    long fileCount = spark.read().format("text").load(fileListLocation).count();
    assertThat(fileCount).isEqualTo(expectedFileCount);
  }
}
