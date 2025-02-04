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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.Table;
import org.apache.spark.sql.AnalysisException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestRewriteTablePathProcedure extends SparkExtensionsTestBase {
  @Rule public TemporaryFolder temp = new TemporaryFolder();

  public String staging = null;
  public String targetTableDir = null;

  public TestRewriteTablePathProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void setupTableLocation() throws Exception {
    this.staging = temp.newFolder("staging").toURI().toString();
    this.targetTableDir = temp.newFolder("targetTable").toURI().toString();
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testRewriteTablePathWithPositionalArgument() {
    Table table = validationCatalog.loadTable(tableIdent);
    String metadataJson =
        (((HasTableOperations) table).operations()).current().metadataFileLocation();

    List<Object[]> result =
        sql(
            "CALL %s.system.rewrite_table_path('%s', '%s', '%s')",
            catalogName, tableIdent, table.location(), targetTableDir);
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

  @Test
  public void testRewriteTablePathWithNamedArgument() {
    Table table = validationCatalog.loadTable(tableIdent);
    String v0Metadata =
        RewriteTablePathUtil.fileName(
            (((HasTableOperations) table).operations()).current().metadataFileLocation());
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    String v1Metadata =
        RewriteTablePathUtil.fileName(
            (((HasTableOperations) table).operations()).refresh().metadataFileLocation());

    String expectedFileListLocation = staging + "file-list";

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
            this.targetTableDir,
            table.location(),
            v1Metadata,
            v0Metadata,
            this.staging);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).as("Should return correct latest version").isEqualTo(v1Metadata);
    assertThat(result.get(0)[1])
        .as("Should return correct file_list_location")
        .isEqualTo(expectedFileListLocation);
    checkFileListLocationCount((String) result.get(0)[1], 4);
  }

  @Test
  public void testProcedureWithInvalidInput() {

    assertThatThrownBy(
            () -> sql("CALL %s.system.rewrite_table_path('%s')", catalogName, tableIdent))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Missing required parameters: [source_prefix,target_prefix]");
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_table_path('%s','%s')",
                    catalogName, tableIdent, this.targetTableDir))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Missing required parameters: [target_prefix]");
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_table_path('%s', '%s','%s')",
                    catalogName, "notExists", this.targetTableDir, this.targetTableDir))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Couldn't load table");

    Table table = validationCatalog.loadTable(tableIdent);
    String v0Metadata =
        RewriteTablePathUtil.fileName(
            (((HasTableOperations) table).operations()).current().metadataFileLocation());
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_table_path("
                        + "table => '%s', "
                        + "source_prefix => '%s', "
                        + "target_prefix => '%s', "
                        + "start_version => '%s')",
                    catalogName,
                    tableIdent,
                    table.location(),
                    this.targetTableDir,
                    "v20.metadata.json"))
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
                    this.targetTableDir,
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
