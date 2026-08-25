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
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexIdentifier;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.spark.SparkIndexCatalogs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBuildScalarIndexProcedure extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testBuildHashIndexOnStringColumn() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, 'aaa'), (2, 'bbb'), (3, 'ccc'), (4, 'ddd'), (5, 'eee')",
        tableName);

    List<Object[]> output =
        sql(
            "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
                + " transform => 'HASH')",
            catalogName, tableIdent);

    assertThat(output).hasSize(1);
    Object[] row = output.get(0);
    // index_location, leaf_file_count, record_count
    assertThat((String) row[0]).isNotBlank();
    assertThat((int) row[1]).isGreaterThan(0);
    assertThat((long) row[2]).isEqualTo(5L);

    Table table = validationCatalog.loadTable(tableIdent);
    IndexCatalog indexCatalog = SparkIndexCatalogs.get().catalogFor(table);
    // Must match how BuildScalarIndexProcedure derives its IndexIdentifier: from the core Table's
    // own name (table.name()), not the Spark catalog Identifier -- SparkScanBuilder on the read
    // side only has the core Table, so both sides need a source they can each compute
    // independently, and table.name() is it.
    IndexIdentifier indexIdent =
        IndexIdentifier.of(
            org.apache.iceberg.catalog.TableIdentifier.parse(table.name()), "data_idx");
    assertThat(indexCatalog.indexExists(indexIdent)).isTrue();

    IndexMetadata metadata = indexCatalog.loadIndex(indexIdent);
    assertThat(metadata.type()).isEqualTo("SCALAR");
    assertThat(metadata.transformFunction()).isEqualTo("HASH");
    assertThat(metadata.snapshots()).hasSize(1);
  }

  @TestTemplate
  public void testBuildIdentityIndexOnLongColumn() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    List<Object[]> output =
        sql(
            "CALL %s.system.build_scalar_index(table => '%s', columns => array('id'),"
                + " transform => 'IDENTITY')",
            catalogName, tableIdent);

    assertThat(output).hasSize(1);
    assertThat((long) output.get(0)[2]).isEqualTo(3L);
  }

  @TestTemplate
  public void testIdentityRejectsStringColumn() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
                        + " transform => 'IDENTITY')",
                    catalogName, tableIdent))
        .hasMessageContaining("IDENTITY transform requires a numeric");
  }

  @TestTemplate
  public void testRejectsMultipleColumns() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.build_scalar_index(table => '%s', columns => array('id',"
                        + " 'data'), transform => 'HASH')",
                    catalogName, tableIdent))
        .hasMessageContaining("supports exactly one key column");
  }

  @TestTemplate
  public void testCustomBucketCountOption() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
        tableName);

    List<Object[]> output =
        sql(
            "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
                + " transform => 'HASH', options => map('hash.num-buckets', '8',"
                + " 'target-leaf-files', '2'))",
            catalogName, tableIdent);

    assertThat((long) output.get(0)[2]).isEqualTo(4L);
  }
}
