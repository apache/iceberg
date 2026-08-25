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

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Verifies the read side of the SCALAR index at the SQL level: once {@code build_scalar_index}
 * has populated an index, a subsequent equality query on the indexed column returns the correct
 * row -- functional correctness, which never depends on pruning actually kicking in. Whether
 * {@code FileScanTaskFilteringScan} (the mechanism that actually enforces pruning) narrows the
 * planned file set correctly is verified separately and in isolation by {@code
 * TestFileScanTaskFilteringScan}: {@code Dataset#inputFiles()} does not reflect Iceberg's
 * DataSourceV2 scans in this Spark version, so it is not a usable signal at this level.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestScalarIndexScanPruning extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testEqualityQueryReturnsCorrectRowAfterIndexBuild() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    // Separate INSERTs so the rows land in separate data files -- otherwise there is nothing for
    // the index to prune between.
    sql("INSERT INTO TABLE %s VALUES (1, 'aaa')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'bbb')", tableName);
    sql("INSERT INTO TABLE %s VALUES (3, 'ccc')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
            + " transform => 'HASH')",
        catalogName, tableIdent);

    List<Object[]> result = sql("SELECT id, data FROM %s WHERE data = 'bbb'", tableName);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo(2L);
    assertThat(result.get(0)[1]).isEqualTo("bbb");
  }

  @TestTemplate
  public void testEqualityQueryOnAbsentValueReturnsNoRows() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'aaa')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'bbb')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
            + " transform => 'HASH')",
        catalogName, tableIdent);

    List<Object[]> result = sql("SELECT id FROM %s WHERE data = 'does-not-exist'", tableName);
    assertThat(result).isEmpty();
  }

  @TestTemplate
  public void testQueryStillCorrectWithoutAnyIndex() {
    // Sanity check that nothing about tryPruneUsingScalarIndex breaks a table with no index at
    // all -- this is the common case (most tables), so it must be a complete no-op.
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'aaa'), (2, 'bbb')", tableName);

    List<Object[]> result = sql("SELECT id FROM %s WHERE data = 'bbb'", tableName);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo(2L);
  }
}
