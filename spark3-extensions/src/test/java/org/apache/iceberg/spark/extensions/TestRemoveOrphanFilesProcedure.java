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

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Test;

public class TestRemoveOrphanFilesProcedure extends SparkExtensionsTestBase {

  public TestRemoveOrphanFilesProcedure(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    if (catalogName.equals("spark_catalog")) {
      // Spark will drop the table using v1 without cleaning the table location
      validationCatalog.dropTable(tableIdent);
    } else {
      sql("DROP TABLE IF EXISTS %s", tableName);
    }
  }

  @Test
  public void testRemoveOrphanFilesInEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    List<Object[]> output = sql(
        "CALL %s.system.remove_orphan_files('%s', '%s')",
        catalogName, tableIdent.namespace(), tableIdent.name());
    assertEquals("Procedure output must match", ImmutableList.of(), output);

    assertEquals("Should have no rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testRemoveOrphanFilesWithTooShortInterval() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    AssertHelpers.assertThrows("Should reject too short interval",
        IllegalArgumentException.class, "Cannot remove orphan files with an interval less than",
        () -> sql("CALL %s.system.remove_orphan_files('%s', '%s', TIMESTAMP '%s')",
            catalogName, tableIdent.namespace(), tableIdent.name(), LocalDateTime.now()));

    List<Object[]> output = sql(
        "CALL %s.system.remove_orphan_files(" +
            "namespace => '%s'," +
            "table => '%s'," +
            "older_than => TIMESTAMP '%s'," +
            "validate_interval => false)",
        catalogName, tableIdent.namespace(), tableIdent.name(), LocalDateTime.now());
    assertEquals("Procedure output must match", ImmutableList.of(), output);
  }

  @Test
  public void testInvalidRemoveOrphanFilesCases() {
    AssertHelpers.assertThrows("Should not allow mixed args",
        AnalysisException.class, "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.remove_orphan_files('n', table => 't')", catalogName));

    AssertHelpers.assertThrows("Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class, "not found",
        () -> sql("CALL %s.custom.remove_orphan_files('n', 't')", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.remove_orphan_files('n')", catalogName));

    AssertHelpers.assertThrows("Should reject calls with invalid arg types",
        RuntimeException.class, "Couldn't parse identifier",
        () -> sql("CALL %s.system.remove_orphan_files('n', 2.2)", catalogName));

    AssertHelpers.assertThrows("Should reject empty namespace",
        IllegalArgumentException.class, "Namespace cannot be empty",
        () -> sql("CALL %s.system.remove_orphan_files('', 't')", catalogName));

    AssertHelpers.assertThrows("Should reject empty table name",
        IllegalArgumentException.class, "Table name cannot be empty",
        () -> sql("CALL %s.system.remove_orphan_files('n', '')", catalogName));
  }
}
