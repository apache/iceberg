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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.functions.IcebergVersionFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFunctionCatalog extends TestBaseWithCatalog {
  private static final String[] EMPTY_NAMESPACE = new String[] {};
  private static final String[] SYSTEM_NAMESPACE = new String[] {"system"};
  private static final String[] DEFAULT_NAMESPACE = new String[] {"default"};
  private static final String[] DB_NAMESPACE = new String[] {"db"};
  private FunctionCatalog asFunctionCatalog;

  @BeforeEach
  public void before() {
    super.before();
    this.asFunctionCatalog = castToFunctionCatalog(catalogName);
    sql("CREATE NAMESPACE IF NOT EXISTS %s", catalogName + ".default");
  }

  @AfterEach
  public void dropDefaultNamespace() {
    sql("DROP NAMESPACE IF EXISTS %s", catalogName + ".default");
  }

  @TestTemplate
  public void testListFunctionsViaCatalog() throws NoSuchNamespaceException {
    assertThat(asFunctionCatalog.listFunctions(EMPTY_NAMESPACE))
        .anyMatch(func -> "iceberg_version".equals(func.name()));

    assertThat(asFunctionCatalog.listFunctions(SYSTEM_NAMESPACE))
        .anyMatch(func -> "iceberg_version".equals(func.name()));

    assertThat(asFunctionCatalog.listFunctions(DEFAULT_NAMESPACE))
        .as("Listing functions in an existing namespace that's not system should not throw")
        .isEqualTo(new Identifier[0]);

    assertThatThrownBy(() -> asFunctionCatalog.listFunctions(DB_NAMESPACE))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageStartingWith("[SCHEMA_NOT_FOUND] The schema `db` cannot be found.");

    // POC: registering a SQL UDF should surface via the Iceberg function catalog listing
    org.apache.iceberg.spark.udf.SparkUDFRegistrar.registerFromJson(
        spark,
        "add_one",
        "{\n"
            + "  \"function-uuid\": \"42fd3f91-bc10-41c1-8a52-92b57dd0a9b2\",\n"
            + "  \"format-version\": 1,\n"
            + "  \"definitions\": [\n"
            + "    {\n"
            + "      \"definition-id\": \"(int)\",\n"
            + "      \"parameters\": [ { \"name\": \"x\", \"type\": \"int\", \"doc\": \"Input integer\" } ],\n"
            + "      \"return-type\": \"int\",\n"
            + "      \"doc\": \"Add one to the input integer\",\n"
            + "      \"versions\": [\n"
            + "        {\n"
            + "          \"version-id\": 1,\n"
            + "          \"deterministic\": true,\n"
            + "          \"representations\": [ { \"dialect\": \"trino\", \"body\": \"x + 2\" } ],\n"
            + "          \"timestamp-ms\": 1734507000123\n"
            + "        },\n"
            + "        {\n"
            + "          \"version-id\": 2,\n"
            + "          \"deterministic\": true,\n"
            + "          \"representations\": [ { \"dialect\": \"trino\", \"parameters\": [{ \"name\": \"val\", \"type\": \"int\" }], \"body\": \"val + 1\" }, { \"dialect\": \"spark\", \"parameters\": [{ \"name\": \"x\", \"type\": \"int\" }], \"body\": \"x + 1\" } ],\n"
            + "          \"timestamp-ms\": 1735507000124\n"
            + "        }\n"
            + "      ],\n"
            + "      \"current-version-id\": 2\n"
            + "    },\n"
            + "    {\n"
            + "      \"definition-id\": \"(float)\",\n"
            + "      \"parameters\": [ { \"name\": \"x\", \"type\": \"float\", \"doc\": \"Input float\" } ],\n"
            + "      \"return-type\": \"float\",\n"
            + "      \"doc\": \"Add one to the input float\",\n"
            + "      \"versions\": [\n"
            + "        {\n"
            + "          \"version-id\": 1,\n"
            + "          \"deterministic\": true,\n"
            + "          \"representations\": [ { \"dialect\": \"trino\", \"body\": \"x + 1.0\" }, { \"dialect\": \"spark\", \"body\": \"x + 1.0\" } ],\n"
            + "          \"timestamp-ms\": 1734507001123\n"
            + "        }\n"
            + "      ],\n"
            + "      \"current-version-id\": 1\n"
            + "    }\n"
            + "  ],\n"
            + "  \"definition-log\": [\n"
            + "    {\n"
            + "      \"timestamp-ms\": 1734507001123,\n"
            + "      \"definition-versions\": [ { \"definition-id\": \"(int)\", \"version-id\": 1 }, { \"definition-id\": \"(float)\", \"version-id\": 1 } ]\n"
            + "    },\n"
            + "    {\n"
            + "      \"timestamp-ms\": 1735507000124,\n"
            + "      \"definition-versions\": [ { \"definition-id\": \"(int)\", \"version-id\": 2 }, { \"definition-id\": \"(float)\", \"version-id\": 1 } ]\n"
            + "    }\n"
            + "  ],\n"
            + "  \"doc\": \"Overloaded scalar UDF for integer and float inputs\",\n"
            + "  \"secure\": false\n"
            + "}");

    Identifier[] functions = asFunctionCatalog.listFunctions(EMPTY_NAMESPACE);
    assertThat(functions).anyMatch(func -> "add_one".equals(func.name()));

    // Registrar registers both (int) and (float) overloads under the same name; Spark shows one
    // identifier per function name. To ensure both overloads are executable, call both.
    Object r1 = scalarSql("SELECT add_one(41)");
    assertThat(((Number) r1).intValue()).isEqualTo(42);
    Object r2 = scalarSql("SELECT add_one(1.5)");
    assertThat(((Number) r2).doubleValue()).isEqualTo(2.5d);
  }

  @TestTemplate
  public void testLoadFunctions() throws NoSuchFunctionException {
    for (String[] namespace : ImmutableList.of(EMPTY_NAMESPACE, SYSTEM_NAMESPACE)) {
      Identifier identifier = Identifier.of(namespace, "iceberg_version");
      UnboundFunction func = asFunctionCatalog.loadFunction(identifier);

      assertThat(func)
          .isNotNull()
          .isInstanceOf(UnboundFunction.class)
          .isExactlyInstanceOf(IcebergVersionFunction.class);
    }

    assertThatThrownBy(
            () ->
                asFunctionCatalog.loadFunction(Identifier.of(DEFAULT_NAMESPACE, "iceberg_version")))
        .isInstanceOf(NoSuchFunctionException.class)
        .hasMessageStartingWith(
            String.format(
                "[ROUTINE_NOT_FOUND] The routine default.iceberg_version cannot be found"));

    Identifier undefinedFunction = Identifier.of(SYSTEM_NAMESPACE, "undefined_function");
    assertThatThrownBy(() -> asFunctionCatalog.loadFunction(undefinedFunction))
        .isInstanceOf(NoSuchFunctionException.class)
        .hasMessageStartingWith(
            String.format(
                "[ROUTINE_NOT_FOUND] The routine system.undefined_function cannot be found"));

    assertThatThrownBy(() -> sql("SELECT undefined_function(1, 2)"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "[UNRESOLVED_ROUTINE] Cannot resolve routine `undefined_function` on search path");
  }

  @TestTemplate
  public void testCallingFunctionInSQLEndToEnd() {
    String buildVersion = IcebergBuild.version();

    assertThat(scalarSql("SELECT %s.system.iceberg_version()", catalogName))
        .as(
            "Should be able to use the Iceberg version function from the fully qualified system namespace")
        .isEqualTo(buildVersion);

    assertThat(scalarSql("SELECT %s.iceberg_version()", catalogName))
        .as(
            "Should be able to use the Iceberg version function when fully qualified without specifying a namespace")
        .isEqualTo(buildVersion);

    sql("USE %s", catalogName);

    assertThat(scalarSql("SELECT system.iceberg_version()"))
        .as(
            "Should be able to call iceberg_version from system namespace without fully qualified name when using Iceberg catalog")
        .isEqualTo(buildVersion);

    assertThat(scalarSql("SELECT iceberg_version()"))
        .as(
            "Should be able to call iceberg_version from empty namespace without fully qualified name when using Iceberg catalog")
        .isEqualTo(buildVersion);
  }

  private FunctionCatalog castToFunctionCatalog(String name) {
    return (FunctionCatalog) spark.sessionState().catalogManager().catalog(name);
  }
}
