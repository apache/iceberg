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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * End-to-end test for REST-backed scalar SQL UDFs executed via Iceberg Spark extensions.
 *
 * <p>Stage 1: scalar only. UDTF/table functions will be added in a follow-up (stage 2).
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestRestScalarFunctionCatalog extends ExtensionsTestBase {

  private static final String FUNCTION_NAMESPACE = "udf_ns";

  // Catalog-backed (production) REST functions are only supported for the REST catalog type.
  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.REST.catalogName(),
        SparkCatalogConfig.REST.implementation(),
        ImmutableMap.builder()
            .putAll(SparkCatalogConfig.REST.properties())
            .put(CatalogProperties.URI, restCatalog.properties().get(CatalogProperties.URI))
            .build()
      }
    };
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String ADD_ONE_SPEC =
      "{"
          + "\"function-uuid\":\"123\","
          + "\"format-version\":1,"
          + "\"doc\":\"Add one UDF\","
          + "\"parameter-names\":[{\"name\":\"x\"}],"
          + "\"definitions\":[{"
          + "  \"definition-id\":\"(int)\","
          + "  \"parameters\":[{\"type\":\"int\"}],"
          + "  \"return-type\":\"int\","
          + "  \"doc\":\"Adds one to x\","
          + "  \"current-version-id\":1,"
          + "  \"versions\":[{"
          + "    \"version-id\":1,"
          + "    \"timestamp-ms\":0,"
          + "    \"deterministic\":true,"
          + "    \"representations\":[{"
          + "      \"type\":\"sql\","
          + "      \"dialect\":\"spark\","
          + "      \"body\":\"x + 1\""
          + "    }]"
          + "  }]"
          + "}],"
          + "\"definition-log\":[{\"timestamp-ms\":0,\"definition-versions\":[{\"definition-id\":\"(int)\",\"version-id\":1}]}],"
          + "\"secure\":false"
          + "}";

  @BeforeEach
  public void before() {
    super.before();

    // Ensure unqualified function names resolve against the parameterized Iceberg catalog
    spark.conf().set("spark.sql.defaultCatalog", catalogName);

    sql("CREATE NAMESPACE IF NOT EXISTS %s", catalogName + "." + FUNCTION_NAMESPACE);

    // Register a function spec in the REST catalog test server.
    ObjectNode spec;
    try {
      spec = (ObjectNode) MAPPER.readTree(ADD_ONE_SPEC);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse function spec JSON", e);
    }

    REST_SERVER_EXTENSION.putFunction(Namespace.of(FUNCTION_NAMESPACE), "add_one_file", spec);
  }

  @TestTemplate
  public void testListAndExecuteScalarFromRest() throws Exception {
    FunctionCatalog functionCatalog = castToFunctionCatalog(catalogName);
    Identifier[] functions = functionCatalog.listFunctions(new String[] {FUNCTION_NAMESPACE});
    long userCount =
        Arrays.stream(functions).filter(id -> id.name().equals("add_one_file")).count();
    assertThat(userCount).isEqualTo(1);

    Object result = scalarSql("SELECT %s.%s.add_one_file(41)", catalogName, FUNCTION_NAMESPACE);
    assertThat(((Number) result).intValue()).isEqualTo(42);
  }

  private FunctionCatalog castToFunctionCatalog(String name) {
    return (FunctionCatalog) spark.sessionState().catalogManager().catalog(name);
  }
}
