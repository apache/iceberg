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
package org.apache.iceberg.spark.udf;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.RESTCatalogServer;
import org.apache.iceberg.rest.RESTServerExtension;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestRestFunctionCatalogE2E extends TestBaseWithCatalog {

  @RegisterExtension
  public static final RESTServerExtension SERVER =
      new RESTServerExtension(Map.of(RESTCatalogServer.REST_PORT, RESTServerExtension.FREE_PORT));

  @BeforeEach
  public void before() {
    // Point the catalog to the REST server BEFORE base setup so the catalog picks it up
    String port = SERVER.config().get(RESTCatalogServer.REST_PORT);
    String baseUrl = "http://localhost:" + port;
    spark.conf().set("spark.sql.catalog." + catalogName + ".functions.rest.uri", baseUrl);
    super.before();
  }

  @TestTemplate
  public void testListAndExecuteFromOpenApiServer() throws Exception {
    // Ensure referenced table exists before function registration runs
    sql("CREATE TABLE fruits(name STRING, color STRING) USING PARQUET");

    FunctionCatalog functionCatalog =
        (FunctionCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    Identifier[] functions = functionCatalog.listFunctions(new String[] {});
    assertThat(functions).anyMatch(id -> id.name().equals("add_one_file"));
    assertThat(functions).anyMatch(id -> id.name().equals("fruits_by_color"));

    // Explicitly load functions to trigger registration through BaseCatalog.loadFunction
    for (Identifier id : functions) {
      functionCatalog.loadFunction(Identifier.of(new String[] {}, id.name()));
    }

    Object result = scalarSql("SELECT add_one_file(41)");
    assertThat(((Number) result).intValue()).isEqualTo(42);

    sql("INSERT INTO fruits VALUES ('apple','red'), ('plum','red'), ('banana','yellow')");
    List<Object[]> rows = sql("SELECT * FROM fruits_by_color('red') ORDER BY name");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)[0]).isEqualTo("apple");
    assertThat(rows.get(0)[1]).isEqualTo("red");
    assertThat(rows.get(1)[0]).isEqualTo("plum");
    assertThat(rows.get(1)[1]).isEqualTo("red");
  }
}
