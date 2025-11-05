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
import org.apache.iceberg.spark.TestBase;
import org.junit.jupiter.api.Test;

public class TestSparkUDFRegistrar extends TestBase {

  @Test
  public void testRegisterScalarUdf() {
    String json =
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
            + "          \"representations\": [\n"
            + "            { \"dialect\": \"trino\", \"parameters\": [{ \"name\": \"val\", \"type\": \"int\" }], \"body\": \"val + 1\" },\n"
            + "            { \"dialect\": \"spark\", \"parameters\": [{ \"name\": \"x\", \"type\": \"int\" }], \"body\": \"x + 1\" }\n"
            + "          ],\n"
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
            + "          \"representations\": [ { \"dialect\": \"trino\", \"body\": \"x + 1.0\" } ],\n"
            + "          \"timestamp-ms\": 1734507001123\n"
            + "        }\n"
            + "      ],\n"
            + "      \"current-version-id\": 1\n"
            + "    }\n"
            + "  ],\n"
            + "  \"definition-log\": [\n"
            + "    {\n"
            + "      \"timestamp-ms\": 1734507001123,\n"
            + "      \"definition-versions\": [\n"
            + "        { \"definition-id\": \"(int)\", \"version-id\": 1 },\n"
            + "        { \"definition-id\": \"(float)\", \"version-id\": 1 }\n"
            + "      ]\n"
            + "    },\n"
            + "    {\n"
            + "      \"timestamp-ms\": 1735507000124,\n"
            + "      \"definition-versions\": [\n"
            + "        { \"definition-id\": \"(int)\", \"version-id\": 2 },\n"
            + "        { \"definition-id\": \"(float)\", \"version-id\": 1 }\n"
            + "      ]\n"
            + "    }\n"
            + "  ],\n"
            + "  \"doc\": \"Overloaded scalar UDF for integer and float inputs\",\n"
            + "  \"secure\": false\n"
            + "}";

    SparkUDFRegistrar.registerFromJson(spark, "add_one", json);
    Object result = scalarSql("SELECT add_one(41)");
    assertThat(((Number) result).intValue()).isEqualTo(42);
  }

  @Test
  public void testRegisterUdtf() {
    sql("CREATE TABLE fruits (name STRING, color STRING) USING PARQUET");
    sql("INSERT INTO fruits VALUES ('apple','red'),('banana','yellow'),('plum','red')");

    String json =
        "{\n"
            + "  \"function-uuid\": \"8a7fa39a-6d8f-4a2f-9d8d-3f3a8f3c2a10\",\n"
            + "  \"format-version\": 1,\n"
            + "  \"definitions\": [\n"
            + "    {\n"
            + "      \"definition-id\": \"(string)\",\n"
            + "      \"parameters\": [ { \"name\": \"c\", \"type\": \"string\", \"doc\": \"Color of fruits\" } ],\n"
            + "      \"return-type\": { \"type\": \"struct\", \"fields\": [ { \"id\": 1, \"name\": \"name\", \"type\": \"string\" }, { \"id\": 2, \"name\": \"color\", \"type\": \"string\" } ] },\n"
            + "      \"function-type\": \"udtf\",\n"
            + "      \"doc\": \"Return fruits of a specific color from the fruits table\",\n"
            + "      \"versions\": [\n"
            + "        {\n"
            + "          \"version-id\": 1,\n"
            + "          \"deterministic\": true,\n"
            + "          \"representations\": [ { \"dialect\": \"trino\", \"body\": \"SELECT name, color FROM fruits WHERE color = c\" }, { \"dialect\": \"spark\", \"body\": \"SELECT name, color FROM fruits WHERE color = c\" } ],\n"
            + "          \"timestamp-ms\": 1734508000123\n"
            + "        }\n"
            + "      ],\n"
            + "      \"current-version-id\": 1\n"
            + "    }\n"
            + "  ],\n"
            + "  \"definition-log\": [\n"
            + "    {\n"
            + "      \"timestamp-ms\": 1734508000123,\n"
            + "      \"definition-versions\": [ { \"definition-id\": \"(string)\", \"version-id\": 1 } ]\n"
            + "    }\n"
            + "  ],\n"
            + "  \"doc\": \"UDTF returning (name, color) rows filtered by the given color\",\n"
            + "  \"secure\": false\n"
            + "}";

    SparkUDFRegistrar.registerFromJson(spark, "fruits_by_color", json);

    List<Object[]> rows = sql("SELECT * FROM fruits_by_color('red') ORDER BY name");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)[0]).isEqualTo("apple");
    assertThat(rows.get(0)[1]).isEqualTo("red");
    assertThat(rows.get(1)[0]).isEqualTo("plum");
    assertThat(rows.get(1)[1]).isEqualTo("red");
  }

  @Test
  public void testRegisterScalarUdfFromFile() {
    java.nio.file.Path path =
        java.nio.file.Paths.get(
                "..",
                "..",
                "..",
                "core",
                "src",
                "main",
                "java",
                "org",
                "apache",
                "iceberg",
                "udf",
                "examples",
                "add_one.json")
            .toAbsolutePath()
            .normalize();

    SparkUDFRegistrar.registerFromJsonFile(spark, "add_one_file", path);
    Object result = scalarSql("SELECT add_one_file(41)");
    assertThat(((Number) result).intValue()).isEqualTo(42);
  }
}
