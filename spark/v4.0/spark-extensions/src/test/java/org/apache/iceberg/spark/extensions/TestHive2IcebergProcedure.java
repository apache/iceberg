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
import org.apache.iceberg.ParameterizedTestExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestHive2IcebergProcedure extends ExtensionsTestBase {

  private static final String SOURCE_NAME = "spark_catalog.default.source";

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", SOURCE_NAME);
  }

  @TestTemplate
  public void testHive2Iceberg() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);
    List<Object[]> result =
        sql("CALL %s.system.hive_to_iceberg(source_table => '%s')", catalogName, SOURCE_NAME);
    assertThat(result).as("Hive2Iceberg action result should not be null").isNotNull();
    assertThat(result.get(0)[0])
        .as("First row should indicate successful execution")
        .isEqualTo(true);
    assertThat(result.get(0)[1]).as("Failure message should be 'N/A'").isEqualTo("N/A");
    assertThat(result.get(0)[2])
        .as("Latest Iceberg metadata version should not be null")
        .isNotNull();
  }

  @TestTemplate
  public void testHive2IcebergWithInvalidParallelism() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = Files.createTempDirectory(temp, "junit").toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        SOURCE_NAME, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", SOURCE_NAME);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", SOURCE_NAME);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.hive_to_iceberg(source_table => '%s', parallelism => %d)",
                    catalogName, SOURCE_NAME, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Parallelism should be larger than 0");
  }
}
