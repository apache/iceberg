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

package org.apache.iceberg;

import java.util.stream.Stream;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestCatalogType {

  @ParameterizedTest
  @MethodSource("mixedCaseCatalogTypeImpls")
  public void testCaseInsensitivity(Arguments inputCase) {
    String inputType = (inputCase.get()[0] != null) ? (String) inputCase.get()[0] : null;
    Assertions.assertThat(CatalogType.of(inputType).impl())
        .as("Unexpected catalog impl class detected for %s. This change can break client configuration.",
            inputType)
        .isEqualTo(inputCase.get()[1]);
  }

  static Stream<Arguments> mixedCaseCatalogTypeImpls() {
    return Stream.of(Arguments.of("hadoop", HadoopCatalog.class.getName()),
        Arguments.of("HADOOP", HadoopCatalog.class.getName()),
        Arguments.of("HaDoOp", HadoopCatalog.class.getName()),
        Arguments.of("jdbc", JdbcCatalog.class.getName()),
        Arguments.of("JDBC", JdbcCatalog.class.getName()),
        Arguments.of("nessie", "org.apache.iceberg.nessie.NessieCatalog"),
        Arguments.of("NESSIE", "org.apache.iceberg.nessie.NessieCatalog"),
        Arguments.of("dynamodb", "org.apache.iceberg.aws.dynamodb.DynamoDbCatalog"),
        Arguments.of("DYNAMODB", "org.apache.iceberg.aws.dynamodb.DynamoDbCatalog"),
        Arguments.of("glue", "org.apache.iceberg.aws.glue.GlueCatalog"),
        Arguments.of("GLUE", "org.apache.iceberg.aws.glue.GlueCatalog"));
  }

  @Test
  public void testUnknownCatalog() {
    Assertions.assertThatThrownBy(() -> CatalogType.of("FOO"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "Unknown catalog type: FOO. Valid values are [hive, hadoop, glue, nessie, dynamodb, jdbc]. To use a custom catalog, please use the [catalog-impl] conf instead of [type] with the value set to a fully qualified catalog impl class name.");
  }

  @ParameterizedTest
  @MethodSource("typeWithNames")
  public void testTypeNames(Arguments inputCase) {
    CatalogType type = (CatalogType) inputCase.get()[0];
    Assertions.assertThat(type.value())
        .as("Unexpected typeName detected for %s. This change can break client configuration.",
            type)
        .isEqualTo(inputCase.get()[1]);
  }

  static Stream<Arguments> typeWithNames() {
    return Stream.of(
        Arguments.of(CatalogType.DYNAMODB, "dynamodb"),
        Arguments.of(CatalogType.GLUE, "glue"),
        Arguments.of(CatalogType.HADOOP, "hadoop"),
        Arguments.of(CatalogType.HIVE, "hive"),
        Arguments.of(CatalogType.JDBC, "jdbc"),
        Arguments.of(CatalogType.NESSIE, "nessie"));
  }
}
