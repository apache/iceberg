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

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

public enum CatalogType {
  HIVE("org.apache.iceberg.hive.HiveCatalog"),
  HADOOP("org.apache.iceberg.hadoop.HadoopCatalog"),
  GLUE("org.apache.iceberg.aws.glue.GlueCatalog"),
  NESSIE("org.apache.iceberg.nessie.NessieCatalog"),
  DYNAMODB("org.apache.iceberg.aws.dynamodb.DynamoDbCatalog"),
  JDBC("org.apache.iceberg.jdbc.JdbcCatalog");

  private final String catalogImpl;
  private final String value;

  CatalogType(String catalogImpl) {
    this.catalogImpl = catalogImpl;
    this.value = name().toLowerCase(Locale.ENGLISH);
  }

  public String value() {
    return value;
  }

  public String impl() {
    return catalogImpl;
  }

  /**
   * Returns catalog-impl class name for the input type name.
   *
   * @param inputType Non-null input type.
   * @return catalog-impl class name
   */
  public static CatalogType of(String inputType) {
    try {
      return CatalogType.valueOf(inputType.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new UnsupportedOperationException(
          String.format(
              "Unknown catalog type: %s. Valid values are [%s]. To use a custom catalog, please " +
                  "use the [%s] conf instead of [%s] with the value set to a fully qualified catalog impl class name.",
              inputType,
              Arrays.stream(values()).map(CatalogType::value).collect(Collectors.joining(", ")),
              CatalogProperties.CATALOG_IMPL,
              CatalogProperties.CATALOG_TYPE));
    }
  }
}
