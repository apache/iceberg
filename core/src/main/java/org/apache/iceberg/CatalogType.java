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

  CatalogType(String catalogImpl) {
    this.catalogImpl = catalogImpl;
  }

  public String getTypeName() {
    return name().toLowerCase(Locale.ENGLISH);
  }

  public String getCatalogImpl() {
    return catalogImpl;
  }

  /**
   * Returns catalog-impl class name for the input type name.
   *
   * @param inputType Non-null input type.
   * @return catalog-impl class name
   */
  public static String getCatalogImpl(String inputType) {
    try {
      CatalogType type = CatalogType.valueOf(inputType.toUpperCase(Locale.ENGLISH));
      return type.getCatalogImpl();
    } catch (IllegalArgumentException e) {
      throw new UnsupportedOperationException(
          String.format("Unknown catalog type: %s. Valid values are [%s]", inputType,
              Arrays.stream(values()).map(CatalogType::getTypeName).collect(Collectors.joining(", "))));
    }
  }
}
