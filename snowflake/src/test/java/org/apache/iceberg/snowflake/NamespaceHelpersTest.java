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
package org.apache.iceberg.snowflake;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class NamespaceHelpersTest {
  @Test
  public void testToSnowflakeIdentifierRoot() {
    Assertions.assertThat(NamespaceHelpers.toSnowflakeIdentifier(Namespace.empty()))
        .isEqualTo(SnowflakeIdentifier.ofRoot());
  }

  @Test
  public void testToSnowflakeIdentifierDatabase() {
    Assertions.assertThat(NamespaceHelpers.toSnowflakeIdentifier(Namespace.of("DB1")))
        .isEqualTo(SnowflakeIdentifier.ofDatabase("DB1"));
  }

  @Test
  public void testToSnowflakeIdentifierSchema() {
    Assertions.assertThat(NamespaceHelpers.toSnowflakeIdentifier(Namespace.of("DB1", "SCHEMA1")))
        .isEqualTo(SnowflakeIdentifier.ofSchema("DB1", "SCHEMA1"));
  }

  @Test
  public void testToSnowflakeIdentifierMaxNamespaceLevel() {
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                NamespaceHelpers.toSnowflakeIdentifier(
                    Namespace.of("DB1", "SCHEMA1", "THIRD_NS_LVL")))
        .withMessageContaining("max namespace level");
  }

  @Test
  public void testToSnowflakeIdentifierTable() {
    Assertions.assertThat(
            NamespaceHelpers.toSnowflakeIdentifier(TableIdentifier.of("DB1", "SCHEMA1", "TABLE1")))
        .isEqualTo(SnowflakeIdentifier.ofTable("DB1", "SCHEMA1", "TABLE1"));
  }

  @Test
  public void testToSnowflakeIdentifierTableBadNamespace() {
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                NamespaceHelpers.toSnowflakeIdentifier(
                    TableIdentifier.of(Namespace.of("DB1_WITHOUT_SCHEMA"), "TABLE1")))
        .withMessageContaining("must be at the SCHEMA level");
  }

  @Test
  public void testToIcebergNamespaceRoot() {
    Assertions.assertThat(NamespaceHelpers.toIcebergNamespace(SnowflakeIdentifier.ofRoot()))
        .isEqualTo(Namespace.empty());
  }

  @Test
  public void testToIcebergNamespaceDatabase() {
    Assertions.assertThat(
            NamespaceHelpers.toIcebergNamespace(SnowflakeIdentifier.ofDatabase("DB1")))
        .isEqualTo(Namespace.of("DB1"));
  }

  @Test
  public void testToIcebergNamespaceSchema() {
    Assertions.assertThat(
            NamespaceHelpers.toIcebergNamespace(SnowflakeIdentifier.ofSchema("DB1", "SCHEMA1")))
        .isEqualTo(Namespace.of("DB1", "SCHEMA1"));
  }

  @Test
  public void testToIcebergNamespaceTableFails() {
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                NamespaceHelpers.toIcebergNamespace(
                    SnowflakeIdentifier.ofTable("DB1", "SCHEMA1", "TABLE1")))
        .withMessageContaining("Cannot convert identifier");
  }

  @Test
  public void testToIcebergTableIdentifier() {
    Assertions.assertThat(
            NamespaceHelpers.toIcebergTableIdentifier(
                SnowflakeIdentifier.ofTable("DB1", "SCHEMA1", "TABLE1")))
        .isEqualTo(TableIdentifier.of("DB1", "SCHEMA1", "TABLE1"));
  }

  @Test
  public void testToIcebergTableIdentifierWrongType() {
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                NamespaceHelpers.toIcebergTableIdentifier(
                    SnowflakeIdentifier.ofSchema("DB1", "SCHEMA1")))
        .withMessageContaining("must be type TABLE");
  }
}
