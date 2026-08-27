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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestMetadataTableType {

  @ParameterizedTest
  @EnumSource(MetadataTableType.class)
  void singleLevelNamespaceIsNotAMetadataTable(MetadataTableType type) {
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("ns"), type.name());
    // a single namespace level names a regular table whose base table has no namespace
    assertThat(MetadataTableType.from(identifier)).isNull();
    // the plain name still resolves to the metadata table type
    assertThat(MetadataTableType.from(identifier.name())).isEqualTo(type);
  }

  @ParameterizedTest
  @EnumSource(MetadataTableType.class)
  void nestedNamespaceIsAMetadataTable(MetadataTableType type) {
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("ns", "tbl"), type.name());
    assertThat(MetadataTableType.from(identifier)).isEqualTo(type);
    assertThat(MetadataTableType.from(identifier.name())).isEqualTo(type);
  }

  @ParameterizedTest
  @EnumSource(MetadataTableType.class)
  void identifierWithoutNamespaceIsNotAMetadataTable(MetadataTableType type) {
    TableIdentifier identifier = TableIdentifier.of(type.name());
    assertThat(MetadataTableType.from(identifier)).isNull();
  }
}
