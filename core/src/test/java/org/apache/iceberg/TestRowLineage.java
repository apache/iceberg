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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestRowLineage {

  private TableMetadata.Builder testMetadataBuilder() {
    return TableMetadata.buildFromEmpty(TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE)
        .enableRowLineage();
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testRowLineageSupported(int formatVersion) {
    if (formatVersion == TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE) {
      assertThat(TableMetadata.buildFromEmpty(formatVersion).enableRowLineage()).isNotNull();
    } else {
      IllegalArgumentException notSupported =
          assertThrows(
              IllegalArgumentException.class,
              () -> TableMetadata.buildFromEmpty(formatVersion).enableRowLineage());
      assertThat(notSupported.getMessage()).contains("Cannot use row lineage");
    }
  }

  @Test
  public void testLastRowIdMustIncrease() {
    assertThat(testMetadataBuilder().withLastRowId(TableMetadata.INITIAL_ROW_ID + 5)).isNotNull();
    IllegalArgumentException noDecrease =  assertThrows(
        IllegalArgumentException.class,
        () -> testMetadataBuilder().withLastRowId(TableMetadata.INITIAL_ROW_ID - 5));
    assertThat(noDecrease.getMessage()).contains("Cannot decrease last-row-id");
  }
}
