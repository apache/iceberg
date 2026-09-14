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
package org.apache.iceberg.flink.sink.dynamic;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestDynamicRecordProcessor {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final Schema SCHEMA_WITH_IDENTIFIER =
      new Schema(
          Lists.newArrayList(
              Types.NestedField.required(1, "id", Types.IntegerType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get())),
          Sets.newHashSet(1));

  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("default", "table");
  private static final String BRANCH = SnapshotRef.MAIN_BRANCH;

  @Test
  void testForwardEligibleWhenNoEqualityFields() {
    DynamicRecord record = recordWithDistributionMode(SCHEMA, null);

    assertThat(DynamicRecordProcessor.isForwardEligible(record)).isTrue();
  }

  @Test
  void testNotForwardEligibleWhenDistributionModeSet() {
    DynamicRecord record = recordWithDistributionMode(SCHEMA, DistributionMode.NONE);

    assertThat(DynamicRecordProcessor.isForwardEligible(record)).isFalse();
  }

  @Test
  void testNotForwardEligibleWhenUserEqualityFieldsSet() {
    DynamicRecord record = recordWithDistributionMode(SCHEMA, null);
    record.setEqualityFields(Collections.singleton("id"));

    assertThat(DynamicRecordProcessor.isForwardEligible(record)).isFalse();
  }

  @Test
  void testNotForwardEligibleWhenSchemaHasIdentifierFields() {
    DynamicRecord record = recordWithDistributionMode(SCHEMA_WITH_IDENTIFIER, null);

    assertThat(DynamicRecordProcessor.isForwardEligible(record)).isFalse();
  }

  @Test
  void testNotForwardEligibleWhenEmptyEqualityFieldsButIdentifierFieldsPresent() {
    DynamicRecord record = recordWithDistributionMode(SCHEMA_WITH_IDENTIFIER, null);
    record.setEqualityFields(Collections.emptySet());

    assertThat(DynamicRecordProcessor.isForwardEligible(record)).isFalse();
  }

  private static DynamicRecord recordWithDistributionMode(
      Schema schema, DistributionMode distributionMode) {
    return new DynamicRecord(
        TABLE_IDENTIFIER,
        BRANCH,
        schema,
        GenericRowData.of(1, StringData.fromString("foo")),
        PartitionSpec.unpartitioned(),
        distributionMode,
        2);
  }
}
