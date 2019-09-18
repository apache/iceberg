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

import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.Test;

public class TestPartitionSpecValidation {
  private static final Schema SCHEMA = new Schema(
      NestedField.required(1, "id", Types.LongType.get()),
      NestedField.required(2, "ts", Types.TimestampType.withZone()),
      NestedField.required(3, "another_ts", Types.TimestampType.withZone()),
      NestedField.required(4, "d", Types.TimestampType.withZone()),
      NestedField.required(5, "another_d", Types.TimestampType.withZone())
  );

  @Test
  public void testMultipleTimestampPartitions() {
    AssertHelpers.assertThrows("Should not allow year(ts) and year(ts)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).year("ts").year("ts").build());
    AssertHelpers.assertThrows("Should not allow year(ts) and month(ts)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).year("ts").month("ts").build());
    AssertHelpers.assertThrows("Should not allow year(ts) and day(ts)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).year("ts").day("ts").build());
    AssertHelpers.assertThrows("Should not allow year(ts) and hour(ts)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).year("ts").hour("ts").build());

    AssertHelpers.assertThrows("Should not allow month(ts) and month(ts)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).month("ts").month("ts").build());
    AssertHelpers.assertThrows("Should not allow month(ts) and day(ts)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).month("ts").day("ts").build());
    AssertHelpers.assertThrows("Should not allow month(ts) and hour(ts)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).month("ts").hour("ts").build());

    AssertHelpers.assertThrows("Should not allow day(ts) and day(ts)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).day("ts").day("ts").build());
    AssertHelpers.assertThrows("Should not allow day(ts) and hour(ts)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).day("ts").hour("ts").build());

    AssertHelpers.assertThrows("Should not allow hour(ts) and hour(ts)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).hour("ts").hour("ts").build());
  }

  @Test
  public void testMultipleDatePartitions() {
    AssertHelpers.assertThrows("Should not allow year(d) and year(d)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).year("d").year("d").build());
    AssertHelpers.assertThrows("Should not allow year(d) and month(d)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).year("d").month("d").build());
    AssertHelpers.assertThrows("Should not allow year(d) and day(d)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).year("d").day("d").build());

    AssertHelpers.assertThrows("Should not allow month(d) and month(d)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).month("d").month("d").build());
    AssertHelpers.assertThrows("Should not allow month(d) and day(d)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).month("d").day("d").build());

    AssertHelpers.assertThrows("Should not allow day(d) and day(d)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).day("d").day("d").build());
  }

  @Test
  public void testMultipleTimestampPartitionsWithDifferentSourceColumns() {
    PartitionSpec.builderFor(SCHEMA).year("ts").year("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).year("ts").month("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).year("ts").day("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).year("ts").hour("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).month("ts").month("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).month("ts").day("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).month("ts").hour("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).day("ts").day("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).day("ts").hour("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).hour("ts").hour("another_ts").build();
  }

  @Test
  public void testMultipleDatePartitionsWithDifferentSourceColumns() {
    PartitionSpec.builderFor(SCHEMA).year("d").year("another_d").build();
    PartitionSpec.builderFor(SCHEMA).year("d").month("another_d").build();
    PartitionSpec.builderFor(SCHEMA).year("d").day("another_d").build();
    PartitionSpec.builderFor(SCHEMA).year("d").hour("another_d").build();
    PartitionSpec.builderFor(SCHEMA).month("d").month("another_d").build();
    PartitionSpec.builderFor(SCHEMA).month("d").day("another_d").build();
    PartitionSpec.builderFor(SCHEMA).month("d").hour("another_d").build();
    PartitionSpec.builderFor(SCHEMA).day("d").day("another_d").build();
    PartitionSpec.builderFor(SCHEMA).day("d").hour("another_d").build();
    PartitionSpec.builderFor(SCHEMA).hour("d").hour("another_d").build();
  }

  @Test
  public void testMissingSourceColumn() {
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).year("missing").build());
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).month("missing").build());
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).day("missing").build());
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).hour("missing").build());
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).bucket("missing", 4).build());
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).truncate("missing", 5).build());
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).identity("missing").build());
  }
}
