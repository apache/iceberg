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

package org.apache.iceberg.mr.hive;

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestHiveIcebergPartitionTextParser {

  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.LongType.get()),
      optional(2, "name", Types.StringType.get()),
      optional(3, "employee_info", Types.StructType.of(
          optional(6, "employer", Types.StringType.get()),
          optional(7, "id", Types.LongType.get()),
          optional(8, "address", Types.StringType.get())
      )),
      optional(4, "created", Types.TimestampType.withoutZone()),
      optional(5, "updated", Types.TimestampType.withoutZone())
  );

  @Test
  public void testTextTooLong() {
    AssertHelpers.assertThrows("should fail when input exceeding max allowed length",
        IllegalArgumentException.class,
        "Partition spec text too long: max allowed length 1000, but got 1024",
        () -> HiveIcebergPartitionTextParser.fromText(SCHEMA, new String(new char[1024]).replace("\0", "s"),
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
  }

  @Test
  public void testParsingColumns() {
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("id").identity("name").build();
    Assert.assertEquals(expected, HiveIcebergPartitionTextParser.fromText(SCHEMA, "id|name",
        InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
    Assert.assertEquals("space on the right of pipe should not matter",
        expected, HiveIcebergPartitionTextParser.fromText(SCHEMA, "id| name",
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
    Assert.assertEquals("space on the left of pipe should not matter",
        expected, HiveIcebergPartitionTextParser.fromText(SCHEMA, "id |name",
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
    Assert.assertEquals("space on both sides of pipe should not matter",
        expected, HiveIcebergPartitionTextParser.fromText(SCHEMA, "id | name",
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
  }

  @Test
  public void testParsingColumnsFailure() {
    AssertHelpers.assertThrows("should fail when column does not exist",
        IllegalArgumentException.class,
        "Cannot recognized partition transform or find column",
        () -> HiveIcebergPartitionTextParser.fromText(SCHEMA, "col",
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
  }

  @Test
  public void testParsing2ArgTransforms() {
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .bucket("name", 16).truncate("employee_info.address", 8).build();
    Assert.assertEquals(expected, HiveIcebergPartitionTextParser.fromText(
        SCHEMA, "bucket(16,name)|truncate(employee_info.address,8)",
        InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
    Assert.assertEquals("space in args should not matter",
        expected, HiveIcebergPartitionTextParser.fromText(
            SCHEMA, "bucket( 16, name)| truncate(employee_info.address , 8 )",
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
  }

  @Test
  public void testParsing2ArgTransformsFailure() {
    AssertHelpers.assertThrows("should fail when there is only 1 arg",
        IllegalArgumentException.class,
        "Cannot recognized partition transform",
        () -> HiveIcebergPartitionTextParser.fromText(SCHEMA, "bucket(a)",
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));

    AssertHelpers.assertThrows("should fail when column does not exist",
        IllegalArgumentException.class,
        "Cannot find source column",
        () -> HiveIcebergPartitionTextParser.fromText(SCHEMA, "bucket(8, col)",
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));

    AssertHelpers.assertThrows("should fail when input to transform is wrong",
        IllegalArgumentException.class,
        "Cannot find integer as argument 1 in 2-arg partition transform",
        () -> HiveIcebergPartitionTextParser.fromText(SCHEMA, "bucket(name, 8)",
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
  }

  @Test
  public void testParsing1ArgTransforms() {
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .day("created").hour("updated").alwaysNull("id").build();
    Assert.assertEquals(expected, HiveIcebergPartitionTextParser.fromText(
        SCHEMA, "day(created)|hour(updated)|alwaysNull(id)",
        InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
    Assert.assertEquals("space in args should not matter",
        expected, HiveIcebergPartitionTextParser.fromText(
            SCHEMA, "day(created )| hour( updated)| alwaysNull(id)",
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
    Assert.assertEquals("case sensitivity should not matter",
        expected, HiveIcebergPartitionTextParser.fromText(
            SCHEMA, "day(created)|hour(updated)|alwaysnull(id)",
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
  }

  @Test
  public void testParsing1ArgTransformsFailure() {
    AssertHelpers.assertThrows("should fail when column does not exist",
        IllegalArgumentException.class,
        "Cannot find source column",
        () -> HiveIcebergPartitionTextParser.fromText(SCHEMA, "year(col)",
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
    AssertHelpers.assertThrows("should treat additional arguments as plain text column and fail",
        IllegalArgumentException.class,
        "Cannot find source column",
        () -> HiveIcebergPartitionTextParser.fromText(SCHEMA, "year(8,col)",
            InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
  }

  @Test
  public void textMixedTransforms() {
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .day("created")
        .bucket("id", 16)
        .identity("employee_info.employer")
        .build();
    Assert.assertEquals(expected, HiveIcebergPartitionTextParser.fromText(
        SCHEMA, "day(created)|bucket(16,id)|employee_info.employer",
        InputFormatConfig.PARTITIONING_DELIMITER_DEFAULT));
  }

  @Test
  public void testNonDefaultDelimiter() {
    Schema schema = new Schema(
        optional(1, "i|d", Types.LongType.get()),
        optional(2, "na,me", Types.StringType.get()));
    PartitionSpec expected = PartitionSpec.builderFor(schema)
        .bucket("i|d", 16).alwaysNull("na,me").build();
    Assert.assertEquals(expected, HiveIcebergPartitionTextParser.fromText(
        schema, "bucket(16,i|d);alwaysNull(na,me)", ";"));
  }
}
