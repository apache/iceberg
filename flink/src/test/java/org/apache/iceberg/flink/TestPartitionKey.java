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

package org.apache.iceberg.flink;

import org.apache.flink.types.Row;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestPartitionKey {

  @Test
  public void testSimplePartition() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "address", Types.StringType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("address")
        .build();
    RowWrapper rowWrapper = new RowWrapper(schema.asStruct());

    Row row1 = Row.of(101, "hello", "addr-1");
    PartitionKey partitionKey = new PartitionKey(spec, schema);
    partitionKey.partition(rowWrapper.wrap(row1));
    Assert.assertEquals(partitionKey.size(), 1);
    Assert.assertEquals(partitionKey.get(0, String.class), "addr-1");

    Row row2 = Row.of(102, "world", "addr-2");
    partitionKey.partition(rowWrapper.wrap(row2));
    Assert.assertEquals(partitionKey.size(), 1);
    Assert.assertEquals(partitionKey.get(0, String.class), "addr-2");
  }

  @Test
  public void testPartitionWithNestedType() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "structType", Types.StructType.of(
            Types.NestedField.optional(3, "innerStringType", Types.StringType.get()),
            Types.NestedField.optional(4, "innerIntegerType", Types.IntegerType.get())
        )),
        Types.NestedField.optional(5, "listType", Types.ListType.ofOptional(6, Types.LongType.get())),
        Types.NestedField.optional(7, "mapType",
            Types.MapType.ofRequired(8, 9, Types.IntegerType.get(), Types.StringType.get())),
        Types.NestedField.required(10, "ts", Types.TimestampType.withZone())
    );
    RowWrapper rowWrapper = new RowWrapper(schema.asStruct());

    Row row = Row.of(
        1001,
        Row.of("addr-1", 200),
        new Long[] {101L, 102L},
        ImmutableMap.of(1001, "1001-value"),
        DateTimeUtil.microsFromTimestamp(DateTimeUtil.timestampFromMicros(0L))
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("structType.innerStringType")
        .build();
    PartitionKey partitionKey = new PartitionKey(spec, schema);
    partitionKey.partition(rowWrapper.wrap(row));
    Assert.assertEquals(partitionKey.size(), 1);
    Assert.assertEquals(partitionKey.get(0, String.class), "addr-1");
    Assert.assertEquals(partitionKey.toPath(), "structType.innerStringType=addr-1");

    PartitionSpec spec2 = PartitionSpec.builderFor(schema)
        .identity("structType.innerIntegerType")
        .build();
    PartitionKey partitionKey2 = new PartitionKey(spec2, schema);
    partitionKey2.partition(rowWrapper.wrap(row));
    Assert.assertEquals(1, partitionKey2.size());
    Assert.assertEquals(200, (int) partitionKey2.get(0, Integer.class));
    Assert.assertEquals(partitionKey2.toPath(), "structType.innerIntegerType=200");

    PartitionSpec spec3 = PartitionSpec.builderFor(schema)
        .identity("structType.innerStringType")
        .identity("structType.innerIntegerType")
        .build();
    PartitionKey partitionKey3 = new PartitionKey(spec3, schema);
    partitionKey3.partition(rowWrapper.wrap(row));
    Assert.assertEquals(2, partitionKey3.size());
    Assert.assertEquals("addr-1", partitionKey3.get(0, String.class));
    Assert.assertEquals(200, (int) partitionKey3.get(1, Integer.class));
    Assert.assertEquals(partitionKey3.toPath(), "structType.innerStringType=addr-1/structType.innerIntegerType=200");

    PartitionSpec spec4 = PartitionSpec.builderFor(schema)
        .identity("structType.innerIntegerType")
        .identity("structType.innerStringType")
        .hour("ts")
        .build();
    PartitionKey partitionKey4 = new PartitionKey(spec4, schema);
    partitionKey4.partition(rowWrapper.wrap(row));
    Assert.assertEquals(3, partitionKey4.size());
    Assert.assertEquals(200, (int) partitionKey4.get(0, Integer.class));
    Assert.assertEquals("addr-1", partitionKey4.get(1, String.class));
    Assert.assertEquals(0, (int) partitionKey4.get(2, Integer.class));
    Assert.assertEquals(partitionKey4.toPath(),
        "structType.innerIntegerType=200/structType.innerStringType=addr-1/ts_hour=1970-01-01-00");

    PartitionSpec.Builder spec5Builder = PartitionSpec.builderFor(schema)
        .identity("id")
        .identity("structType.innerStringType")
        .identity("structType.innerIntegerType")
        .identity("listType");
    AssertHelpers.assertThrows("Should not be able to partition non-primitive field.",
        ValidationException.class,
        "Cannot partition by non-primitive source field: list<long>",
        spec5Builder::build);

    PartitionSpec.Builder spec6Builder = PartitionSpec.builderFor(schema)
        .identity("mapType");
    AssertHelpers.assertThrows("Should not be able to partition non-primitive field.",
        ValidationException.class, "Cannot partition by non-primitive source field: map<int, string>",
        spec6Builder::build);
  }
}
