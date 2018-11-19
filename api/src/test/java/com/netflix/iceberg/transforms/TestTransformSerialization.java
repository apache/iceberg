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

package com.netflix.iceberg.transforms;

import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.TestHelpers;
import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestTransformSerialization {
  @Test
  public void testTransforms() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(1, "i", Types.IntegerType.get()),
        Types.NestedField.required(2, "l", Types.LongType.get()),
        Types.NestedField.required(3, "d", Types.DateType.get()),
        Types.NestedField.required(4, "t", Types.TimeType.get()),
        Types.NestedField.required(5, "ts", Types.TimestampType.withoutZone()),
        Types.NestedField.required(6, "dec", Types.DecimalType.of(9, 2)),
        Types.NestedField.required(7, "s", Types.StringType.get()),
        Types.NestedField.required(8, "u", Types.UUIDType.get()),
        Types.NestedField.required(9, "f", Types.FixedType.ofLength(3)),
        Types.NestedField.required(10, "b", Types.BinaryType.get())
    );

    // a spec with all of the allowed transform/type pairs
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("i")
        .identity("l")
        .identity("d")
        .identity("t")
        .identity("ts")
        .identity("dec")
        .identity("s")
        .identity("u")
        .identity("f")
        .identity("b")
        .bucket("i", 128)
        .bucket("l", 128)
        .bucket("d", 128)
        .bucket("t", 128)
        .bucket("ts", 128)
        .bucket("dec", 128)
        .bucket("s", 128)
        .bucket("u", 128)
        .bucket("f", 128)
        .bucket("b", 128)
        .year("d")
        .month("d")
        .day("d")
        .year("ts")
        .month("ts")
        .day("ts")
        .hour("ts")
        .truncate("i", 10)
        .truncate("l", 10)
        .truncate("dec", 10)
        .truncate("s", 10)
        .build();

    Assert.assertEquals("Deserialization should produce equal partition spec",
        spec, TestHelpers.roundTripSerialize(spec));
  }
}
