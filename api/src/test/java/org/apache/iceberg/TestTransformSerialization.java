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
    PartitionSpec[] specs = new PartitionSpec[] {
        PartitionSpec.builderFor(schema).identity("i").build(),
        PartitionSpec.builderFor(schema).identity("l").build(),
        PartitionSpec.builderFor(schema).identity("d").build(),
        PartitionSpec.builderFor(schema).identity("t").build(),
        PartitionSpec.builderFor(schema).identity("ts").build(),
        PartitionSpec.builderFor(schema).identity("dec").build(),
        PartitionSpec.builderFor(schema).identity("s").build(),
        PartitionSpec.builderFor(schema).identity("u").build(),
        PartitionSpec.builderFor(schema).identity("f").build(),
        PartitionSpec.builderFor(schema).identity("b").build(),
        PartitionSpec.builderFor(schema).bucket("i", 128).build(),
        PartitionSpec.builderFor(schema).bucket("l", 128).build(),
        PartitionSpec.builderFor(schema).bucket("d", 128).build(),
        PartitionSpec.builderFor(schema).bucket("t", 128).build(),
        PartitionSpec.builderFor(schema).bucket("ts", 128).build(),
        PartitionSpec.builderFor(schema).bucket("dec", 128).build(),
        PartitionSpec.builderFor(schema).bucket("s", 128).build(),
        PartitionSpec.builderFor(schema).bucket("u", 128).build(),
        PartitionSpec.builderFor(schema).bucket("f", 128).build(),
        PartitionSpec.builderFor(schema).bucket("b", 128).build(),
        PartitionSpec.builderFor(schema).year("d").build(),
        PartitionSpec.builderFor(schema).month("d").build(),
        PartitionSpec.builderFor(schema).day("d").build(),
        PartitionSpec.builderFor(schema).year("ts").build(),
        PartitionSpec.builderFor(schema).month("ts").build(),
        PartitionSpec.builderFor(schema).day("ts").build(),
        PartitionSpec.builderFor(schema).hour("ts").build(),
        PartitionSpec.builderFor(schema).truncate("i", 10).build(),
        PartitionSpec.builderFor(schema).truncate("l", 10).build(),
        PartitionSpec.builderFor(schema).truncate("dec", 10).build(),
        PartitionSpec.builderFor(schema).truncate("s", 10).build(),
        PartitionSpec.builderFor(schema).add(6, "dec_unsupported", "unsupported").build(),
    };

    for (PartitionSpec spec : specs) {
      Assert.assertEquals("Deserialization should produce equal partition spec",
          spec, TestHelpers.roundTripSerialize(spec));
    }
  }
}
