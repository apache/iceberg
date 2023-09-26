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

import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class PartitionSpecTestBase {
  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "i", Types.IntegerType.get()),
          Types.NestedField.required(2, "l", Types.LongType.get()),
          Types.NestedField.required(3, "d", Types.DateType.get()),
          Types.NestedField.required(4, "t", Types.TimeType.get()),
          Types.NestedField.required(5, "ts", Types.TimestampType.microsWithoutZone()),
          Types.NestedField.required(6, "dec", Types.DecimalType.of(9, 2)),
          Types.NestedField.required(7, "s", Types.StringType.get()),
          Types.NestedField.required(8, "u", Types.UUIDType.get()),
          Types.NestedField.required(9, "f", Types.FixedType.ofLength(3)),
          Types.NestedField.required(10, "b", Types.BinaryType.get()),
          Types.NestedField.required(11, "tsn", Types.TimestampType.nanosWithoutZone()));

  // a spec with all of the allowed transform/type pairs
  public static final PartitionSpec[] SPECS =
      new PartitionSpec[] {
        PartitionSpec.builderFor(SCHEMA).identity("i").build(),
        PartitionSpec.builderFor(SCHEMA).identity("l").build(),
        PartitionSpec.builderFor(SCHEMA).identity("d").build(),
        PartitionSpec.builderFor(SCHEMA).identity("t").build(),
        PartitionSpec.builderFor(SCHEMA).identity("ts").build(),
        PartitionSpec.builderFor(SCHEMA).identity("dec").build(),
        PartitionSpec.builderFor(SCHEMA).identity("s").build(),
        PartitionSpec.builderFor(SCHEMA).identity("u").build(),
        PartitionSpec.builderFor(SCHEMA).identity("f").build(),
        PartitionSpec.builderFor(SCHEMA).identity("b").build(),
        PartitionSpec.builderFor(SCHEMA).identity("tsn").build(),
        PartitionSpec.builderFor(SCHEMA).bucket("i", 128).build(),
        PartitionSpec.builderFor(SCHEMA).bucket("l", 128).build(),
        PartitionSpec.builderFor(SCHEMA).bucket("d", 128).build(),
        PartitionSpec.builderFor(SCHEMA).bucket("t", 128).build(),
        PartitionSpec.builderFor(SCHEMA).bucket("ts", 128).build(),
        PartitionSpec.builderFor(SCHEMA).bucket("dec", 128).build(),
        PartitionSpec.builderFor(SCHEMA).bucket("s", 128).build(),
        PartitionSpec.builderFor(SCHEMA).bucket("u", 128).build(),
        PartitionSpec.builderFor(SCHEMA).bucket("f", 128).build(),
        PartitionSpec.builderFor(SCHEMA).bucket("b", 128).build(),
        PartitionSpec.builderFor(SCHEMA).bucket("tsn", 128).build(),
        PartitionSpec.builderFor(SCHEMA).year("d").build(),
        PartitionSpec.builderFor(SCHEMA).month("d").build(),
        PartitionSpec.builderFor(SCHEMA).day("d").build(),
        PartitionSpec.builderFor(SCHEMA).year("ts").build(),
        PartitionSpec.builderFor(SCHEMA).month("ts").build(),
        PartitionSpec.builderFor(SCHEMA).day("ts").build(),
        PartitionSpec.builderFor(SCHEMA).hour("ts").build(),
        PartitionSpec.builderFor(SCHEMA).year("tsn").build(),
        PartitionSpec.builderFor(SCHEMA).month("tsn").build(),
        PartitionSpec.builderFor(SCHEMA).day("tsn").build(),
        PartitionSpec.builderFor(SCHEMA).hour("tsn").build(),
        PartitionSpec.builderFor(SCHEMA).truncate("i", 10).build(),
        PartitionSpec.builderFor(SCHEMA).truncate("l", 10).build(),
        PartitionSpec.builderFor(SCHEMA).truncate("dec", 10).build(),
        PartitionSpec.builderFor(SCHEMA).truncate("s", 10).build(),
        PartitionSpec.builderFor(SCHEMA)
            .add(6, "dec_unsupported", Transforms.fromString("unsupported"))
            .build(),
        PartitionSpec.builderFor(SCHEMA)
            .add(6, 1111, "dec_unsupported", Transforms.fromString("unsupported"))
            .build(),
        PartitionSpec.builderFor(SCHEMA).alwaysNull("ts").build(),
      };
}
