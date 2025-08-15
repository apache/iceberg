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
package org.apache.iceberg.expressions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestBoundReference {
  // Build a schema with a single nested struct with optionalList.size() levels with the following
  // structure:
  // s1: struct(s2: struct(s3: struct(..., sn: struct(leaf: int))))
  // where each s{i} is an optional struct if optionalList.get(i) is true and a required struct if
  // false
  private static Schema buildSchemaFromOptionalList(List<Boolean> optionalList, String leafName) {
    if (optionalList == null || optionalList.isEmpty()) {
      throw new IllegalArgumentException("optionalList must not be empty");
    }

    Types.NestedField leaf =
        optionalList.get(optionalList.size() - 1)
            ? optional(optionalList.size(), leafName, Types.IntegerType.get())
            : required(optionalList.size(), leafName, Types.IntegerType.get());

    Types.StructType current = Types.StructType.of(leaf);

    for (int i = optionalList.size() - 2; i >= 0; i--) {
      int id = i + 1;
      String name = "s" + (i + 1);
      current =
          Types.StructType.of(
              optionalList.get(i) ? optional(id, name, current) : required(id, name, current));
    }

    return new Schema(current.fields());
  }

  private static Stream<Arguments> producesNullCases() {
    return Stream.of(
        // basic fields, no struct levels
        Arguments.of(Arrays.asList(false), false),
        Arguments.of(Arrays.asList(true), true),
        // one level
        Arguments.of(Arrays.asList(false, false), false),
        Arguments.of(Arrays.asList(false, true), true),
        Arguments.of(Arrays.asList(true, false), true),
        // two levels
        Arguments.of(Arrays.asList(false, false, false), false),
        Arguments.of(Arrays.asList(false, false, true), true),
        Arguments.of(Arrays.asList(true, false, false), true),
        Arguments.of(Arrays.asList(false, true, false), true),
        // three levels
        Arguments.of(Arrays.asList(false, false, false, false), false),
        Arguments.of(Arrays.asList(false, false, false, true), true),
        Arguments.of(Arrays.asList(true, false, false, false), true),
        Arguments.of(Arrays.asList(false, true, false, false), true),
        // four levels
        Arguments.of(Arrays.asList(false, false, false, false, false), false),
        Arguments.of(Arrays.asList(false, false, false, false, true), true),
        Arguments.of(Arrays.asList(true, false, false, false, false), true),
        Arguments.of(Arrays.asList(false, true, true, true, false), true));
  }

  @ParameterizedTest
  @MethodSource("producesNullCases")
  public void testProducesNull(List<Boolean> optionalList, boolean expectedProducesNull) {
    String leafName = "leaf";
    Schema schema = buildSchemaFromOptionalList(optionalList, leafName);
    int leafId = optionalList.size();
    Types.NestedField leafField = schema.findField(leafId);
    Accessor<StructLike> accessor = schema.accessorForField(leafId);

    BoundReference<Integer> ref = new BoundReference<>(leafField, accessor, leafName);
    assertThat(ref.producesNull()).isEqualTo(expectedProducesNull);
  }
}
