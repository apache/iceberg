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

package org.apache.iceberg.types;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

public class TestReassignIds {
  @Test
  public void testReassignIds() {
    Schema schema = TypeUtil.reassignIds(
        new Schema(
            Types.NestedField.required(10, "a", Types.LongType.get()),
            Types.NestedField.required(11, "b", Types.StringType.get())
        ),
        new Schema(
            Types.NestedField.required(0, "a", Types.LongType.get())));

    check(ImmutableMap.of("b", 1, "a", 0), schema);

    schema = TypeUtil.reassignIds(
        new Schema(
            Types.NestedField.required(20, "a", Types.MapType.ofRequired(21, 22,
                Types.StringType.get(),
                Types.StructType.of(
                    Types.NestedField.required(25, "b", Types.StringType.get()),
                    Types.NestedField.required(27, "c", Types.StructType.of(
                        Types.NestedField.required(28, "d", Types.FloatType.get()),
                        Types.NestedField.required(29, "e", Types.FloatType.get())
                    )))))),
        new Schema(
            Types.NestedField.required(1, "a", Types.MapType.ofRequired(2, 3,
                Types.StringType.get(),
                Types.StructType.of(
                    Types.NestedField.required(4, "b", Types.StringType.get()))))));

    check(ImmutableMap.of("a.c", 7, "a.c.d", 5, "a.c.e", 6), schema);

    schema = TypeUtil.reassignIds(
        new Schema(
            Types.NestedField.required(9, "e", Types.IntegerType.get()),
            Types.NestedField.required(10, "a",
                Types.StructType.of(
                    Types.NestedField.required(11, "b",
                        Types.StructType.of(
                            Types.NestedField.required(12, "c",
                                Types.StructType.of(
                                    Types.NestedField.required(13, "d", Types.IntegerType.get())))))))),

        new Schema(
            Types.NestedField.required(0, "e", Types.IntegerType.get())));

    check(ImmutableMap.of("a.b.c.d", 1, "a.b.c", 2, "a.b", 3, "a", 4, "e", 0), schema);
  }

  private void check(Map<String, Integer> expectedNameToId, Schema schema) {
    for (Map.Entry<String, Integer> e : expectedNameToId.entrySet()) {
      Assert.assertEquals(e.getValue().intValue(), schema.findField(e.getKey()).fieldId());
    }
  }
}
