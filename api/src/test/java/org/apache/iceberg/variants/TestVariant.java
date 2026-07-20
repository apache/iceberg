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
package org.apache.iceberg.variants;

import java.util.Map;
import org.apache.iceberg.TestHelpers.RoundTripSerializer;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestVariant {
  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void testSerialization(RoundTripSerializer<Variant> serializer) throws Exception {
    Map<String, VariantValue> data =
        ImmutableMap.of(
            "a",
            SerializedPrimitive.from(new byte[] {0b1100, 1}), // int8 = 1
            "b",
            VariantTestUtil.createShortString("iceberg"));
    Variant variant = VariantTestUtil.variant(data);

    Variant result = serializer.apply(variant);

    VariantTestUtil.assertEqual(variant.metadata(), result.metadata());
    VariantTestUtil.assertEqual(variant.value(), result.value());
  }
}
