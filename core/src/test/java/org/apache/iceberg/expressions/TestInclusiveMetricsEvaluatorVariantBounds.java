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

import static org.apache.iceberg.expressions.Expressions.extract;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;

class TestInclusiveMetricsEvaluatorVariantBounds {
  private static final Schema SCHEMA = new Schema(required(1, "variant", Types.VariantType.get()));

  private static final int MIN_VALUE = 30;
  private static final int MAX_VALUE = 79;

  @Test
  void boundsInBigEndianBuffers() {
    DataFile file =
        new TestDataFile(
            "file.avro",
            Row.of(),
            50,
            Map.of(1, 50L),
            Map.of(1, 0L),
            null,
            Map.of(1, bigEndianBound(MIN_VALUE)),
            Map.of(1, bigEndianBound(MAX_VALUE)));

    assertThat(shouldRead(greaterThan(extract("variant", "$.event_id", "long"), 50), file))
        .as("Should read: 50 is within bounds")
        .isTrue();
    assertThat(shouldRead(lessThan(extract("variant", "$.event_id", "long"), MIN_VALUE), file))
        .as("Should skip: all values are at least the lower bound")
        .isFalse();
  }

  private static boolean shouldRead(Expression expr, DataFile file) {
    return new InclusiveMetricsEvaluator(SCHEMA, expr).eval(file);
  }

  private static ByteBuffer bigEndianBound(int eventId) {
    ByteBuffer littleEndian =
        VariantTestUtil.variantBuffer(Map.of("$['event_id']", Variants.of(eventId)));
    byte[] bytes = new byte[littleEndian.remaining()];
    littleEndian.duplicate().get(bytes);
    // manifest bounds are deserialized into buffers with the default big-endian order
    return ByteBuffer.wrap(bytes);
  }
}
