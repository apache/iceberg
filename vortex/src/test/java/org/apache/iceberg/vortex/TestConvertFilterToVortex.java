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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import dev.vortex.api.Expression;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests for filter conversion. The Vortex {@link Expression} API in 0.71+ wraps an opaque
 * native handle so we cannot compare expressions structurally; tests instead check that supported
 * shapes return a non-sentinel expression and unsupported shapes fall back to ALWAYS_TRUE.
 */
public class TestConvertFilterToVortex {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "salary", Types.LongType.get()));

  @Test
  public void testIn() {
    Expression result = ConvertFilterToVortex.convert(SCHEMA, Expressions.in("id", 1L, 2L, 3L));
    assertThat(result).isNotNull();
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_TRUE);
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_FALSE);
  }

  @Test
  public void testNotIn() {
    Expression result = ConvertFilterToVortex.convert(SCHEMA, Expressions.notIn("id", 1L, 2L, 3L));
    assertThat(result).isNotNull();
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_TRUE);
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_FALSE);
  }

  @Test
  public void testInSingleValue() {
    // A single-value IN is optimized by Iceberg to EQ during binding
    Expression result = ConvertFilterToVortex.convert(SCHEMA, Expressions.in("id", 42L));
    assertThat(result).isNotNull();
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_TRUE);
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_FALSE);
  }

  @Test
  public void testInStrings() {
    Expression result =
        ConvertFilterToVortex.convert(SCHEMA, Expressions.in("name", "Alice", "Bob"));
    assertThat(result).isNotNull();
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_TRUE);
    assertThat(result).isNotSameAs(ConvertFilterToVortex.ALWAYS_FALSE);
  }
}
