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
package org.apache.iceberg.rest.restrictions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.functions.MaskAlphanum;
import org.apache.iceberg.functions.ShowLast4;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestReadRestrictions {

  private static final Schema SCHEMA_V0 =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "email", Types.StringType.get()),
          optional(3, "ssn", Types.StringType.get()));

  // "ssn" (field 3) has been dropped
  private static final Schema SCHEMA_V1 =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "email", Types.StringType.get()));

  private static final Map<Integer, Schema> SCHEMA_HISTORY =
      ImmutableMap.of(0, SCHEMA_V0, 1, SCHEMA_V1);

  @Test
  public void validateAcceptsFieldIdsInTheCurrentSchema() {
    ReadRestrictions restrictions =
        ReadRestrictions.of(null, ImmutableList.of(new MaskAlphanum(2)));

    assertThatCode(() -> restrictions.validate(SCHEMA_HISTORY)).doesNotThrowAnyException();
  }

  @Test
  public void validateAcceptsFieldIdsFromAnOlderSchema() {
    // a time-travel read may legitimately be restricted on a column that has since been dropped, so
    // membership in any schema is enough
    ReadRestrictions restrictions = ReadRestrictions.of(null, ImmutableList.of(new ShowLast4(3)));

    assertThatCode(() -> restrictions.validate(SCHEMA_HISTORY)).doesNotThrowAnyException();
  }

  @Test
  public void validateRejectsFieldIdInNoSchema() {
    ReadRestrictions restrictions =
        ReadRestrictions.of(null, ImmutableList.of(new MaskAlphanum(999)));

    assertThatThrownBy(() -> restrictions.validate(SCHEMA_HISTORY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unknown field ids")
        .hasMessageContaining("999");
  }

  @Test
  public void validateReportsEveryUnknownFieldId() {
    ReadRestrictions restrictions =
        ReadRestrictions.of(
            null, ImmutableList.of(new MaskAlphanum(2), new MaskAlphanum(998), new ShowLast4(999)));

    assertThatThrownBy(() -> restrictions.validate(SCHEMA_HISTORY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("998")
        .hasMessageContaining("999");
  }

  @Test
  public void validateIgnoresTheRowFilter() {
    // the row filter still serializes column references by name, so it is not field-id validated
    // here; reads fail closed on a filter that cannot bind
    ReadRestrictions restrictions =
        ReadRestrictions.of(Expressions.equal("no-such-column", "x"), ImmutableList.of());

    assertThatCode(() -> restrictions.validate(SCHEMA_HISTORY)).doesNotThrowAnyException();
  }
}
