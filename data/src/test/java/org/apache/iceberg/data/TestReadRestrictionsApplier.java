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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.restrictions.Action;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestReadRestrictionsApplier {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "email", Types.StringType.get()),
          optional(3, "ssn", Types.StringType.get()));

  private static final Record TEMPLATE = GenericRecord.create(SCHEMA);

  @Test
  public void testNoActionsReturnsInputUnchanged() {
    CloseableIterable<Record> input =
        CloseableIterable.withNoopClose(
            ImmutableList.of(TEMPLATE.copy(ImmutableMap.of("id", 1L, "email", "a@b.com"))));

    CloseableIterable<Record> out =
        ReadRestrictionsApplier.apply(input, ReadRestrictions.empty(), SCHEMA);

    assertThat(out).isSameAs(input);
  }

  @Test
  public void testMaskAlphanumRewritesEmailField() {
    CloseableIterable<Record> input =
        CloseableIterable.withNoopClose(
            ImmutableList.of(
                TEMPLATE.copy(ImmutableMap.of("id", 1L, "email", "alice@example.com")),
                TEMPLATE.copy(ImmutableMap.of("id", 2L, "email", "bob123@example.com"))));

    ReadRestrictions restrictions =
        ReadRestrictions.of(null, ImmutableList.of(new Action.MaskAlphanum(2)));

    List<Record> result =
        Lists.newArrayList(ReadRestrictionsApplier.apply(input, restrictions, SCHEMA));

    assertThat(result).hasSize(2);
    assertThat(result.get(0).getField("id")).isEqualTo(1L);
    assertThat(result.get(0).getField("email")).isEqualTo("xxxxx@xxxxxxx.xxx");
    assertThat(result.get(1).getField("email")).isEqualTo("xxxnnn@xxxxxxx.xxx");
  }

  @Test
  public void testMultipleMasksApplyIndependently() {
    CloseableIterable<Record> input =
        CloseableIterable.withNoopClose(
            ImmutableList.of(
                TEMPLATE.copy(
                    ImmutableMap.of(
                        "id", 42L, "email", "alice@example.com", "ssn", "123-45-6789"))));

    ReadRestrictions restrictions =
        ReadRestrictions.of(
            null, ImmutableList.of(new Action.MaskAlphanum(2), new Action.ShowLast4(3)));

    List<Record> result =
        Lists.newArrayList(ReadRestrictionsApplier.apply(input, restrictions, SCHEMA));

    assertThat(result.get(0).getField("id")).isEqualTo(42L);
    assertThat(result.get(0).getField("email")).isEqualTo("xxxxx@xxxxxxx.xxx");
    assertThat(result.get(0).getField("ssn")).isEqualTo("nnn-nn-6789");
  }

  @Test
  public void testNullValuesPassThroughAsNull() {
    CloseableIterable<Record> input =
        CloseableIterable.withNoopClose(ImmutableList.of(TEMPLATE.copy(ImmutableMap.of("id", 1L))));

    ReadRestrictions restrictions =
        ReadRestrictions.of(null, ImmutableList.of(new Action.MaskAlphanum(2)));

    List<Record> result =
        Lists.newArrayList(ReadRestrictionsApplier.apply(input, restrictions, SCHEMA));

    assertThat(result.get(0).getField("email")).isNull();
  }

  @Test
  public void testUnknownFieldIdFailsClosed() {
    CloseableIterable<Record> input =
        CloseableIterable.withNoopClose(ImmutableList.of(TEMPLATE.copy()));

    ReadRestrictions restrictions =
        ReadRestrictions.of(null, ImmutableList.of(new Action.MaskAlphanum(999)));

    assertThatThrownBy(() -> ReadRestrictionsApplier.apply(input, restrictions, SCHEMA))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("unknown field id: 999");
  }

  @Test
  public void testNestedFieldIdFailsClosed() {
    Schema nested =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2, "contact", Types.StructType.of(optional(3, "email", Types.StringType.get()))));

    CloseableIterable<Record> input =
        CloseableIterable.withNoopClose(ImmutableList.of(GenericRecord.create(nested)));

    ReadRestrictions restrictions =
        ReadRestrictions.of(null, ImmutableList.of(new Action.MaskAlphanum(3)));

    assertThatThrownBy(() -> ReadRestrictionsApplier.apply(input, restrictions, nested))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("nested fields are not yet supported")
        .hasMessageContaining("fieldId=3");
  }

  @Test
  public void testRowFilterDropsNonMatchingRows() {
    CloseableIterable<Record> input =
        CloseableIterable.withNoopClose(
            ImmutableList.of(
                TEMPLATE.copy(ImmutableMap.of("id", 1L, "email", "a@b.com")),
                TEMPLATE.copy(ImmutableMap.of("id", 2L, "email", "c@d.com")),
                TEMPLATE.copy(ImmutableMap.of("id", 3L, "email", "e@f.com"))));

    ReadRestrictions restrictions =
        ReadRestrictions.of(Expressions.greaterThan("id", 1L), ImmutableList.of());

    List<Record> result =
        Lists.newArrayList(ReadRestrictionsApplier.apply(input, restrictions, SCHEMA));

    assertThat(result).hasSize(2);
    assertThat(result.get(0).getField("id")).isEqualTo(2L);
    assertThat(result.get(1).getField("id")).isEqualTo(3L);
  }

  @Test
  public void testRowFilterSeesOriginalValuesBeforeMasks() {
    CloseableIterable<Record> input =
        CloseableIterable.withNoopClose(
            ImmutableList.of(
                TEMPLATE.copy(ImmutableMap.of("id", 1L, "email", "keep@example.com")),
                TEMPLATE.copy(ImmutableMap.of("id", 2L, "email", "drop@example.com"))));

    ReadRestrictions restrictions =
        ReadRestrictions.of(
            Expressions.equal("email", "keep@example.com"),
            ImmutableList.of(new Action.MaskAlphanum(2)));

    List<Record> result =
        Lists.newArrayList(ReadRestrictionsApplier.apply(input, restrictions, SCHEMA));

    assertThat(result).hasSize(1);
    assertThat(result.get(0).getField("id")).isEqualTo(1L);
    assertThat(result.get(0).getField("email")).isEqualTo("xxxx@xxxxxxx.xxx");
  }

  @Test
  public void testNullRowFilterSkipsFilterStep() {
    CloseableIterable<Record> input =
        CloseableIterable.withNoopClose(
            ImmutableList.of(
                TEMPLATE.copy(ImmutableMap.of("id", 1L)),
                TEMPLATE.copy(ImmutableMap.of("id", 2L))));

    ReadRestrictions restrictions = ReadRestrictions.of(null, ImmutableList.of());

    List<Record> result =
        Lists.newArrayList(ReadRestrictionsApplier.apply(input, restrictions, SCHEMA));

    assertThat(result).hasSize(2);
  }
}
