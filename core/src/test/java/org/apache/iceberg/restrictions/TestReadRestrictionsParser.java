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
package org.apache.iceberg.restrictions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestReadRestrictionsParser {

  @Test
  public void emptyRestrictionsRoundTrip() {
    String json = ReadRestrictionsParser.toJson(ReadRestrictions.empty());
    ReadRestrictions parsed = ReadRestrictionsParser.fromJson(json);
    assertThat(parsed.isEmpty()).isTrue();
  }

  @Test
  public void emptyObjectParsesAsEmpty() {
    ReadRestrictions parsed = ReadRestrictionsParser.fromJson("{}");
    assertThat(parsed.isEmpty()).isTrue();
  }

  @Test
  public void rowFilterRoundTrip() {
    Expression filter = Expressions.equal("country", "US");
    ReadRestrictions restrictions = ReadRestrictions.of(filter, ImmutableList.of());
    String json = ReadRestrictionsParser.toJson(restrictions);

    assertThat(json).contains("required-row-filter");
    assertThat(json).doesNotContain("required-column-projections");

    ReadRestrictions parsed = ReadRestrictionsParser.fromJson(json);
    assertThat(parsed.rowFilter()).isNotNull();
    assertThat(parsed.columnProjections()).isEmpty();
  }

  @Test
  public void allTenActionTypesRoundTrip() {
    Expression filterExpr = Expressions.alwaysTrue();
    List<Action> actions =
        ImmutableList.of(
            new Action.MaskAlphanum(1),
            new Action.MaskToDefault(2),
            new Action.ReplaceWithNull(3),
            new Action.ShowFirst4(4),
            new Action.ShowLast4(5),
            new Action.TruncateToYear(6),
            new Action.TruncateToMonth(7),
            new Action.Sha256Global(8),
            new Action.Sha256QueryLocal(9),
            new Action.ApplyExpression(10, filterExpr));

    ReadRestrictions restrictions = ReadRestrictions.of(null, actions);
    String json = ReadRestrictionsParser.toJson(restrictions);
    ReadRestrictions parsed = ReadRestrictionsParser.fromJson(json);

    assertThat(parsed.columnProjections()).hasSize(10);
    for (int i = 0; i < actions.size(); i++) {
      assertThat(parsed.columnProjections().get(i).actionType())
          .isEqualTo(actions.get(i).actionType());
      assertThat(parsed.columnProjections().get(i).fieldId()).isEqualTo(actions.get(i).fieldId());
    }
  }

  @Test
  public void unknownActionTypeRejected() {
    String json =
        "{\"required-column-projections\":[{\"action\":\"xxx-not-real\",\"field-id\":1}]}";
    assertThatThrownBy(() -> ReadRestrictionsParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("xxx-not-real");
  }

  @Test
  public void applyExpressionRequiresExpression() {
    String json =
        "{\"required-column-projections\":[{\"action\":\"apply-expression\",\"field-id\":1}]}";
    assertThatThrownBy(() -> ReadRestrictionsParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expression");
  }

  @Test
  public void missingFieldIdRejected() {
    String json = "{\"required-column-projections\":[{\"action\":\"mask-alphanum\"}]}";
    assertThatThrownBy(() -> ReadRestrictionsParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("field-id");
  }

  @Test
  public void rowFilterAndProjectionsRoundTrip() {
    Expression filter =
        Expressions.and(Expressions.equal("country", "US"), Expressions.greaterThan("amount", 100));
    List<Action> actions =
        ImmutableList.of(new Action.MaskAlphanum(2), new Action.ReplaceWithNull(3));
    ReadRestrictions restrictions = ReadRestrictions.of(filter, actions);

    String json = ReadRestrictionsParser.toJson(restrictions);
    ReadRestrictions parsed = ReadRestrictionsParser.fromJson(json);

    assertThat(parsed.rowFilter()).isNotNull();
    assertThat(parsed.columnProjections()).hasSize(2);
    assertThat(parsed.maskedFieldIds()).containsExactlyInAnyOrder(2, 3);
  }
}
