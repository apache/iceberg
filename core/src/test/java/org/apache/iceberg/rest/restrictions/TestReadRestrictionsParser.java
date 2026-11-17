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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.functions.IcebergFunctions;
import org.apache.iceberg.functions.UnknownFunction;
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
    assertThat(ExpressionParser.toJson(parsed.rowFilter()))
        .isEqualTo(ExpressionParser.toJson(filter));
    assertThat(parsed.columnProjections()).isEmpty();
  }

  @Test
  public void allActionTypesRoundTrip() {
    List<Action> actions =
        ImmutableList.of(
            new Action(1, IcebergFunctions.maskAlphanum()),
            new Action(2, IcebergFunctions.maskToFixedValue()),
            new Action(3, IcebergFunctions.replaceWithNull()),
            new Action(4, IcebergFunctions.showFirst4()),
            new Action(5, IcebergFunctions.showLast4()),
            new Action(6, IcebergFunctions.truncateToYear()),
            new Action(7, IcebergFunctions.truncateToMonth()),
            new Action(8, IcebergFunctions.sha256Global()),
            new Action(9, IcebergFunctions.sha256QueryLocal()));

    ReadRestrictions restrictions = ReadRestrictions.of(null, actions);
    String json = ReadRestrictionsParser.toJson(restrictions);
    ReadRestrictions parsed = ReadRestrictionsParser.fromJson(json);

    assertThat(parsed.columnProjections()).hasSize(actions.size());
    for (int i = 0; i < actions.size(); i++) {
      assertThat(parsed.columnProjections().get(i).function().toString())
          .isEqualTo(actions.get(i).function().toString());
      assertThat(parsed.columnProjections().get(i).fieldId()).isEqualTo(actions.get(i).fieldId());
    }
  }

  @Test
  public void unknownActionTypePreserved() {
    // Forward-compat: parsing an unrecognized discriminator must not throw, so older clients
    // keep interoperating with newer servers. Enforcement code is responsible for failing
    // closed when it encounters UnknownFunction; silently skipping would leak unmasked data.
    String json =
        "{\"required-column-projections\":[{\"action\":\"xxx-not-real\",\"field-id\":1}]}";
    ReadRestrictions restrictions = ReadRestrictionsParser.fromJson(json);
    assertThat(restrictions.columnProjections()).hasSize(1);
    Action action = restrictions.columnProjections().get(0);
    assertThat(action.function()).isInstanceOf(UnknownFunction.class);
    assertThat(ActionParser.toJson(action))
        .isEqualTo("{\"action\":\"xxx-not-real\",\"field-id\":1}");
  }

  @Test
  public void duplicateFieldIdRejected() {
    // The spec requires the reader to fail when a field id carries more than one projection.
    String json =
        "{\"required-column-projections\":["
            + "{\"action\":\"mask-alphanum\",\"field-id\":2},"
            + "{\"action\":\"show-last-4\",\"field-id\":2}]}";
    assertThatThrownBy(() -> ReadRestrictionsParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("duplicate column projections")
        .hasMessageContaining("2");
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
        ImmutableList.of(
            new Action(2, IcebergFunctions.maskAlphanum()),
            new Action(3, IcebergFunctions.replaceWithNull()));
    ReadRestrictions restrictions = ReadRestrictions.of(filter, actions);

    String json = ReadRestrictionsParser.toJson(restrictions);
    ReadRestrictions parsed = ReadRestrictionsParser.fromJson(json);

    assertThat(parsed.rowFilter()).isNotNull();
    assertThat(parsed.columnProjections()).hasSize(2);
    assertThat(parsed.maskedFieldIds()).containsExactlyInAnyOrder(2, 3);
  }
}
