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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.functions.IcebergFunction;
import org.apache.iceberg.functions.MaskAlphanum;
import org.apache.iceberg.functions.MaskToFixedValue;
import org.apache.iceberg.functions.ReplaceWithNull;
import org.apache.iceberg.functions.Sha256Global;
import org.apache.iceberg.functions.Sha256QueryLocal;
import org.apache.iceberg.functions.ShowFirst4;
import org.apache.iceberg.functions.ShowLast4;
import org.apache.iceberg.functions.TruncateToMonth;
import org.apache.iceberg.functions.TruncateToYear;
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
    assertThat(parsed.rowFilter()).isNotNull();
    assertThat(parsed.columnProjections()).isEmpty();
  }

  @Test
  public void allActionTypesRoundTrip() {
    List<IcebergFunction<?, ?>> actions =
        ImmutableList.of(
            new MaskAlphanum(1),
            new MaskToFixedValue(2),
            new ReplaceWithNull(3),
            new ShowFirst4(4),
            new ShowLast4(5),
            new TruncateToYear(6),
            new TruncateToMonth(7),
            new Sha256Global(8),
            new Sha256QueryLocal(9));

    ReadRestrictions restrictions = ReadRestrictions.of(null, actions);
    String json = ReadRestrictionsParser.toJson(restrictions);
    ReadRestrictions parsed = ReadRestrictionsParser.fromJson(json);

    assertThat(parsed.columnProjections()).hasSize(actions.size());
    for (int i = 0; i < actions.size(); i++) {
      assertThat(parsed.columnProjections().get(i).name()).isEqualTo(actions.get(i).name());
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
    IcebergFunction<?, ?> action = restrictions.columnProjections().get(0);
    assertThat(action).isInstanceOf(UnknownFunction.class);
    assertThat(action.name()).isEqualTo("xxx-not-real");
    assertThat(action.fieldId()).isEqualTo(1);
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
    List<IcebergFunction<?, ?>> actions =
        ImmutableList.of(new MaskAlphanum(2), new ReplaceWithNull(3));
    ReadRestrictions restrictions = ReadRestrictions.of(filter, actions);

    String json = ReadRestrictionsParser.toJson(restrictions);
    ReadRestrictions parsed = ReadRestrictionsParser.fromJson(json);

    assertThat(parsed.rowFilter()).isNotNull();
    assertThat(parsed.columnProjections()).hasSize(2);
    assertThat(parsed.maskedFieldIds()).containsExactlyInAnyOrder(2, 3);
  }
}
