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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Comparator;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** PROTOTYPE: collation support — field annotation, schema round-trip, and comparison. */
public class TestCollationPrototype {

  private static Types.NestedField collatedField(int id, String name, String collation) {
    return Types.NestedField.optional(name)
        .withId(id)
        .ofType(Types.StringType.get())
        .withCollation(collation)
        .build();
  }

  @Test
  public void collationRoundTripsThroughSchemaJson() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            collatedField(2, "name", "icu.en_US-ci"));

    String json = SchemaParser.toJson(schema);
    assertThat(json).contains("\"collation\":\"icu.en_US-ci\"");

    Schema roundTripped = SchemaParser.fromJson(json);
    assertThat(roundTripped.findField("name").collation()).isEqualTo("icu.en_US-ci");
  }

  @Test
  public void plainStringHasNoCollation() {
    Schema schema = new Schema(Types.NestedField.optional(1, "name", Types.StringType.get()));

    assertThat(SchemaParser.toJson(schema)).doesNotContain("collation");
    assertThat(SchemaParser.fromJson(SchemaParser.toJson(schema)).findField("name").collation())
        .isNull();
  }

  @Test
  public void collationOnNonStringFieldIsRejected() {
    assertThatThrownBy(
            () ->
                Types.NestedField.optional("n")
                    .withId(1)
                    .ofType(Types.LongType.get())
                    .withCollation("icu.en_US-ci")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot set collation on non-string field");
  }

  @Test
  public void caseInsensitiveComparatorTreatsCaseAsEqual() {
    Comparator<CharSequence> ci = Comparators.charSequences("icu.en_US-ci");
    assertThat(ci.compare("APPLE", "apple")).isEqualTo(0);
    assertThat(ci.compare("apple", "banana")).isNotEqualTo(0);

    // The default (binary) comparator distinguishes case.
    assertThat(Comparators.charSequences(null).compare("APPLE", "apple")).isNotEqualTo(0);
    assertThat(Comparators.charSequences("utf8").compare("APPLE", "apple")).isNotEqualTo(0);
  }

  @Test
  public void accentInsensitiveComparatorIgnoresDiacritics() {
    Comparator<CharSequence> ai = Comparators.charSequences("icu.en_US-ai");
    assertThat(ai.compare("résumé", "resume")).isEqualTo(0);
  }
}
