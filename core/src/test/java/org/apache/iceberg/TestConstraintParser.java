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

import java.util.List;
import org.junit.jupiter.api.Test;

public class TestConstraintParser {
  private static final String CHECK_EXPRESSION =
      "{\"type\":\"gt\",\"left\":{\"type\":\"reference\",\"id\":1},\"right\":0}";

  @Test
  public void testPrimaryKeyRoundTrip() {
    Constraint constraint = BaseConstraint.primaryKey(1, "pk", false, 1000L, List.of(1, 2));

    String expectedJson =
        "{\"constraint-id\":1,\"type\":\"primary-key\",\"name\":\"pk\",\"enforced\":false,"
            + "\"timestamp-ms\":1000,\"field-ids\":[1,2]}";
    String json = ConstraintParser.toJson(constraint, false);
    assertThat(json).isEqualTo(expectedJson);

    Constraint actual = ConstraintParser.fromJson(json);
    assertThat(actual).isEqualTo(constraint).isInstanceOf(PrimaryKeyConstraint.class);
    assertThat(((KeyConstraint) actual).fieldIds()).containsExactly(1, 2);
  }

  @Test
  public void testUniqueRoundTrip() {
    Constraint constraint = BaseConstraint.unique(2, "uq_email", true, 1000L, List.of(3));

    Constraint actual = ConstraintParser.fromJson(ConstraintParser.toJson(constraint));
    assertThat(actual).isEqualTo(constraint).isInstanceOf(UniqueConstraint.class);
    assertThat(actual.type()).isEqualTo(ConstraintType.UNIQUE);
    assertThat(((KeyConstraint) actual).fieldIds()).containsExactly(3);
  }

  @Test
  public void testCheckRoundTrip() {
    Constraint constraint = BaseConstraint.check(3, "chk_amount", true, 1000L, CHECK_EXPRESSION);

    String expectedJson =
        "{\"constraint-id\":3,\"type\":\"check\",\"name\":\"chk_amount\",\"enforced\":true,"
            + "\"timestamp-ms\":1000,\"expression\":"
            + CHECK_EXPRESSION
            + "}";
    String json = ConstraintParser.toJson(constraint, false);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(ConstraintParser.fromJson(json))
        .isEqualTo(constraint)
        .isInstanceOf(CheckConstraint.class);
  }

  @Test
  public void testCheckWithBooleanExpression() {
    String json =
        "{\"constraint-id\":1,\"type\":\"check\",\"name\":\"chk\",\"enforced\":false,"
            + "\"timestamp-ms\":1000,\"expression\":true}";
    assertThat(ConstraintParser.toJson(ConstraintParser.fromJson(json), false)).isEqualTo(json);
  }

  @Test
  public void testConstraintsOfDifferentTypesAreNotEqual() {
    assertThat(BaseConstraint.unique(1, "c", false, 1000L, List.of(1)))
        .isNotEqualTo(BaseConstraint.primaryKey(1, "c", false, 1000L, List.of(1)));
  }

  @Test
  public void testInvalidType() {
    assertThatThrownBy(
            () ->
                ConstraintParser.fromJson(
                    "{\"constraint-id\":1,\"type\":\"foreign-key\",\"name\":\"fk\","
                        + "\"enforced\":false,\"timestamp-ms\":1000,\"field-ids\":[1]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid constraint type: foreign-key");
  }

  @Test
  public void testMissingRequiredFields() {
    assertThatThrownBy(
            () ->
                ConstraintParser.fromJson(
                    "{\"type\":\"unique\",\"name\":\"uq\",\"enforced\":false,"
                        + "\"timestamp-ms\":1000,\"field-ids\":[1]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: constraint-id");

    assertThatThrownBy(
            () ->
                ConstraintParser.fromJson(
                    "{\"constraint-id\":1,\"type\":\"unique\",\"name\":\"uq\","
                        + "\"timestamp-ms\":1000,\"field-ids\":[1]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing boolean: enforced");
  }

  @Test
  public void testCheckPayloadValidation() {
    assertThatThrownBy(
            () ->
                ConstraintParser.fromJson(
                    "{\"constraint-id\":1,\"type\":\"check\",\"name\":\"chk\","
                        + "\"enforced\":false,\"timestamp-ms\":1000}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid check constraint chk: missing expression");

    assertThatThrownBy(
            () ->
                ConstraintParser.fromJson(
                    "{\"constraint-id\":1,\"type\":\"check\",\"name\":\"chk\",\"enforced\":false,"
                        + "\"timestamp-ms\":1000,\"expression\":true,\"field-ids\":[1]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid check constraint chk: field IDs must not be set");
  }

  @Test
  public void testKeyPayloadValidation() {
    assertThatThrownBy(
            () ->
                ConstraintParser.fromJson(
                    "{\"constraint-id\":1,\"type\":\"unique\",\"name\":\"uq\","
                        + "\"enforced\":false,\"timestamp-ms\":1000}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid unique constraint uq: missing field IDs");

    assertThatThrownBy(
            () ->
                ConstraintParser.fromJson(
                    "{\"constraint-id\":1,\"type\":\"unique\",\"name\":\"uq\",\"enforced\":false,"
                        + "\"timestamp-ms\":1000,\"expression\":true,\"field-ids\":[1]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid unique constraint uq: expression must not be set");

    assertThatThrownBy(() -> BaseConstraint.unique(1, "uq", false, 1000L, List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid unique constraint uq: field IDs must not be null or empty");

    assertThatThrownBy(() -> BaseConstraint.primaryKey(1, "pk", false, 1000L, List.of(1, 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid primary-key constraint pk: duplicate field ID 1");
  }
}
