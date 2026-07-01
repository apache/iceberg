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
package org.apache.iceberg.deletes;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

public class TestEqualityDeleteWriter {
  private static final Schema SCHEMA =
      new Schema(
          NestedField.required(1, "id", Types.IntegerType.get()),
          NestedField.optional(2, "name", Types.StringType.get()));

  @Test
  public void testValidateRejectsNullEqualityFieldIds() {
    assertThatThrownBy(() -> EqualityDeleteWriter.validateEqualityFieldIds(null, SCHEMA))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Equality delete field ids must not be null or empty");
  }

  @Test
  public void testValidateRejectsEmptyEqualityFieldIds() {
    assertThatThrownBy(() -> EqualityDeleteWriter.validateEqualityFieldIds(new int[0], SCHEMA))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Equality delete field ids must not be null or empty");
  }

  @Test
  public void testValidateRejectsDuplicateEqualityFieldIds() {
    assertThatThrownBy(
            () -> EqualityDeleteWriter.validateEqualityFieldIds(new int[] {1, 1}, SCHEMA))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate equality delete field id: 1");
  }

  @Test
  public void testValidateRejectsMissingEqualityFieldId() {
    assertThatThrownBy(() -> EqualityDeleteWriter.validateEqualityFieldIds(new int[] {99}, SCHEMA))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid equality delete field id: 99");
  }

  @Test
  public void testValidateRejectsNullSchema() {
    assertThatThrownBy(() -> EqualityDeleteWriter.validateEqualityFieldIds(new int[] {1}, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Schema must not be null");
  }

  @Test
  public void testValidateAcceptsValidEqualityFieldIds() {
    assertThatNoException()
        .isThrownBy(() -> EqualityDeleteWriter.validateEqualityFieldIds(new int[] {1}, SCHEMA));
    assertThatNoException()
        .isThrownBy(() -> EqualityDeleteWriter.validateEqualityFieldIds(new int[] {1, 2}, SCHEMA));
  }
}
