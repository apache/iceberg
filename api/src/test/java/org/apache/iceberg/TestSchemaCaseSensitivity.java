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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestSchemaCaseSensitivity {

  @Test
  public void
      testSchemaWithColumnNamesThatDifferOnlyInLetterCaseThrowsOnCaseInsensitiveFindField() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            required(3, "DATA", Types.StringType.get()));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> schema.caseInsensitiveFindField("DATA"))
        .withMessage(
            "Unable to build field name to id mapping because two fields have the same lower case name: data and DATA");
  }

  @Test
  public void
      testSchemaWithColumnNamesThatDifferOnlyInLetterCaseSucceedsOnCaseSensitiveFindField() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            required(3, "DATA", Types.StringType.get()));

    Types.NestedField actual1 = schema.findField("data");
    assertThat(actual1).isEqualTo(Types.NestedField.required(2, "data", Types.StringType.get()));
    Types.NestedField actual2 = schema.findField("DATA");
    assertThat(actual2).isEqualTo(Types.NestedField.required(3, "DATA", Types.StringType.get()));
  }

  @Test
  public void testCaseInsensitiveFindFieldSucceeds() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

    Types.NestedField actual1 = schema.caseInsensitiveFindField("Data");
    assertThat(actual1).isEqualTo(Types.NestedField.required(2, "data", Types.StringType.get()));
  }
}
