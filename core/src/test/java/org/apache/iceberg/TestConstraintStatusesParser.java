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

import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestConstraintStatusesParser {

  @Test
  public void testRoundTrip() {
    Map<Integer, ConstraintStatus> statuses =
        Map.of(
            1, ConstraintStatus.VALIDATED,
            3, ConstraintStatus.VALID,
            2, ConstraintStatus.VALID,
            4, ConstraintStatus.INVALID,
            5, ConstraintStatus.UNVALIDATED);

    String expectedJson = "{\"validated\":[1],\"valid\":[2,3],\"invalid\":[4],\"unvalidated\":[5]}";
    String json = ConstraintStatusesParser.toJson(statuses, false);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(ConstraintStatusesParser.fromJson(json)).isEqualTo(statuses);
  }

  @Test
  public void testEmptyListsAreOmitted() {
    String json = ConstraintStatusesParser.toJson(Map.of(1, ConstraintStatus.VALIDATED), false);
    assertThat(json).isEqualTo("{\"validated\":[1]}");
    assertThat(ConstraintStatusesParser.fromJson("{\"validated\":[1],\"valid\":[]}"))
        .isEqualTo(Map.of(1, ConstraintStatus.VALIDATED));

    assertThat(ConstraintStatusesParser.toJson(Map.of(), false)).isEqualTo("{}");
    assertThat(ConstraintStatusesParser.fromJson("{}")).isEmpty();
  }

  @Test
  public void testConstraintListedMoreThanOnce() {
    assertThatThrownBy(
            () -> ConstraintStatusesParser.fromJson("{\"validated\":[1],\"invalid\":[2,1]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid constraint statuses: constraint 1 is listed more than once");
  }

  @Test
  public void testNonObject() {
    assertThatThrownBy(() -> ConstraintStatusesParser.fromJson("[1]"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse constraint statuses from non-object: [1]");
  }
}
