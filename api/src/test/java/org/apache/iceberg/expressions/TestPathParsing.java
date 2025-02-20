/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.expressions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestPathParsing {

  @Test
  public void testSimplePath() {
    assertThat(PathUtil.parse("$.event.id")).isEqualTo(List.of("event", "id"));
  }

  private static final String[] VALID_PATHS =
      new String[] {
        "$", // root path
        "$.event_id",
        "$.event.id",
        "$.\u2603", // snowman
        "$.\uD834\uDD1E", // surrogate pair, U+1D11E
      };

  @ParameterizedTest
  @FieldSource("VALID_PATHS")
  public void testExtractExpressionBindingPaths(String path) {
    assertThatCode(() -> PathUtil.parse(path)).doesNotThrowAnyException();
  }

  private static final String[] INVALID_PATHS =
      new String[] {
        null,
        "",
        "event_id", // missing root
        "$['event_id']", // uses bracket notation
        "$..event_id", // uses recursive descent
        "$.events[0].event_id", // uses position accessor
        "$.events.*", // uses wildcard
        "$.0invalid", // starts with a digit
        "$._\uD834", // dangling high surrogate
        "$._\uDC34", // low surrogate without high surrogate
      };

  @ParameterizedTest
  @FieldSource("INVALID_PATHS")
  public void testExtractBindingWithInvalidPath(String path) {
    assertThatThrownBy(() -> PathUtil.parse(path)).isInstanceOf(IllegalArgumentException.class);
  }
}
