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
package org.apache.iceberg.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestViewRepresentationParser {

  @Test
  public void testParseUnknownViewRepresentation() {
    String json = "{\"type\":\"unknown-sql-representation\"}";
    ViewRepresentation unknownRepresentation = ViewRepresentationParser.fromJson(json);
    assertThat(
            ImmutableUnknownViewRepresentation.builder().type("unknown-sql-representation").build())
        .isEqualTo(unknownRepresentation);

    assertThatThrownBy(() -> ViewRepresentationParser.toJson(unknownRepresentation))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot serialize unsupported view representation: unknown-sql-representation");
  }

  @Test
  public void testNullViewRepresentation() {
    assertThatThrownBy(() -> ViewRepresentationParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid view representation: null");
  }

  @Test
  public void testViewRepresentationMissingType() {
    assertThatThrownBy(() -> ViewRepresentationParser.fromJson("{\"sql\":\"select * from foo\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: type");
  }
}
