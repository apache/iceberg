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
package org.apache.iceberg.rest.events.parsers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.events.operations.DropNamespaceOperation;
import org.apache.iceberg.rest.events.operations.ImmutableDropNamespaceOperation;
import org.junit.jupiter.api.Test;

public class TestDropNamespaceOperationParser {
  @Test
  void testToJson() {
    DropNamespaceOperation op =
        ImmutableDropNamespaceOperation.builder().namespace(Namespace.of("a", "b")).build();
    String expected = "{\"operation-type\":\"drop-namespace\",\"namespace\":[\"a\",\"b\"]}";
    assertThat(DropNamespaceOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    DropNamespaceOperation op =
        ImmutableDropNamespaceOperation.builder().namespace(Namespace.of("a", "b")).build();
    String expected =
        "{\n"
            + "  \"operation-type\" : \"drop-namespace\",\n"
            + "  \"namespace\" : [ \"a\", \"b\" ]\n"
            + "}";
    assertThat(DropNamespaceOperationParser.toJsonPretty(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatNullPointerException()
        .isThrownBy(() -> DropNamespaceOperationParser.toJson(null))
        .withMessage("Invalid drop namespace operation: null");
  }

  @Test
  void testFromJson() {
    String json = "{\"operation-type\":\"drop-namespace\",\"namespace\":[\"a\",\"b\"]}";
    DropNamespaceOperation expected =
        ImmutableDropNamespaceOperation.builder().namespace(Namespace.of("a", "b")).build();
    assertThat(DropNamespaceOperationParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> DropNamespaceOperationParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse drop namespace operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingNamespace = "{\"operation-type\":\"drop-namespace\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> DropNamespaceOperationParser.fromJson(missingNamespace));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String jsonInvalidNamespace = "{\"operation-type\":\"drop-namespace\",\"namespace\":\"a\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> DropNamespaceOperationParser.fromJson(jsonInvalidNamespace));
  }
}
