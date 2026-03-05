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
package org.apache.iceberg.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

public class TestNamespaceParser {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testNullNamespace() throws JsonProcessingException {
    String json = "null";
    Namespace namespace = NamespaceParser.fromJson(MAPPER.readTree(json));
    assertThat(namespace).isNull();
  }

  @Test
  public void testArrayNamespaceString() throws JsonProcessingException {
    String json = "[\"accounting\",\"tax\"]";
    Namespace namespace = NamespaceParser.fromJson(MAPPER.readTree(json));
    assertThat(namespace.levels()).containsExactly("accounting", "tax");
    assertThat(namespace.uuid()).isNull();

    String serialized = JsonUtil.generate(gen -> NamespaceParser.toJson(namespace, gen), false);
    assertThat(serialized).isEqualTo(json);
  }

  @Test
  public void testEmptyArrayNamespaceString() throws JsonProcessingException {
    String json = "[]";
    Namespace namespace = NamespaceParser.fromJson(MAPPER.readTree(json));
    assertThat(namespace.isEmpty()).isTrue();
    assertThat(namespace.uuid()).isNull();

    String serialized = JsonUtil.generate(gen -> NamespaceParser.toJson(namespace, gen), false);
    assertThat(serialized).isEqualTo(json);
  }

  @Test
  public void testObjectNamespaceString() throws JsonProcessingException {
    String json = "{\"namespace\":[\"accounting\",\"tax\"],\"namespace-uuid\":\"12345-67890\"}";
    Namespace namespace = NamespaceParser.fromJson(MAPPER.readTree(json));
    assertThat(namespace.levels()).containsExactly("accounting", "tax");
    assertThat(namespace.uuid()).isEqualTo("12345-67890");

    String serialized = JsonUtil.generate(gen -> NamespaceParser.toJson(namespace, gen), false);
    assertThat(serialized).isEqualTo(json);
  }

  @Test
  public void testInvalidNamespaceString() {
    String json = "\"accounting\"";
    assertThatThrownBy(() -> NamespaceParser.fromJson(MAPPER.readTree(json)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse namespace from non-array or non-object node");

    String missingNamespaceField = "{\"namespace-uuid\":\"12345-67890\"}";
    assertThatThrownBy(() -> NamespaceParser.fromJson(MAPPER.readTree(missingNamespaceField)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse namespace from object: missing namespace field");
  }
}
