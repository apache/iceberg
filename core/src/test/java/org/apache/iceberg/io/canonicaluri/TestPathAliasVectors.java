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
package org.apache.iceberg.io.canonicaluri;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class TestPathAliasVectors {
  @Test
  void appliesSharedMappingVectors() throws IOException {
    JsonNode vectors = vectors();
    assertThat(vectors.get("version").asInt()).isEqualTo(1);
    assertThat(vectors.get("unmatched").asText()).isEqualTo("fail");
    assertThat(vectors.get("writeMode").asText()).isEqualTo("strict");

    List<MappingRule> rules = new ArrayList<>();
    for (JsonNode rule : vectors.withArray("rules")) {
      rules.add(
          new MappingRule(
              rule.get("id").asText(),
              rule.get("canonical").asText(),
              rule.get("physical").asText()));
    }

    PathMapper mapper = new PathMapper(true, PathMapper.UnmatchedPolicy.FAIL, rules);
    for (JsonNode testCase : vectors.withArray("valid")) {
      String physical = mapper.toPhysical(testCase.get("input").asText(), false).location();
      assertThat(physical)
          .as(testCase.get("name").asText())
          .isEqualTo(testCase.get("expectedPhysical").asText());
      assertThat(mapper.toCanonical(physical).location())
          .as(testCase.get("name").asText())
          .isEqualTo(testCase.get("expectedCanonical").asText());
    }

    for (JsonNode invalid : vectors.withArray("invalid")) {
      assertThatThrownBy(() -> mapper.toPhysical(invalid.asText(), false))
          .as(invalid.asText())
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  private static JsonNode vectors() throws IOException {
    try (InputStream input =
        TestPathAliasVectors.class.getResourceAsStream("/pathalias-test-vectors.json")) {
      if (input == null) {
        throw new IllegalStateException("Missing pathalias-test-vectors.json");
      }

      return new ObjectMapper().readTree(new String(input.readAllBytes(), UTF_8));
    }
  }
}
