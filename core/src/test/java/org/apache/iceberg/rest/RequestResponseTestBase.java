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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

public abstract class RequestResponseTestBase<T extends RESTMessage> {

  private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();

  public static ObjectMapper mapper() {
    return MAPPER;
  }

  /** Return a list of all the fields used in this class, as defined in the spec. */
  public abstract String[] allFieldsFromSpec();

  /** Return a valid instance of the request / response object. Used when validating fields. */
  public abstract T createExampleInstance();

  /**
   * Compare if two request / response objects are equivalent.
   *
   * <p>This helper method is used as opposed to implementing equals so that fields that deserialize
   * into null can be compared to the fields of instances created via the corresponding Builder,
   * which typically have a default value (such as an empty collection) for those fields.
   *
   * @param actual - request / response object to validate
   * @param expected - the corresponding object to check that {@code actual} is semantically
   *     equivalent to.
   */
  public abstract void assertEquals(T actual, T expected);

  /** Parse and return the input json into a value of type T. */
  public abstract T deserialize(String json) throws JsonProcessingException;

  /** Serialize T to a String. */
  public String serialize(T object) throws JsonProcessingException {
    return MAPPER.writeValueAsString(object);
  }

  /**
   * This test ensures that the serialized JSON of each class has only fields that are expected from
   * the spec. Only top level fields are checked presently, as nested fields generally come from
   * some existing type that is tested elsewhere. The fields from the spec should be populated into
   * each subclass's {@link RequestResponseTestBase#allFieldsFromSpec()}.
   */
  @Test
  public void testHasOnlyKnownFields() {
    Set<String> fieldsFromSpec = Sets.newHashSet();
    Collections.addAll(fieldsFromSpec, allFieldsFromSpec());
    try {
      JsonNode node = mapper().readValue(serialize(createExampleInstance()), JsonNode.class);
      for (String field : fieldsFromSpec) {
        assertThat(node.has(field)).as("Should have field: %s", field).isTrue();
      }

      for (String field : ((Iterable<? extends String>) node::fieldNames)) {
        assertThat(fieldsFromSpec).as("Should not have field: %s", field).contains(field);
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Test that the input JSON can be parsed into an equivalent object as {@code expected}, and then
   * re-serialized into the same JSON.
   */
  protected void assertRoundTripSerializesEquallyFrom(String json, T expected)
      throws JsonProcessingException {
    // Check that the JSON deserializes into the expected value;
    T actual = deserialize(json);
    assertEquals(actual, expected);

    // Check that the deserialized value serializes back into the original JSON
    assertThat(serialize(expected)).isEqualTo(json);
  }
}
