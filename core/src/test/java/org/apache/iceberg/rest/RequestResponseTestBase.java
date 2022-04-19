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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class RequestResponseTestBase<T extends RESTMessage> {

  private static final JsonFactory FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  @BeforeClass
  public static void beforeClass() {
    RESTSerializers.registerAll(MAPPER);
    // This is a workaround for Jackson since Iceberg doesn't use the standard get/set bean notation.
    // This allows Jackson to work with the fields directly (both public and private) and not require
    // custom serializers for all the request/response objects.
    MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
  }

  public static ObjectMapper mapper() {
    return MAPPER;
  }

  /**
   * Return a list of all the fields used in this class, as defined in the spec.
   */
  public abstract String[] allFieldsFromSpec();

  /**
   * Return a valid instance of the request / response object. Used when validating fields.
   */
  public abstract T createExampleInstance();

  /**
   * Compare if two request / response objects are equivalent.
   * <p>
   * This helper method is used as opposed to implementing equals so that fields that deserialize into
   * null can be compared to the fields of instances created via the corresponding Builder, which typically
   * have a default value (such as an empty collection) for those fields.
   * @param actual - request / response object to validate
   * @param expected - the corresponding object to check that {@code actual} is semantically equivalent to.
   */
  public abstract void assertEquals(T actual, T expected);

  /**
   * Parse and return the input json into a value of type T.
   */
  public abstract T deserialize(String json) throws JsonProcessingException;

  /**
   * This test ensures that only the fields that are expected, e.g. from the spec, are found on the class.
   * If new fields are added to the spec, they should be added to the function
   * {@link RequestResponseTestBase#allFieldsFromSpec()}
   */
  @Test
  public void testHasOnlyKnownFields() {
    T value = createExampleInstance();

    Assertions.assertThat(value)
        .hasOnlyFields(allFieldsFromSpec());
  }

  /**
   * Test that the input JSON can be parsed into an equivalent object as {@code expected}, and then
   * re-serialized into the same JSON.
   */
  protected void assertRoundTripSerializesEquallyFrom(String json, T expected) throws JsonProcessingException {
    // Check that the JSON deserializes into the expected value;
    T actual = deserialize(json);
    assertEquals(actual, expected);

    // Check that the deserialized value serializes back into the original JSON
    Assertions.assertThat(MAPPER.writeValueAsString(actual))
        .isEqualTo(json);
  }
}
