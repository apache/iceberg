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
package org.apache.iceberg.rest.requests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.junit.jupiter.api.Test;

public class TestUpdateNamespacePropertiesRequest
    extends RequestResponseTestBase<UpdateNamespacePropertiesRequest> {

  /* Values used to fill in request fields */
  private static final Map<String, String> UPDATES = ImmutableMap.of("owner", "Hank");
  private static final List<String> REMOVALS = ImmutableList.of("foo", "bar");
  private static final Map<String, String> EMPTY_UPDATES = ImmutableMap.of();
  private static final List<String> EMPTY_REMOVALS = ImmutableList.of();

  @Test
  public void testRoundTripSerDe() throws JsonProcessingException {
    // Full request
    String fullJson = "{\"removals\":[\"foo\",\"bar\"],\"updates\":{\"owner\":\"Hank\"}}";
    assertRoundTripSerializesEquallyFrom(
        fullJson,
        UpdateNamespacePropertiesRequest.builder().updateAll(UPDATES).removeAll(REMOVALS).build());

    // Only updates
    String emptyRemoval = "{\"removals\":[],\"updates\":{\"owner\":\"Hank\"}}";
    assertRoundTripSerializesEquallyFrom(
        emptyRemoval,
        UpdateNamespacePropertiesRequest.builder()
            .updateAll(UPDATES)
            .removeAll(EMPTY_REMOVALS)
            .build());

    assertRoundTripSerializesEquallyFrom(
        emptyRemoval, UpdateNamespacePropertiesRequest.builder().update("owner", "Hank").build());

    // Only removals
    String emptyUpdates = "{\"removals\":[\"foo\",\"bar\"],\"updates\":{}}";
    assertRoundTripSerializesEquallyFrom(
        emptyUpdates,
        UpdateNamespacePropertiesRequest.builder()
            .removeAll(REMOVALS)
            .updateAll(EMPTY_UPDATES)
            .build());

    assertRoundTripSerializesEquallyFrom(
        emptyUpdates,
        UpdateNamespacePropertiesRequest.builder().remove("foo").remove("bar").build());

    // All empty
    String jsonAllFieldsEmpty = "{\"removals\":[],\"updates\":{}}";
    assertRoundTripSerializesEquallyFrom(
        jsonAllFieldsEmpty, UpdateNamespacePropertiesRequest.builder().build());
  }

  @Test
  // Test cases that can't be constructed with our Builder class e2e but that will parse correctly
  public void testCanDeserializeWithoutDefaultValues() throws JsonProcessingException {
    // `removals` is null
    UpdateNamespacePropertiesRequest noRemovals =
        UpdateNamespacePropertiesRequest.builder().updateAll(UPDATES).build();
    String jsonWithNullRemovals = "{\"removals\":null,\"updates\":{\"owner\":\"Hank\"}}";
    UpdateNamespacePropertiesRequest parsed = deserialize(jsonWithNullRemovals);
    assertEquals(parsed, noRemovals);

    // `removals` is missing from the JSON
    String jsonWithMissingRemovals = "{\"updates\":{\"owner\":\"Hank\"}}";
    assertEquals(deserialize(jsonWithMissingRemovals), noRemovals);

    // `updates` is null
    UpdateNamespacePropertiesRequest noUpdates =
        UpdateNamespacePropertiesRequest.builder().removeAll(REMOVALS).build();
    String jsonWithNullUpdates = "{\"removals\":[\"foo\",\"bar\"],\"updates\":null}";
    assertEquals(deserialize(jsonWithNullUpdates), noUpdates);

    // `updates` is missing from the JSON
    String jsonWithMissingUpdates = "{\"removals\":[\"foo\",\"bar\"]}";
    assertEquals(deserialize(jsonWithMissingUpdates), noUpdates);

    // all null / no values set
    UpdateNamespacePropertiesRequest allMissing =
        UpdateNamespacePropertiesRequest.builder().build();
    String jsonAllNull = "{\"removals\":null,\"updates\":null}";
    assertEquals(deserialize(jsonAllNull), allMissing);

    String jsonAllMissing = "{}";
    assertEquals(deserialize(jsonAllMissing), allMissing);
  }

  @Test
  public void testParseInvalidJson() {
    // Invalid top-level types
    String jsonInvalidTypeOnRemovalField =
        "{\"removals\":{\"foo\":\"bar\"},\"updates\":{\"owner\":\"Hank\"}}";
    assertThatThrownBy(() -> deserialize(jsonInvalidTypeOnRemovalField))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageStartingWith("Cannot deserialize value of type");

    String jsonInvalidTypeOnUpdatesField =
        "{\"removals\":[\"foo\":\"bar\"],\"updates\":[\"owner\"]}";
    assertThatThrownBy(() -> deserialize(jsonInvalidTypeOnUpdatesField))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageStartingWith("Unexpected character")
        .hasMessageContaining("expecting comma to separate Array entries");

    // Valid top-level (array) types, but at least one entry in the list is not the expected type
    // NOTE: non-string values that are integral types will still parse into a string list.
    //    e.g. { removals: [ "foo", "bar", 1234 ] } will parse correctly.
    String invalidJsonWrongTypeInRemovalsList =
        "{\"removals\":[\"foo\",\"bar\", {\"owner\": \"Hank\"}],\"updates\":{\"owner\":\"Hank\"}}";
    assertThatThrownBy(() -> deserialize(invalidJsonWrongTypeInRemovalsList))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageStartingWith("Cannot deserialize value of type");

    String nullJson = null;
    assertThatThrownBy(() -> deserialize(nullJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("argument \"content\" is null");
  }

  @Test
  public void testBuilderDoesNotCreateInvalidObjects() {
    assertThatThrownBy(() -> UpdateNamespacePropertiesRequest.builder().remove(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid property to remove: null");

    assertThatThrownBy(() -> UpdateNamespacePropertiesRequest.builder().removeAll(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid list of properties to remove: null");

    List<String> listWithNull = Lists.newArrayList("a", null, null);
    assertThatThrownBy(
            () -> UpdateNamespacePropertiesRequest.builder().removeAll(listWithNull).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid property to remove: null");

    assertThatThrownBy(() -> UpdateNamespacePropertiesRequest.builder().update(null, "100").build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid property to update: null");

    assertThatThrownBy(
            () -> UpdateNamespacePropertiesRequest.builder().update("owner", null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid value to update for key [owner]: null. Use remove instead");

    assertThatThrownBy(() -> UpdateNamespacePropertiesRequest.builder().updateAll(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid collection of properties to update: null");

    Map<String, String> mapWithNullKey = Maps.newHashMap();
    mapWithNullKey.put(null, "hello");
    assertThatThrownBy(
            () -> UpdateNamespacePropertiesRequest.builder().updateAll(mapWithNullKey).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid property to update: null");

    Map<String, String> mapWithMultipleNullValues = Maps.newHashMap();
    mapWithMultipleNullValues.put("a", null);
    mapWithMultipleNullValues.put("b", "b");
    assertThatThrownBy(
            () ->
                UpdateNamespacePropertiesRequest.builder()
                    .updateAll(mapWithMultipleNullValues)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid value to update for properties [a]: null. Use remove instead");
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"updates", "removals"};
  }

  @Override
  public UpdateNamespacePropertiesRequest createExampleInstance() {
    return UpdateNamespacePropertiesRequest.builder()
        .updateAll(UPDATES)
        .removeAll(REMOVALS)
        .build();
  }

  @Override
  public void assertEquals(
      UpdateNamespacePropertiesRequest actual, UpdateNamespacePropertiesRequest expected) {
    assertThat(actual.updates())
        .as("Properties to update should be equal")
        .isEqualTo(expected.updates());
    assertThat(Sets.newHashSet(actual.removals()))
        .as("Properties to remove should be equal")
        .containsExactlyInAnyOrderElementsOf(Sets.newHashSet(expected.removals()));
  }

  @Override
  public UpdateNamespacePropertiesRequest deserialize(String json) throws JsonProcessingException {
    UpdateNamespacePropertiesRequest request =
        mapper().readValue(json, UpdateNamespacePropertiesRequest.class);
    request.validate();
    return request;
  }
}
