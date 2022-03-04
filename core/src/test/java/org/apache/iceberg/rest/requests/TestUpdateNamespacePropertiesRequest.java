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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestUpdateNamespacePropertiesRequest extends RequestResponseTestBase<UpdateNamespacePropertiesRequest> {

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
        fullJson, ImmutableUpdateNamespacePropertiesRequest.builder().updates(UPDATES).removals(REMOVALS).build());

    // Only updates
    String emptyRemoval = "{\"removals\":[],\"updates\":{\"owner\":\"Hank\"}}";
    assertRoundTripSerializesEquallyFrom(
        emptyRemoval, ImmutableUpdateNamespacePropertiesRequest.builder()
            .updates(UPDATES).removals(EMPTY_REMOVALS).build());

    assertRoundTripSerializesEquallyFrom(
        emptyRemoval, ImmutableUpdateNamespacePropertiesRequest.builder().putUpdates("owner", "Hank").build());

    // Only removals
    String emptyUpdates = "{\"removals\":[\"foo\",\"bar\"],\"updates\":{}}";
    assertRoundTripSerializesEquallyFrom(
        emptyUpdates, ImmutableUpdateNamespacePropertiesRequest.builder()
            .removals(REMOVALS).updates(EMPTY_UPDATES).build());

    assertRoundTripSerializesEquallyFrom(
        emptyUpdates, ImmutableUpdateNamespacePropertiesRequest.builder().addRemovals("foo", "bar").build());

    // All empty
    String jsonAllFieldsEmpty = "{\"removals\":[],\"updates\":{}}";
    assertRoundTripSerializesEquallyFrom(
        jsonAllFieldsEmpty, ImmutableUpdateNamespacePropertiesRequest.builder().build());
  }

  @Test
  // Test cases that can't be constructed with our Builder class e2e but that will parse correctly
  public void testCanDeserializeWithoutDefaultValues() throws JsonProcessingException {
    // `removals` is null
    UpdateNamespacePropertiesRequest noRemovals = ImmutableUpdateNamespacePropertiesRequest.builder()
        .updates(UPDATES).build();
    String jsonWithNullRemovals = "{\"removals\":null,\"updates\":{\"owner\":\"Hank\"}}";
    UpdateNamespacePropertiesRequest parsed = deserialize(jsonWithNullRemovals);
    assertEquals(parsed, noRemovals);

    // `removals` is missing from the JSON
    String jsonWithMissingRemovals = "{\"updates\":{\"owner\":\"Hank\"}}";
    assertEquals(deserialize(jsonWithMissingRemovals), noRemovals);

    // `updates` is null
    UpdateNamespacePropertiesRequest noUpdates = ImmutableUpdateNamespacePropertiesRequest.builder()
        .removals(REMOVALS).build();
    String jsonWithNullUpdates = "{\"removals\":[\"foo\",\"bar\"],\"updates\":null}";
    assertEquals(deserialize(jsonWithNullUpdates), noUpdates);

    // `updates` is missing from the JSON
    String jsonWithMissingUpdates = "{\"removals\":[\"foo\",\"bar\"]}";
    assertEquals(deserialize(jsonWithMissingUpdates), noUpdates);

    // all null / no values set
    UpdateNamespacePropertiesRequest allMissing = ImmutableUpdateNamespacePropertiesRequest.builder().build();
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
    Assertions.assertThatThrownBy(() -> deserialize(jsonInvalidTypeOnRemovalField))
        .isInstanceOf(JsonProcessingException.class);

    String jsonInvalidTypeOnUpdatesField =
        "{\"removals\":[\"foo\":\"bar\"],\"updates\":[\"owner\"]}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonInvalidTypeOnUpdatesField))
        .isInstanceOf(JsonProcessingException.class);

    // Valid top-level (array) types, but at least one entry in the list is not the expected type
    // NOTE: non-string values that are integral types will still parse into a string list.
    //    e.g. { removals: [ "foo", "bar", 1234 ] } will parse correctly.
    String invalidJsonWrongTypeInRemovalsList =
        "{\"removals\":[\"foo\",\"bar\", {\"owner\": \"Hank\"}],\"updates\":{\"owner\":\"Hank\"}}";
    Assertions.assertThatThrownBy(() -> deserialize(invalidJsonWrongTypeInRemovalsList))
        .isInstanceOf(JsonProcessingException.class);

    String nullJson = null;
    Assertions.assertThatThrownBy(() -> deserialize(nullJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("argument \"content\" is null");
  }

  @Test
  public void testBuilderDoesNotCreateInvalidObjects() {
    Assertions.assertThatThrownBy(() ->
            ImmutableUpdateNamespacePropertiesRequest.builder().addRemovals((String) null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("removals element");

    Assertions.assertThatThrownBy(() ->
            ImmutableUpdateNamespacePropertiesRequest.builder().removals(null).build())
        .isInstanceOf(NullPointerException.class);

    List<String> listWithNull = Lists.newArrayList("a", null, null);
    Assertions.assertThatThrownBy(() ->
            ImmutableUpdateNamespacePropertiesRequest.builder().removals(listWithNull).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("removals element");

    Assertions.assertThatThrownBy(() ->
            ImmutableUpdateNamespacePropertiesRequest.builder().putUpdates(null, "100").build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("updates key");

    Assertions.assertThatThrownBy(() ->
            ImmutableUpdateNamespacePropertiesRequest.builder().putUpdates("owner", null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("updates value");

    Assertions.assertThatThrownBy(() ->
            ImmutableUpdateNamespacePropertiesRequest.builder().updates(null).build())
        .isInstanceOf(NullPointerException.class);

    Map<String, String> mapWithNullKey = Maps.newHashMap();
    mapWithNullKey.put(null, "hello");
    Assertions.assertThatThrownBy(() ->
            ImmutableUpdateNamespacePropertiesRequest.builder().updates(mapWithNullKey).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("updates key");

    Map<String, String> mapWithMultipleNullValues = Maps.newHashMap();
    mapWithMultipleNullValues.put("a", null);
    mapWithMultipleNullValues.put("b", "b");
    Assertions.assertThatThrownBy(() ->
            ImmutableUpdateNamespacePropertiesRequest.builder().updates(mapWithMultipleNullValues).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("updates value");
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] { "updates", "removals" };
  }

  @Override
  public UpdateNamespacePropertiesRequest createExampleInstance() {
    return ImmutableUpdateNamespacePropertiesRequest.builder()
        .updates(UPDATES)
        .removals(REMOVALS)
        .build();
  }

  @Override
  public void assertEquals(UpdateNamespacePropertiesRequest actual, UpdateNamespacePropertiesRequest expected) {
    Assert.assertEquals("Properties to update should be equal", actual.updates(), expected.updates());
    Assert.assertEquals("Properties to remove should be equal",
        Sets.newHashSet(actual.removals()), Sets.newHashSet(expected.removals()));
  }

  @Override
  public UpdateNamespacePropertiesRequest deserialize(String json) throws JsonProcessingException {
    return mapper().readValue(json, ImmutableUpdateNamespacePropertiesRequest.class);
  }
}
