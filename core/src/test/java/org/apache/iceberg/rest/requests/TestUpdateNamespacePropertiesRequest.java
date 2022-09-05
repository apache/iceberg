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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.junit.Assert;
import org.junit.Test;

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
    AssertHelpers.assertThrows(
        "A JSON request with an invalid type for one of the fields should fail to parse",
        JsonProcessingException.class,
        () -> deserialize(jsonInvalidTypeOnRemovalField));

    String jsonInvalidTypeOnUpdatesField =
        "{\"removals\":[\"foo\":\"bar\"],\"updates\":[\"owner\"]}";
    AssertHelpers.assertThrows(
        "A JSON value with an invalid type for one of the fields should fail to parse",
        JsonProcessingException.class,
        () -> deserialize(jsonInvalidTypeOnUpdatesField));

    // Valid top-level (array) types, but at least one entry in the list is not the expected type
    // NOTE: non-string values that are integral types will still parse into a string list.
    //    e.g. { removals: [ "foo", "bar", 1234 ] } will parse correctly.
    String invalidJsonWrongTypeInRemovalsList =
        "{\"removals\":[\"foo\",\"bar\", {\"owner\": \"Hank\"}],\"updates\":{\"owner\":\"Hank\"}}";
    AssertHelpers.assertThrows(
        "A JSON value with an invalid type inside one of the list fields should fail to parse",
        JsonProcessingException.class,
        () -> deserialize(invalidJsonWrongTypeInRemovalsList));

    String nullJson = null;
    AssertHelpers.assertThrows(
        "A null JSON should fail to parse",
        IllegalArgumentException.class,
        "argument \"content\" is null",
        () -> deserialize(nullJson));
  }

  @Test
  public void testBuilderDoesNotCreateInvalidObjects() {
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a key to remove",
        NullPointerException.class,
        "Invalid property to remove: null",
        () -> UpdateNamespacePropertiesRequest.builder().remove(null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null list of properties to remove",
        NullPointerException.class,
        "Invalid list of properties to remove: null",
        () -> UpdateNamespacePropertiesRequest.builder().removeAll(null).build());

    List<String> listWithNull = Lists.newArrayList("a", null, null);
    AssertHelpers.assertThrows(
        "The builder should not allow passing a list of properties to remove with a null element",
        IllegalArgumentException.class,
        "Invalid property to remove: null",
        () -> UpdateNamespacePropertiesRequest.builder().removeAll(listWithNull).build());

    AssertHelpers.assertThrows(
        "The builder should not allow using null as a key to update",
        NullPointerException.class,
        "Invalid property to update: null",
        () -> UpdateNamespacePropertiesRequest.builder().update(null, "100").build());

    AssertHelpers.assertThrows(
        "The builder should not allow using null as a value to update",
        NullPointerException.class,
        "Invalid value to update for key [owner]: null. Use remove instead",
        () -> UpdateNamespacePropertiesRequest.builder().update("owner", null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null collection of properties to update",
        NullPointerException.class,
        "Invalid collection of properties to update: null",
        () -> UpdateNamespacePropertiesRequest.builder().updateAll(null).build());

    Map<String, String> mapWithNullKey = Maps.newHashMap();
    mapWithNullKey.put(null, "hello");
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a key to update from a collection of updates",
        IllegalArgumentException.class,
        "Invalid property to update: null",
        () -> UpdateNamespacePropertiesRequest.builder().updateAll(mapWithNullKey).build());

    Map<String, String> mapWithMultipleNullValues = Maps.newHashMap();
    mapWithMultipleNullValues.put("a", null);
    mapWithMultipleNullValues.put("b", "b");
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a value to update from a collection of updates",
        IllegalArgumentException.class,
        "Invalid value to update for properties [a]: null. Use remove instead",
        () ->
            UpdateNamespacePropertiesRequest.builder()
                .updateAll(mapWithMultipleNullValues)
                .build());
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
    Assert.assertEquals(
        "Properties to update should be equal", actual.updates(), expected.updates());
    Assert.assertEquals(
        "Properties to remove should be equal",
        Sets.newHashSet(actual.removals()),
        Sets.newHashSet(expected.removals()));
  }

  @Override
  public UpdateNamespacePropertiesRequest deserialize(String json) throws JsonProcessingException {
    UpdateNamespacePropertiesRequest request =
        mapper().readValue(json, UpdateNamespacePropertiesRequest.class);
    request.validate();
    return request;
  }
}
