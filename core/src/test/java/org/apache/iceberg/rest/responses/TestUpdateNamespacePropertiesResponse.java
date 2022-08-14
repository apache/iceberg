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
package org.apache.iceberg.rest.responses;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.junit.Assert;
import org.junit.Test;

public class TestUpdateNamespacePropertiesResponse
    extends RequestResponseTestBase<UpdateNamespacePropertiesResponse> {

  /* Values used to fill in response fields */
  private static final List<String> UPDATED = ImmutableList.of("owner");
  private static final List<String> REMOVED = ImmutableList.of("foo");
  private static final List<String> MISSING = ImmutableList.of("bar");
  private static final List<String> EMPTY_LIST = ImmutableList.of();

  @Test
  public void testRoundTripSerDe() throws JsonProcessingException {
    // Full request
    String fullJson = "{\"removed\":[\"foo\"],\"updated\":[\"owner\"],\"missing\":[\"bar\"]}";
    assertRoundTripSerializesEquallyFrom(
        fullJson,
        UpdateNamespacePropertiesResponse.builder()
            .addUpdated(UPDATED)
            .addRemoved(REMOVED)
            .addMissing(MISSING)
            .build());

    // Only updated
    String jsonOnlyUpdated = "{\"removed\":[],\"updated\":[\"owner\"],\"missing\":[]}";
    assertRoundTripSerializesEquallyFrom(
        jsonOnlyUpdated, UpdateNamespacePropertiesResponse.builder().addUpdated(UPDATED).build());
    assertRoundTripSerializesEquallyFrom(
        jsonOnlyUpdated, UpdateNamespacePropertiesResponse.builder().addUpdated("owner").build());

    assertRoundTripSerializesEquallyFrom(
        jsonOnlyUpdated,
        UpdateNamespacePropertiesResponse.builder()
            .addUpdated(UPDATED)
            .addMissing(EMPTY_LIST)
            .addRemoved(EMPTY_LIST)
            .build());

    // Only removed
    String jsonOnlyRemoved = "{\"removed\":[\"foo\"],\"updated\":[],\"missing\":[]}";
    assertRoundTripSerializesEquallyFrom(
        jsonOnlyRemoved, UpdateNamespacePropertiesResponse.builder().addRemoved(REMOVED).build());
    assertRoundTripSerializesEquallyFrom(
        jsonOnlyRemoved, UpdateNamespacePropertiesResponse.builder().addRemoved("foo").build());

    assertRoundTripSerializesEquallyFrom(
        jsonOnlyRemoved,
        UpdateNamespacePropertiesResponse.builder()
            .addRemoved(REMOVED)
            .addUpdated(EMPTY_LIST)
            .addMissing(EMPTY_LIST)
            .build());

    // Only missing
    String jsonOnlyMissing = "{\"removed\":[],\"updated\":[],\"missing\":[\"bar\"]}";
    assertRoundTripSerializesEquallyFrom(
        jsonOnlyMissing, UpdateNamespacePropertiesResponse.builder().addMissing(MISSING).build());

    assertRoundTripSerializesEquallyFrom(
        jsonOnlyMissing, UpdateNamespacePropertiesResponse.builder().addMissing("bar").build());

    assertRoundTripSerializesEquallyFrom(
        jsonOnlyMissing,
        UpdateNamespacePropertiesResponse.builder()
            .addMissing(MISSING)
            .addUpdated(EMPTY_LIST)
            .addRemoved(EMPTY_LIST)
            .build());

    // All fields are empty
    String jsonWithAllFieldsAsEmptyList = "{\"removed\":[],\"updated\":[],\"missing\":[]}";
    assertRoundTripSerializesEquallyFrom(
        jsonWithAllFieldsAsEmptyList, UpdateNamespacePropertiesResponse.builder().build());
  }

  @Test
  // Test cases that can't be constructed with our Builder class e2e but that will parse correctly
  public void testCanDeserializeWithoutDefaultValues() throws JsonProcessingException {
    // only updated
    UpdateNamespacePropertiesResponse onlyUpdated =
        UpdateNamespacePropertiesResponse.builder().addUpdated(UPDATED).build();
    String jsonOnlyUpdatedOthersNull =
        "{\"removed\":null,\"updated\":[\"owner\"],\"missing\":null}";
    assertEquals(deserialize(jsonOnlyUpdatedOthersNull), onlyUpdated);

    String jsonOnlyUpdatedOthersMissing = "{\"updated\":[\"owner\"]}";
    assertEquals(deserialize(jsonOnlyUpdatedOthersMissing), onlyUpdated);

    // Only removed
    UpdateNamespacePropertiesResponse onlyRemoved =
        UpdateNamespacePropertiesResponse.builder().addRemoved(REMOVED).build();
    String jsonOnlyRemovedOthersNull = "{\"removed\":[\"foo\"],\"updated\":null,\"missing\":null}";
    assertEquals(deserialize(jsonOnlyRemovedOthersNull), onlyRemoved);

    String jsonOnlyRemovedOthersMissing = "{\"removed\":[\"foo\"]}";
    assertEquals(deserialize(jsonOnlyRemovedOthersMissing), onlyRemoved);

    // Only missing
    UpdateNamespacePropertiesResponse onlyMissing =
        UpdateNamespacePropertiesResponse.builder().addMissing(MISSING).build();
    String jsonOnlyMissingFieldOthersNull =
        "{\"removed\":null,\"updated\":null,\"missing\":[\"bar\"]}";
    assertEquals(deserialize(jsonOnlyMissingFieldOthersNull), onlyMissing);

    String jsonOnlyMissingFieldIsPresent = "{\"missing\":[\"bar\"]}";
    assertEquals(deserialize(jsonOnlyMissingFieldIsPresent), onlyMissing);

    // all fields are missing
    UpdateNamespacePropertiesResponse noValues =
        UpdateNamespacePropertiesResponse.builder().build();
    String emptyJson = "{}";
    assertEquals(deserialize(emptyJson), noValues);
  }

  @Test
  public void testDeserializeInvalidResponse() {
    // Invalid top-level types
    String jsonInvalidTypeOnRemovedField =
        "{\"removed\":{\"foo\":true},\"updated\":[\"owner\"],\"missing\":[\"bar\"]}";
    AssertHelpers.assertThrows(
        "A JSON response with an invalid type for one of the fields should fail to parse",
        JsonProcessingException.class,
        () -> deserialize(jsonInvalidTypeOnRemovedField));

    String jsonInvalidTypeOnUpdatedField = "{\"updated\":\"owner\",\"missing\":[\"bar\"]}";
    AssertHelpers.assertThrows(
        "A JSON response with an invalid type for one of the fields should fail to parse",
        JsonProcessingException.class,
        () -> deserialize(jsonInvalidTypeOnUpdatedField));

    // Valid top-level (array) types, but at least one entry in the list is not the expected type
    String jsonInvalidValueOfTypeIntNestedInRemovedList =
        "{\"removed\":[\"foo\", \"bar\", 123456], ,\"updated\":[\"owner\"],\"missing\":[\"bar\"]}";
    AssertHelpers.assertThrows(
        "A JSON response with an invalid type inside one of the list fields should fail to deserialize",
        JsonProcessingException.class,
        () -> deserialize(jsonInvalidValueOfTypeIntNestedInRemovedList));

    // Exception comes from Jackson
    AssertHelpers.assertThrows(
        "A null JSON response body should fail to deserialize",
        IllegalArgumentException.class,
        () -> deserialize(null));
  }

  @Test
  public void testBuilderDoesNotCreateInvalidObjects() {
    List<String> listContainingNull = Lists.newArrayList("a", null, null);

    // updated
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a property that was updated",
        NullPointerException.class,
        "Invalid updated property: null",
        () -> UpdateNamespacePropertiesResponse.builder().addUpdated((String) null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null list of properties that were removed",
        NullPointerException.class,
        "Invalid updated property list: null",
        () -> UpdateNamespacePropertiesResponse.builder().addUpdated((List<String>) null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a list of properties that were removed with a null element",
        IllegalArgumentException.class,
        "Invalid updated property: null",
        () -> UpdateNamespacePropertiesResponse.builder().addUpdated(listContainingNull).build());

    // removed
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a property that was removed",
        NullPointerException.class,
        "Invalid removed property: null",
        () -> UpdateNamespacePropertiesResponse.builder().addRemoved((String) null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null list of properties that were removed",
        NullPointerException.class,
        "Invalid removed property list: null",
        () -> UpdateNamespacePropertiesResponse.builder().addRemoved((List<String>) null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a list of properties that were removed with a null element",
        IllegalArgumentException.class,
        "Invalid removed property: null",
        () -> UpdateNamespacePropertiesResponse.builder().addRemoved(listContainingNull).build());

    // missing
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a property that was missing",
        NullPointerException.class,
        "Invalid missing property: null",
        () -> UpdateNamespacePropertiesResponse.builder().addMissing((String) null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null list of properties that were missing",
        NullPointerException.class,
        "Invalid missing property list: null",
        () -> UpdateNamespacePropertiesResponse.builder().addMissing((List<String>) null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a list of properties that were missing with a null element",
        IllegalArgumentException.class,
        "Invalid missing property: null",
        () -> UpdateNamespacePropertiesResponse.builder().addMissing(listContainingNull).build());
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"updated", "removed", "missing"};
  }

  @Override
  public UpdateNamespacePropertiesResponse createExampleInstance() {
    return UpdateNamespacePropertiesResponse.builder()
        .addUpdated(UPDATED)
        .addMissing(MISSING)
        .addRemoved(REMOVED)
        .build();
  }

  @Override
  public void assertEquals(
      UpdateNamespacePropertiesResponse actual, UpdateNamespacePropertiesResponse expected) {
    Assert.assertEquals(
        "Properties updated should be equal",
        Sets.newHashSet(actual.updated()),
        Sets.newHashSet(expected.updated()));
    Assert.assertEquals(
        "Properties removed should be equal",
        Sets.newHashSet(actual.removed()),
        Sets.newHashSet(expected.removed()));
    Assert.assertEquals(
        "Properties missing should be equal",
        Sets.newHashSet(actual.missing()),
        Sets.newHashSet(expected.missing()));
  }

  @Override
  public UpdateNamespacePropertiesResponse deserialize(String json) throws JsonProcessingException {
    UpdateNamespacePropertiesResponse resp =
        mapper().readValue(json, UpdateNamespacePropertiesResponse.class);
    resp.validate();
    return resp;
  }
}
