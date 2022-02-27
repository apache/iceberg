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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.junit.Assert;
import org.junit.Test;

public class TestDropNamespaceResponse extends RequestResponseTestBase<DropNamespaceResponse> {

  @Test
  public void testRoundTripSerDe() throws JsonProcessingException {
    assertRoundTripSerializesEquallyFrom(
        "{\"dropped\":true}", DropNamespaceResponse.builder().dropped(true).build());

    assertRoundTripSerializesEquallyFrom(
        "{\"dropped\":false}", DropNamespaceResponse.builder().dropped(false).build());
  }

  @Test
  public void testFailParsingWhenNullOrEmptyJson() {
    String nullJson = null;
    AssertHelpers.assertThrows("DropNamespaceResponse should fail to deserialize from a null JSON string",
        IllegalArgumentException.class, "argument \"content\" is null",
        () ->  deserialize(nullJson));

    String emptyString = "";
    AssertHelpers.assertThrows("DropNamespaceResponse should fail to deserialize empty string as JSON",
        JsonProcessingException.class, "No content to map due to end-of-input",
        () ->  deserialize(emptyString));

    String emptyJson = "{}";
    AssertHelpers.assertThrows(
        "DropNamespaceResponse should fail to deserialize and validate if missing the field dropped",
        IllegalArgumentException.class,
        "Invalid response, missing field: dropped",
        () -> deserialize(emptyJson));
  }

  @Test
  public void testFailWhenFieldsHaveInvalidValues() {
    String invalidJsonWithStringType = "{\"dropped\":\"421\"}";
    AssertHelpers.assertThrows("DropNamespaceResponse should fail to deserialize if the dropped field is invalid",
        JsonProcessingException.class,
        () -> deserialize(invalidJsonWithStringType));

    // Note that Jackson follows JSON rules to some degree, so integers wil parse into booleans.
    String invalidJsonWithFloatingPointType = "{\"dropped\":421.62}";
    AssertHelpers.assertThrows("DropNamespaceResponse should fail to deserialize if the dropped field is invalid",
        JsonProcessingException.class,
        () -> deserialize(invalidJsonWithFloatingPointType));

    String invalidJsonWithNullType = "{\"dropped\":null}";
    AssertHelpers.assertThrows("DropNamespaceResponse should fail to deserialize if the dropped field is invalid",
        IllegalArgumentException.class,
        "Invalid response, missing field: dropped",
        () -> deserialize(invalidJsonWithNullType));
  }

  @Test
  public void testBuilderDoesNotBuildInvalidResponse() {
    AssertHelpers.assertThrows(
        "The builder should not allow not setting the dropped field",
        IllegalArgumentException.class,
        "Invalid response, missing field: dropped",
        () -> DropNamespaceResponse.builder().build()
    );
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] { "dropped" };
  }

  @Override
  public DropNamespaceResponse createExampleInstance() {
    return DropNamespaceResponse.builder().dropped(true).build();
  }

  @Override
  public void assertEquals(DropNamespaceResponse actual, DropNamespaceResponse expected) {
    Assert.assertEquals(actual.isDropped(), expected.isDropped());
  }

  @Override
  public DropNamespaceResponse deserialize(String json) throws JsonProcessingException {
    return mapper().readValue(json, DropNamespaceResponse.class).validate();
  }
}
