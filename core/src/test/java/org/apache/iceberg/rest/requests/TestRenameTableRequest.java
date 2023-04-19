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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.junit.Assert;
import org.junit.Test;

public class TestRenameTableRequest extends RequestResponseTestBase<RenameTableRequest> {

  /* Values used to fill in request fields */
  private static final Namespace NAMESPACE = Namespace.of("accounting", "tax");
  private static final TableIdentifier TAX_PAID = TableIdentifier.of(NAMESPACE, "paid");
  private static final TableIdentifier TAX_PAID_RENAMED =
      TableIdentifier.of(NAMESPACE, "paid_2022");

  @Test
  public void testRoundTripSerDe() throws JsonProcessingException {
    String sourceJson = TableIdentifierParser.toJson(TAX_PAID);
    String destinationJson = TableIdentifierParser.toJson(TAX_PAID_RENAMED);
    String fullJson =
        String.format("{\"source\":%s,\"destination\":%s}", sourceJson, destinationJson);
    RenameTableRequest req =
        RenameTableRequest.builder().withSource(TAX_PAID).withDestination(TAX_PAID_RENAMED).build();

    assertRoundTripSerializesEquallyFrom(fullJson, req);
  }

  @Test
  public void testDeserializeInvalidRequestThrows() {
    String jsonSourceNullName =
        "{\"source\":{\"namespace\":[\"accounting\",\"tax\"],\"name\":null},"
            + "\"destination\":{\"namespace\":[\"accounting\",\"tax\"],\"name\":\"paid_2022\"}}";
    AssertHelpers.assertThrows(
        "A JSON request with an invalid source table identifier, with null for the name, should fail to deserialize",
        JsonProcessingException.class,
        "Cannot parse to a string value: name: null",
        () -> deserialize(jsonSourceNullName));

    String jsonDestinationNullName =
        "{\"source\":{\"namespace\":[\"accounting\",\"tax\"],\"name\":\"paid\"},"
            + "\"destination\":{\"namespace\":[\"accounting\",\"tax\"],\"name\":null}}";
    AssertHelpers.assertThrows(
        "A JSON request with an invalid destination table, with null for the name, should fail to deserialize",
        JsonProcessingException.class,
        "Cannot parse to a string value: name: null",
        () -> deserialize(jsonDestinationNullName));

    String jsonSourceMissingName =
        "{\"source\":{\"namespace\":[\"accounting\",\"tax\"]},"
            + "\"destination\":{\"name\":\"paid_2022\"}}";
    AssertHelpers.assertThrows(
        "A JSON request with an invalid source table identifier, with no name, should fail to deserialize",
        JsonProcessingException.class,
        "Cannot parse missing string: name",
        () -> deserialize(jsonSourceMissingName));

    String jsonDestinationMissingName =
        "{\"source\":{\"namespace\":[\"accounting\",\"tax\"],\"name\":\"paid\"},"
            + "\"destination\":{\"namespace\":[\"accounting\",\"tax\"]}}";
    AssertHelpers.assertThrows(
        "A JSON request with an invalid destination table identifier, with no name, should fail to deserialize",
        JsonProcessingException.class,
        "Cannot parse missing string: name",
        () -> deserialize(jsonDestinationMissingName));

    String emptyJson = "{}";
    AssertHelpers.assertThrows(
        "An empty JSON object should not parse into a valid RenameTableRequest instance",
        IllegalArgumentException.class,
        "Invalid source table: null",
        () -> deserialize(emptyJson));

    AssertHelpers.assertThrows(
        "An empty JSON request should fail to deserialize",
        IllegalArgumentException.class,
        () -> deserialize(null));
  }

  @Test
  public void testBuilderDoesNotBuildInvalidRequests() {
    AssertHelpers.assertThrows(
        "The builder should not allow using null for the source table",
        NullPointerException.class,
        "Invalid source table identifier: null",
        () ->
            RenameTableRequest.builder()
                .withSource(null)
                .withDestination(TAX_PAID_RENAMED)
                .build());

    AssertHelpers.assertThrows(
        "The builder should not allow using null for the destination table",
        NullPointerException.class,
        "Invalid destination table identifier: null",
        () -> RenameTableRequest.builder().withSource(TAX_PAID).withDestination(null).build());
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"source", "destination"};
  }

  @Override
  public RenameTableRequest createExampleInstance() {
    return RenameTableRequest.builder()
        .withSource(TAX_PAID)
        .withDestination(TAX_PAID_RENAMED)
        .build();
  }

  @Override
  public void assertEquals(RenameTableRequest actual, RenameTableRequest expected) {
    Assert.assertEquals(
        "Source table identifier should be equal", expected.source(), actual.source());
    Assert.assertEquals(
        "Destination table identifier should be equal",
        expected.destination(),
        actual.destination());
  }

  @Override
  public RenameTableRequest deserialize(String json) throws JsonProcessingException {
    RenameTableRequest request = mapper().readValue(json, RenameTableRequest.class);
    request.validate();
    return request;
  }
}
