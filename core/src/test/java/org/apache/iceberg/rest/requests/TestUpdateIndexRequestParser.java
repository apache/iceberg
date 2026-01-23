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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestUpdateIndexRequestParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> UpdateIndexRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid update index request: null");

    assertThatThrownBy(() -> UpdateIndexRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse update index request from null object");

    UpdateIndexRequest request = UpdateIndexRequestParser.fromJson("{}");
    assertThat(request.identifier()).isNull();
    assertThat(request.updates()).isEmpty();
    assertThat(request.requirements()).isEmpty();
  }

  @Test
  public void invalidIndexIdentifier() {
    // index identifier is optional
    String json =
        """
        {"requirements": [], "updates": []}"""
            .replaceAll("\\s+", "");
    UpdateIndexRequest request = UpdateIndexRequestParser.fromJson(json);
    assertThat(request.identifier()).isNull();

    String invalidIdentifier =
        """
        {"identifier": {}}"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> UpdateIndexRequestParser.fromJson(invalidIdentifier))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");

    String invalidNameType =
        """
        {"identifier": {"name": 23}}"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> UpdateIndexRequestParser.fromJson(invalidNameType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: name: 23");
  }

  @Test
  public void invalidRequirements() {
    String invalidRequirementType =
        """
        {
          "identifier": {"namespace": ["ns1"], "name": "index1"},
          "requirements": [23],
          "updates": []
        }"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> UpdateIndexRequestParser.fromJson(invalidRequirementType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse update requirement from non-object value: 23");

    String missingRequirementType =
        """
        {
          "identifier": {"namespace": ["ns1"], "name": "index1"},
          "requirements": [{}],
          "updates": []
        }"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> UpdateIndexRequestParser.fromJson(missingRequirementType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse update requirement. Missing field: type");

    String missingUuid =
        """
        {
          "identifier": {"namespace": ["ns1"], "name": "index1"},
          "requirements": [{"type": "assert-table-uuid"}],
          "updates": []
        }"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> UpdateIndexRequestParser.fromJson(missingUuid))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: uuid");
  }

  @Test
  public void invalidMetadataUpdates() {
    String invalidUpdateType =
        """
        {
          "identifier": {"namespace": ["ns1"], "name": "index1"},
          "requirements": [],
          "updates": [23]
        }"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> UpdateIndexRequestParser.fromJson(invalidUpdateType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse metadata update from non-object value: 23");

    String missingAction =
        """
        {
          "identifier": {"namespace": ["ns1"], "name": "index1"},
          "requirements": [],
          "updates": [{}]
        }"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> UpdateIndexRequestParser.fromJson(missingAction))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse metadata update. Missing field: action");

    String missingAssignUuid =
        """
        {
          "identifier": {"namespace": ["ns1"], "name": "index1"},
          "requirements": [],
          "updates": [{"action": "assign-uuid"}]
        }"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> UpdateIndexRequestParser.fromJson(missingAssignUuid))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: uuid");
  }

  @Test
  public void roundTripSerde() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    UpdateIndexRequest request =
        UpdateIndexRequest.create(
            TableIdentifier.of("ns1", "index1"),
            ImmutableList.of(
                new org.apache.iceberg.UpdateRequirement.AssertTableUUID(uuid),
                new org.apache.iceberg.UpdateRequirement.AssertTableDoesNotExist()),
            ImmutableList.of(
                new MetadataUpdate.AssignUUID(uuid), new MetadataUpdate.SetCurrentSchema(23)));

    String expectedJson =
        """
        {
          "identifier": {
            "namespace": ["ns1"],
            "name": "index1"
          },
          "requirements": [
            {
              "type": "assert-table-uuid",
              "uuid": "2cc52516-5e73-41f2-b139-545d41a4e151"
            },
            {
              "type": "assert-create"
            }
          ],
          "updates": [
            {
              "action": "assign-uuid",
              "uuid": "2cc52516-5e73-41f2-b139-545d41a4e151"
            },
            {
              "action": "set-current-schema",
              "schema-id": 23
            }
          ]
        }"""
            .replaceAll("\\s+", "");

    String json = UpdateIndexRequestParser.toJson(request);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison on UpdateIndexRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(UpdateIndexRequestParser.toJson(UpdateIndexRequestParser.fromJson(json)))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithoutIdentifier() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    UpdateIndexRequest request =
        new UpdateIndexRequest(
            ImmutableList.of(
                new org.apache.iceberg.UpdateRequirement.AssertTableUUID(uuid),
                new org.apache.iceberg.UpdateRequirement.AssertTableDoesNotExist()),
            ImmutableList.of(
                new MetadataUpdate.AssignUUID(uuid), new MetadataUpdate.SetCurrentSchema(23)));

    String expectedJson =
        """
        {
          "requirements": [
            {
              "type": "assert-table-uuid",
              "uuid": "2cc52516-5e73-41f2-b139-545d41a4e151"
            },
            {
              "type": "assert-create"
            }
          ],
          "updates": [
            {
              "action": "assign-uuid",
              "uuid": "2cc52516-5e73-41f2-b139-545d41a4e151"
            },
            {
              "action": "set-current-schema",
              "schema-id": 23
            }
          ]
        }"""
            .replaceAll("\\s+", "");

    String json = UpdateIndexRequestParser.toJson(request);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison on UpdateIndexRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(UpdateIndexRequestParser.toJson(UpdateIndexRequestParser.fromJson(json)))
        .isEqualTo(expectedJson);
  }

  @Test
  public void emptyRequirementsAndUpdates() {
    UpdateIndexRequest request =
        UpdateIndexRequest.create(
            TableIdentifier.of("ns1", "index1"), ImmutableList.of(), ImmutableList.of());

    String expectedJson =
        """
        {
          "identifier": {"namespace": ["ns1"], "name": "index1"},
          "requirements": [],
          "updates": []
        }"""
            .replaceAll("\\s+", "");

    assertThat(UpdateIndexRequestParser.toJson(request)).isEqualTo(expectedJson);
    // can't do an equality comparison on UpdateIndexRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(UpdateIndexRequestParser.toJson(UpdateIndexRequestParser.fromJson(expectedJson)))
        .isEqualTo(expectedJson);

    String minimalJson =
        """
        {"identifier": {"namespace": ["ns1"], "name": "index1"}}"""
            .replaceAll("\\s+", "");
    assertThat(UpdateIndexRequestParser.toJson(UpdateIndexRequestParser.fromJson(minimalJson)))
        .isEqualTo(expectedJson);
  }
}
