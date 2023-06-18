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

public class TestUpdateTableRequestParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> UpdateTableRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid update table request: null");

    assertThatThrownBy(() -> UpdateTableRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse update table request from null object");

    UpdateTableRequest request = UpdateTableRequestParser.fromJson("{}");
    assertThat(request.identifier()).isNull();
    assertThat(request.updates()).isEmpty();
    assertThat(request.requirements()).isEmpty();
  }

  @Test
  public void invalidTableIdentifier() {
    // table identifier is optional
    UpdateTableRequest request =
        UpdateTableRequestParser.fromJson("{\"requirements\" : [], \"updates\" : []}");
    assertThat(request.identifier()).isNull();

    assertThatThrownBy(() -> UpdateTableRequestParser.fromJson("{\"identifier\" : {}}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");

    assertThatThrownBy(
            () -> UpdateTableRequestParser.fromJson("{\"identifier\" : { \"name\": 23}}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: name: 23");
  }

  @Test
  public void invalidRequirements() {
    assertThatThrownBy(
            () ->
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},"
                        + "\"requirements\":[23],\"updates\":[]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse update requirement from non-object value: 23");

    assertThatThrownBy(
            () ->
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},"
                        + "\"requirements\":[{}],\"updates\":[]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse update requirement. Missing field: type");

    assertThatThrownBy(
            () ->
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},"
                        + "\"requirements\":[{\"type\":\"assert-table-uuid\"}],\"updates\":[]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: uuid");
  }

  @Test
  public void invalidMetadataUpdates() {
    assertThatThrownBy(
            () ->
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},"
                        + "\"requirements\":[],\"updates\":[23]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse metadata update from non-object value: 23");

    assertThatThrownBy(
            () ->
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},"
                        + "\"requirements\":[],\"updates\":[{}]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse metadata update. Missing field: action");

    assertThatThrownBy(
            () ->
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},"
                        + "\"requirements\":[],\"updates\":[{\"action\":\"assign-uuid\"}]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: uuid");
  }

  @Test
  public void roundTripSerde() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    UpdateTableRequest request =
        new UpdateTableRequest(
            TableIdentifier.of("ns1", "table1"),
            ImmutableList.of(
                new org.apache.iceberg.UpdateRequirement.AssertTableUUID(uuid),
                new org.apache.iceberg.UpdateRequirement.AssertTableDoesNotExist()),
            ImmutableList.of(
                new MetadataUpdate.AssignUUID(uuid), new MetadataUpdate.SetCurrentSchema(23)));

    String expectedJson =
        "{\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ \"ns1\" ],\n"
            + "    \"name\" : \"table1\"\n"
            + "  },\n"
            + "  \"requirements\" : [ {\n"
            + "    \"type\" : \"assert-table-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  }, {\n"
            + "    \"type\" : \"assert-create\"\n"
            + "  } ],\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  }, {\n"
            + "    \"action\" : \"set-current-schema\",\n"
            + "    \"schema-id\" : 23\n"
            + "  } ]\n"
            + "}";

    String json = UpdateTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison on UpdateTableRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(UpdateTableRequestParser.toJson(UpdateTableRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithoutIdentifier() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(
                new org.apache.iceberg.UpdateRequirement.AssertTableUUID(uuid),
                new org.apache.iceberg.UpdateRequirement.AssertTableDoesNotExist()),
            ImmutableList.of(
                new MetadataUpdate.AssignUUID(uuid), new MetadataUpdate.SetCurrentSchema(23)));

    String expectedJson =
        "{\n"
            + "  \"requirements\" : [ {\n"
            + "    \"type\" : \"assert-table-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  }, {\n"
            + "    \"type\" : \"assert-create\"\n"
            + "  } ],\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  }, {\n"
            + "    \"action\" : \"set-current-schema\",\n"
            + "    \"schema-id\" : 23\n"
            + "  } ]\n"
            + "}";

    String json = UpdateTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison on UpdateTableRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(UpdateTableRequestParser.toJson(UpdateTableRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void emptyRequirementsAndUpdates() {
    UpdateTableRequest request =
        new UpdateTableRequest(
            TableIdentifier.of("ns1", "table1"), ImmutableList.of(), ImmutableList.of());

    String json =
        "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},\"requirements\":[],\"updates\":[]}";

    assertThat(UpdateTableRequestParser.toJson(request)).isEqualTo(json);
    // can't do an equality comparison on UpdateTableRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(UpdateTableRequestParser.toJson(UpdateTableRequestParser.fromJson(json)))
        .isEqualTo(json);

    assertThat(UpdateTableRequestParser.toJson(request)).isEqualTo(json);
    // can't do an equality comparison on UpdateTableRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(
            UpdateTableRequestParser.toJson(
                UpdateTableRequestParser.fromJson(
                    "{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"}}")))
        .isEqualTo(json);
  }
}
