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
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestCommitTransactionRequestParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> CommitTransactionRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid commit transaction request: null");

    assertThatThrownBy(() -> CommitTransactionRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse commit transaction request from null object");

    assertThatThrownBy(() -> CommitTransactionRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: table-changes");

    assertThatThrownBy(() -> CommitTransactionRequestParser.fromJson("{\"table-changes\":{}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse commit transaction request from non-array: {}");

    assertThatThrownBy(() -> CommitTransactionRequestParser.fromJson("{\"table-changes\":[]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table changes: empty");
  }

  @Test
  public void invalidTableIdentifier() {
    assertThatThrownBy(
            () ->
                CommitTransactionRequestParser.fromJson(
                    "{\"table-changes\":[{\"ns1.table1\" : \"ns1.table1\"}]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table changes: table identifier is required");

    assertThatThrownBy(
            () ->
                CommitTransactionRequestParser.fromJson(
                    "{\"table-changes\":[{\"identifier\" : {}}]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");

    assertThatThrownBy(
            () ->
                CommitTransactionRequestParser.fromJson(
                    "{\"table-changes\":[{\"identifier\" : { \"name\": 23}}]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: name: 23");
  }

  @Test
  public void roundTripSerde() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    UpdateTableRequest commitTableRequestOne =
        UpdateTableRequest.create(
            TableIdentifier.of("ns1", "table1"),
            ImmutableList.of(
                new UpdateRequirement.AssertTableUUID(uuid),
                new UpdateRequirement.AssertTableDoesNotExist()),
            ImmutableList.of(
                new MetadataUpdate.AssignUUID(uuid), new MetadataUpdate.SetCurrentSchema(23)));

    UpdateTableRequest commitTableRequestTwo =
        UpdateTableRequest.create(
            TableIdentifier.of("ns1", "table2"),
            ImmutableList.of(
                new UpdateRequirement.AssertDefaultSpecID(4),
                new UpdateRequirement.AssertCurrentSchemaID(24)),
            ImmutableList.of(
                new MetadataUpdate.RemoveSnapshot(101L), new MetadataUpdate.SetCurrentSchema(25)));

    CommitTransactionRequest request =
        new CommitTransactionRequest(
            ImmutableList.of(commitTableRequestOne, commitTableRequestTwo));

    String expectedJson =
        "{\n"
            + "  \"table-changes\" : [ {\n"
            + "    \"identifier\" : {\n"
            + "      \"namespace\" : [ \"ns1\" ],\n"
            + "      \"name\" : \"table1\"\n"
            + "    },\n"
            + "    \"requirements\" : [ {\n"
            + "      \"type\" : \"assert-table-uuid\",\n"
            + "      \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "    }, {\n"
            + "      \"type\" : \"assert-create\"\n"
            + "    } ],\n"
            + "    \"updates\" : [ {\n"
            + "      \"action\" : \"assign-uuid\",\n"
            + "      \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "    }, {\n"
            + "      \"action\" : \"set-current-schema\",\n"
            + "      \"schema-id\" : 23\n"
            + "    } ]\n"
            + "  }, {\n"
            + "    \"identifier\" : {\n"
            + "      \"namespace\" : [ \"ns1\" ],\n"
            + "      \"name\" : \"table2\"\n"
            + "    },\n"
            + "    \"requirements\" : [ {\n"
            + "      \"type\" : \"assert-default-spec-id\",\n"
            + "      \"default-spec-id\" : 4\n"
            + "    }, {\n"
            + "      \"type\" : \"assert-current-schema-id\",\n"
            + "      \"current-schema-id\" : 24\n"
            + "    } ],\n"
            + "    \"updates\" : [ {\n"
            + "      \"action\" : \"remove-snapshots\",\n"
            + "      \"snapshot-ids\" : [ 101 ]\n"
            + "    }, {\n"
            + "      \"action\" : \"set-current-schema\",\n"
            + "      \"schema-id\" : 25\n"
            + "    } ]\n"
            + "  } ]\n"
            + "}";

    String json = CommitTransactionRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison on CommitTransactionRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(
            CommitTransactionRequestParser.toJson(
                CommitTransactionRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void emptyRequirementsAndUpdates() {
    CommitTransactionRequest commitTxRequest =
        new CommitTransactionRequest(
            ImmutableList.of(
                UpdateTableRequest.create(
                    TableIdentifier.of("ns1", "table1"), ImmutableList.of(), ImmutableList.of())));

    String json =
        "{\"table-changes\":[{\"identifier\":{\"namespace\":[\"ns1\"],\"name\":\"table1\"},\"requirements\":[],\"updates\":[]}]}";

    assertThat(CommitTransactionRequestParser.toJson(commitTxRequest)).isEqualTo(json);
    // can't do an equality comparison on CommitTransactionRequest because updates/requirements
    // don't implement equals/hashcode
    assertThat(CommitTransactionRequestParser.toJson(CommitTransactionRequestParser.fromJson(json)))
        .isEqualTo(json);
  }
}
