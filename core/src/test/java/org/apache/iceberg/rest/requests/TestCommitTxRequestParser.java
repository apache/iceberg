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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.UpdateTableRequest.UpdateRequirement;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestCommitTxRequestParser {

  @Test
  public void nullAndEmptyCheck() {
    Assertions.assertThatThrownBy(() -> CommitTxRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid commit tx request: null");

    Assertions.assertThatThrownBy(() -> CommitTxRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse commit tx request from null object");
  }

  @Test
  public void missingFields() {}

  @Test
  public void invalidTableIdentifier() {}

  @Test
  public void invalidRequirements() {}

  @Test
  public void invalidMetadataUpdates() {}

  @Test
  public void roundTripSerde() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    CommitTxRequest.CommitTableRequest commitTableRequestOne =
        ImmutableCommitTableRequest.builder()
            .identifier(TableIdentifier.of("ns1", "table1"))
            .addRequirements(new UpdateRequirement.AssertTableUUID(uuid))
            .addRequirements(new UpdateRequirement.AssertTableDoesNotExist())
            .addUpdates(new MetadataUpdate.AssignUUID(uuid))
            .addUpdates(new MetadataUpdate.SetCurrentSchema(23))
            .build();

    CommitTxRequest.CommitTableRequest commitTableRequestTwo =
        ImmutableCommitTableRequest.builder()
            .identifier(TableIdentifier.of("ns1", "table2"))
            .addRequirements(new UpdateRequirement.AssertDefaultSpecID(4))
            .addRequirements(new UpdateRequirement.AssertCurrentSchemaID(24))
            .addUpdates(new MetadataUpdate.RemoveSnapshot(101L))
            .addUpdates(new MetadataUpdate.SetCurrentSchema(25))
            .build();

    CommitTxRequest commitTxRequest =
        ImmutableCommitTxRequest.builder()
            .addTableChanges(commitTableRequestOne, commitTableRequestTwo)
            .build();

    String expectedJson =
        "{\n"
            + "  \"ns1.table1\" : {\n"
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
            + "  },\n"
            + "  \"ns1.table2\" : {\n"
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
            + "  }\n"
            + "}";

    String json = CommitTxRequestParser.toJson(commitTxRequest, true);
    Assertions.assertThat(json).isEqualTo(expectedJson);

    // we can't do an equality comparison on CommitTxRequest because updates/requirements don't
    // implement equals/hashcode
    Assertions.assertThat(CommitTxRequestParser.toJson(CommitTxRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }
}
