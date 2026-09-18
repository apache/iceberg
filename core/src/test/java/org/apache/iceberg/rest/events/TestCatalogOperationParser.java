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
package org.apache.iceberg.rest.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestCatalogOperationParser {
  private static final String UUID = "2cc52516-5e73-41f2-b139-545d41a4e151";
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of("ns1", "table1");

  @Test
  public void nullCheck() {
    assertThatThrownBy(() -> CatalogOperationParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid catalog operation: null");

    assertThatThrownBy(() -> CatalogOperationParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse catalog operation from null object");
  }

  @Test
  public void missingOperationType() {
    assertThatThrownBy(() -> CatalogOperationParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: operation-type");
  }

  @Test
  public void unknownOperationType() {
    CatalogOperation parsed =
        CatalogOperationParser.fromJson(
            "{\"operation-type\":\"x-mycatalog.archive-snapshot\",\"extra\":\"ignored\"}");
    assertThat(parsed).isInstanceOf(CatalogOperation.Unknown.class);
    assertThat(parsed.operationType()).isEqualTo("x-mycatalog.archive-snapshot");
    assertThat(CatalogOperationParser.toJson(parsed))
        .isEqualTo("{\"operation-type\":\"x-mycatalog.archive-snapshot\"}");
  }

  @Test
  public void roundTripStandardOperations() {
    List<CatalogOperation> operations =
        ImmutableList.of(
            createTable(),
            registerTable(),
            dropTable(),
            updateTable(),
            renameTable(),
            createView(),
            dropView(),
            updateView(),
            renameView(),
            createNamespace(),
            updateNamespaceProperties(),
            dropNamespace());

    for (CatalogOperation operation : operations) {
      String json = CatalogOperationParser.toJson(operation, true);
      CatalogOperation parsed = CatalogOperationParser.fromJson(json);
      assertThat(parsed.operationType()).isEqualTo(operation.operationType());
      assertThat(parsed).isInstanceOf(operation.getClass());
      assertThat(CatalogOperationParser.toJson(parsed, true)).isEqualTo(json);
    }
  }

  @Test
  public void createTableJson() {
    String expectedJson =
        "{\n"
            + "  \"operation-type\" : \"create-table\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ \"ns1\" ],\n"
            + "    \"name\" : \"table1\"\n"
            + "  },\n"
            + "  \"table-uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\",\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"2cc52516-5e73-41f2-b139-545d41a4e151\"\n"
            + "  } ]\n"
            + "}";

    assertThat(CatalogOperationParser.toJson(createTable(), true)).isEqualTo(expectedJson);
  }

  private static CatalogOperation createTable() {
    return ImmutableCatalogOperation.CreateTable.builder()
        .identifier(IDENTIFIER)
        .tableUuid(UUID)
        .addUpdates(new MetadataUpdate.AssignUUID(UUID))
        .build();
  }

  private static CatalogOperation registerTable() {
    return ImmutableCatalogOperation.RegisterTable.builder()
        .identifier(IDENTIFIER)
        .tableUuid(UUID)
        .build();
  }

  private static CatalogOperation dropTable() {
    return ImmutableCatalogOperation.DropTable.builder()
        .identifier(IDENTIFIER)
        .tableUuid(UUID)
        .purge(true)
        .build();
  }

  private static CatalogOperation updateTable() {
    return ImmutableCatalogOperation.UpdateTable.builder()
        .identifier(IDENTIFIER)
        .tableUuid(UUID)
        .addUpdates(new MetadataUpdate.AssignUUID(UUID))
        .addRequirements(new UpdateRequirement.AssertTableUUID(UUID))
        .build();
  }

  private static CatalogOperation renameTable() {
    return ImmutableCatalogOperation.RenameTable.builder()
        .source(IDENTIFIER)
        .destination(TableIdentifier.of("ns1", "renamed"))
        .tableUuid(UUID)
        .build();
  }

  private static CatalogOperation createView() {
    return ImmutableCatalogOperation.CreateView.builder()
        .identifier(TableIdentifier.of("ns1", "view1"))
        .viewUuid(UUID)
        .addUpdates(new MetadataUpdate.AssignUUID(UUID))
        .build();
  }

  private static CatalogOperation dropView() {
    return ImmutableCatalogOperation.DropView.builder()
        .identifier(TableIdentifier.of("ns1", "view1"))
        .viewUuid(UUID)
        .build();
  }

  private static CatalogOperation updateView() {
    return ImmutableCatalogOperation.UpdateView.builder()
        .identifier(TableIdentifier.of("ns1", "view1"))
        .viewUuid(UUID)
        .addUpdates(new MetadataUpdate.AssignUUID(UUID))
        .addRequirements(new UpdateRequirement.AssertViewUUID(UUID))
        .build();
  }

  private static CatalogOperation renameView() {
    return ImmutableCatalogOperation.RenameView.builder()
        .source(TableIdentifier.of("ns1", "view1"))
        .destination(TableIdentifier.of("ns1", "renamed-view"))
        .viewUuid(UUID)
        .build();
  }

  private static CatalogOperation createNamespace() {
    return ImmutableCatalogOperation.CreateNamespace.builder()
        .namespace(Namespace.of("ns1"))
        .properties(ImmutableMap.of("owner", "Hank"))
        .build();
  }

  private static CatalogOperation updateNamespaceProperties() {
    return ImmutableCatalogOperation.UpdateNamespaceProperties.builder()
        .namespace(Namespace.of("ns1"))
        .updated(ImmutableList.of("owner"))
        .removed(ImmutableList.of("foo"))
        .missing(ImmutableList.of("bar"))
        .build();
  }

  private static CatalogOperation dropNamespace() {
    return ImmutableCatalogOperation.DropNamespace.builder().namespace(Namespace.of("ns1")).build();
  }
}
