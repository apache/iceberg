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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

public class TestCatalogOperationParser {

  @Test
  void testToJsonWithNullOperation() {
    assertThatNullPointerException()
        .isThrownBy(() -> CatalogOperationParser.toJson(null))
        .withMessage("Invalid operation: null");
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> CatalogOperationParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse catalog operation from null object");
  }

  @Test
  void testFromJsonWithMissingOperationType() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> JsonUtil.parse("{}", (JsonNode node) -> CatalogOperationParser.fromJson(node)));
  }

  @Test
  void testFromJsonWithUnknownOperationType() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                JsonUtil.parse(
                    "{\"operation-type\":\"unknown\"}",
                    (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .withMessage("Invalid OperationType: unknown");
  }

  @Test
  void testAllOperationTypes() {
    CatalogOperation.CreateNamespace createNs =
        new CatalogOperation.CreateNamespace(Namespace.of("a", "b"), ImmutableMap.of());
    CatalogOperation.CreateTable createTable =
        new CatalogOperation.CreateTable(
            TableIdentifier.of(Namespace.empty(), "table"), "uuid", Lists.newArrayList());
    CatalogOperation.CreateView createView =
        new CatalogOperation.CreateView(
            TableIdentifier.of(Namespace.empty(), "view"), "uuid", Lists.newArrayList());
    CatalogOperation.DropNamespace dropNs =
        new CatalogOperation.DropNamespace(Namespace.of("a", "b"));
    CatalogOperation.DropTable dropTable =
        new CatalogOperation.DropTable(
            TableIdentifier.of(Namespace.empty(), "table"), "uuid", null);
    CatalogOperation.DropView dropView =
        new CatalogOperation.DropView(TableIdentifier.of(Namespace.empty(), "view"), "uuid");
    CatalogOperation.RegisterTable registerTable =
        new CatalogOperation.RegisterTable(
            TableIdentifier.of(Namespace.empty(), "table"), "uuid", Lists.newArrayList());
    CatalogOperation.RenameTable renameTable =
        new CatalogOperation.RenameTable(
            TableIdentifier.of(Namespace.empty(), "fromTable"),
            TableIdentifier.of(Namespace.empty(), "toTable"),
            "uuid");
    CatalogOperation.RenameView renameView =
        new CatalogOperation.RenameView(
            TableIdentifier.of(Namespace.empty(), "fromView"),
            TableIdentifier.of(Namespace.empty(), "toView"),
            "uuid");
    CatalogOperation.UpdateNamespaceProperties updateNsProps =
        new CatalogOperation.UpdateNamespaceProperties(
            Namespace.of("a", "b"), Lists.newArrayList(), Lists.newArrayList(), null);
    CatalogOperation.UpdateTable updateTable =
        new CatalogOperation.UpdateTable(
            TableIdentifier.of(Namespace.empty(), "table"), "uuid", Lists.newArrayList(), null);
    CatalogOperation.UpdateView updateView =
        new CatalogOperation.UpdateView(
            TableIdentifier.of(Namespace.empty(), "view"), "uuid", Lists.newArrayList(), null);
    CatalogOperation.Custom custom =
        new CatalogOperation.Custom(
            new OperationType.CustomOperationType("x-op"),
            null,
            null,
            null,
            null,
            ImmutableMap.of());

    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(createNs, gen), false))
        .isEqualTo("{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}");
    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(createTable, gen), false))
        .isEqualTo(
            "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[]}");
    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(createView, gen), false))
        .isEqualTo(
            "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":[]}");
    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(dropNs, gen), false))
        .isEqualTo("{\"operation-type\":\"drop-namespace\",\"namespace\":[\"a\",\"b\"]}");
    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(dropTable, gen), false))
        .isEqualTo(
            "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}");
    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(dropView, gen), false))
        .isEqualTo(
            "{\"operation-type\":\"drop-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\"}");
    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(registerTable, gen), false))
        .isEqualTo(
            "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}");
    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(renameTable, gen), false))
        .isEqualTo(
            "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"fromTable\"},\"destination\":{\"namespace\":[],\"name\":\"toTable\"}}");
    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(renameView, gen), false))
        .isEqualTo(
            "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"fromView\"},\"destination\":{\"namespace\":[],\"name\":\"toView\"}}");
    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(updateNsProps, gen), false))
        .isEqualTo(
            "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\",\"b\"],\"updated\":[],\"removed\":[]}");
    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(updateTable, gen), false))
        .isEqualTo(
            "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[]}");
    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(updateView, gen), false))
        .isEqualTo(
            "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":[]}");
    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(custom, gen), false))
        .isEqualTo("{\"operation-type\":\"custom\",\"custom-type\":\"x-op\"}");
  }

  @Test
  void testAllOperationTypesFromJson() {
    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(new CatalogOperation.CreateNamespace(Namespace.of("a", "b"), ImmutableMap.of()));

    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[]}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(
            new CatalogOperation.CreateTable(
                TableIdentifier.of(Namespace.empty(), "table"), "uuid", Lists.newArrayList()));

    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":[]}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(
            new CatalogOperation.CreateView(
                TableIdentifier.of(Namespace.empty(), "view"), "uuid", Lists.newArrayList()));

    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"drop-namespace\",\"namespace\":[\"a\",\"b\"]}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(new CatalogOperation.DropNamespace(Namespace.of("a", "b")));

    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(
            new CatalogOperation.DropTable(
                TableIdentifier.of(Namespace.empty(), "table"), "uuid", null));

    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"drop-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\"}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(
            new CatalogOperation.DropView(TableIdentifier.of(Namespace.empty(), "view"), "uuid"));

    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(
            new CatalogOperation.RegisterTable(
                TableIdentifier.of(Namespace.empty(), "table"), "uuid", Lists.newArrayList()));

    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"fromTable\"},\"destination\":{\"namespace\":[],\"name\":\"toTable\"}}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(
            new CatalogOperation.RenameTable(
                TableIdentifier.of(Namespace.empty(), "fromTable"),
                TableIdentifier.of(Namespace.empty(), "toTable"),
                "uuid"));

    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"fromView\"},\"destination\":{\"namespace\":[],\"name\":\"toView\"}}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(
            new CatalogOperation.RenameView(
                TableIdentifier.of(Namespace.empty(), "fromView"),
                TableIdentifier.of(Namespace.empty(), "toView"),
                "uuid"));

    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\",\"b\"],\"updated\":[],\"removed\":[]}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(
            new CatalogOperation.UpdateNamespaceProperties(
                Namespace.of("a", "b"), Lists.newArrayList(), Lists.newArrayList(), null));

    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[]}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(
            new CatalogOperation.UpdateTable(
                TableIdentifier.of(Namespace.empty(), "table"),
                "uuid",
                Lists.newArrayList(),
                null));

    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":[]}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(
            new CatalogOperation.UpdateView(
                TableIdentifier.of(Namespace.empty(), "view"), "uuid", Lists.newArrayList(), null));

    assertThat(
            JsonUtil.parse(
                "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\"}",
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(
            new CatalogOperation.Custom(
                new OperationType.CustomOperationType("x-op"),
                null,
                null,
                null,
                null,
                ImmutableMap.of()));
  }

  // CreateNamespace

  @Test
  void createNamespaceToJsonPretty() {
    CatalogOperation.CreateNamespace op =
        new CatalogOperation.CreateNamespace(Namespace.of("a", "b"), Map.of());
    String expected =
        "{\n"
            + "  \"operation-type\" : \"create-namespace\",\n"
            + "  \"namespace\" : [ \"a\", \"b\" ]\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void createNamespaceWithProperties() {
    CatalogOperation.CreateNamespace op =
        new CatalogOperation.CreateNamespace(Namespace.of("a", "b"), Map.of("key", "value"));
    String expected =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"],\"properties\":{\"key\":\"value\"}}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(expected);
    assertThat(CatalogOperationParser.fromJson(expected)).isEqualTo(op);
  }

  @Test
  void createNamespaceMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> CatalogOperationParser.fromJson("{\"operation-type\":\"create-namespace\"}"));
  }

  @Test
  void createNamespaceInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-namespace\",\"namespace\":\"a\"}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\"],\"properties\":\"not-an-object\"}"));
  }

  @Test
  void createNamespaceRoundTrip() {
    String json = "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);

    String jsonWithProps =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"],\"properties\":{\"key\":\"value\"}}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(jsonWithProps)))
        .isEqualTo(jsonWithProps);
  }

  // CreateTable

  @Test
  void createTableToJsonPretty() {
    CatalogOperation.CreateTable op =
        new CatalogOperation.CreateTable(
            TableIdentifier.of(Namespace.empty(), "table"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")));
    String expected =
        "{\n"
            + "  \"operation-type\" : \"create-table\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"table\"\n"
            + "  },\n"
            + "  \"table-uuid\" : \"uuid\",\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"uuid\"\n"
            + "  } ]\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void createTableFromJsonWithMetadataUpdates() {
    String json =
        "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    CatalogOperation.CreateTable parsed =
        (CatalogOperation.CreateTable) CatalogOperationParser.fromJson(json);
    assertThat(parsed.identifier()).isEqualTo(TableIdentifier.of(Namespace.empty(), "table"));
    assertThat(parsed.tableUuid()).isEqualTo("uuid");
    assertThat(parsed.updates()).hasSize(1);
    assertThat(((MetadataUpdate.AssignUUID) parsed.updates().get(0)).uuid()).isEqualTo("uuid");
  }

  @Test
  void createTableMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-table\",\"table-uuid\":\"uuid\",\"updates\":[]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"updates\":[]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}"));
  }

  @Test
  void createTableInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-table\",\"identifier\":\"not-an-object\",\"table-uuid\":\"uuid\",\"updates\":[]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":123,\"updates\":[]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":\"not-an-array\"}"));
  }

  @Test
  void createTableRoundTrip() {
    String json =
        "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);
  }

  // CreateView

  @Test
  void createViewToJsonPretty() {
    CatalogOperation.CreateView op =
        new CatalogOperation.CreateView(
            TableIdentifier.of(Namespace.empty(), "view"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")));
    String expected =
        "{\n"
            + "  \"operation-type\" : \"create-view\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"view\"\n"
            + "  },\n"
            + "  \"view-uuid\" : \"uuid\",\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"uuid\"\n"
            + "  } ]\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void createViewFromJsonWithMetadataUpdates() {
    String json =
        "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    CatalogOperation.CreateView parsed =
        (CatalogOperation.CreateView) CatalogOperationParser.fromJson(json);
    assertThat(parsed.identifier()).isEqualTo(TableIdentifier.of(Namespace.empty(), "view"));
    assertThat(parsed.viewUuid()).isEqualTo("uuid");
    assertThat(parsed.updates()).hasSize(1);
    assertThat(((MetadataUpdate.AssignUUID) parsed.updates().get(0)).uuid()).isEqualTo("uuid");
  }

  @Test
  void createViewMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-view\",\"view-uuid\":\"uuid\"}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"}}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\"}"));
  }

  @Test
  void createViewInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-view\",\"identifier\":\"not-an-object\",\"view-uuid\":\"uuid\"}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":123}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":\"not-an-array\"}"));
  }

  @Test
  void createViewRoundTrip() {
    String json =
        "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);
  }

  // DropNamespace

  @Test
  void dropNamespaceToJsonPretty() {
    CatalogOperation.DropNamespace op = new CatalogOperation.DropNamespace(Namespace.of("a", "b"));
    String expected =
        "{\n"
            + "  \"operation-type\" : \"drop-namespace\",\n"
            + "  \"namespace\" : [ \"a\", \"b\" ]\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void dropNamespaceMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> CatalogOperationParser.fromJson("{\"operation-type\":\"drop-namespace\"}"));
  }

  @Test
  void dropNamespaceInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"drop-namespace\",\"namespace\":\"a\"}"));
  }

  @Test
  void dropNamespaceRoundTrip() {
    String json = "{\"operation-type\":\"drop-namespace\",\"namespace\":[\"a\",\"b\"]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);
  }

  // DropTable

  @Test
  void dropTableToJsonPretty() {
    CatalogOperation.DropTable op =
        new CatalogOperation.DropTable(TableIdentifier.of(Namespace.of("a"), "t"), "uuid", null);
    String expected =
        "{\n"
            + "  \"operation-type\" : \"drop-table\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ \"a\" ],\n"
            + "    \"name\" : \"t\"\n"
            + "  },\n"
            + "  \"table-uuid\" : \"uuid\"\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void dropTableWithPurge() {
    CatalogOperation.DropTable op =
        new CatalogOperation.DropTable(TableIdentifier.of(Namespace.of("a"), "t"), "uuid", true);
    String expected =
        "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"purge\":true}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void dropTableMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"drop-table\",\"table-uuid\":\"uuid\"}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"}}"));
  }

  @Test
  void dropTableInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"drop-table\",\"identifier\":\"not-obj\",\"table-uuid\":\"uuid\"}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"table-uuid\":123}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"purge\":\"yes\"}"));
  }

  @Test
  void dropTableRoundTrip() {
    String json =
        "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"table-uuid\":\"uuid\"}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);

    String jsonWithPurge =
        "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"purge\":true}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(jsonWithPurge)))
        .isEqualTo(jsonWithPurge);
  }

  // DropView

  @Test
  void dropViewToJsonPretty() {
    CatalogOperation.DropView op =
        new CatalogOperation.DropView(TableIdentifier.of(Namespace.empty(), "v"), "uuid");
    String expected =
        "{\n"
            + "  \"operation-type\" : \"drop-view\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"v\"\n"
            + "  },\n"
            + "  \"view-uuid\" : \"uuid\"\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void dropViewMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"drop-view\",\"view-uuid\":\"uuid\"}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"drop-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"}}"));
  }

  @Test
  void dropViewInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"drop-view\",\"identifier\":\"not-obj\",\"view-uuid\":\"uuid\"}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"drop-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":123}"));
  }

  @Test
  void dropViewRoundTrip() {
    String json =
        "{\"operation-type\":\"drop-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\"}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);
  }

  // RegisterTable

  @Test
  void registerTableToJsonPretty() {
    CatalogOperation.RegisterTable op =
        new CatalogOperation.RegisterTable(
            TableIdentifier.of(Namespace.empty(), "table"), "uuid", List.of());
    String expected =
        "{\n"
            + "  \"operation-type\" : \"register-table\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"table\"\n"
            + "  },\n"
            + "  \"table-uuid\" : \"uuid\"\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void registerTableWithUpdates() {
    CatalogOperation.RegisterTable op =
        new CatalogOperation.RegisterTable(
            TableIdentifier.of(Namespace.empty(), "table"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")));
    String expected =
        "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(expected);

    CatalogOperation.RegisterTable parsed =
        (CatalogOperation.RegisterTable) CatalogOperationParser.fromJson(expected);
    assertThat(parsed.identifier()).isEqualTo(op.identifier());
    assertThat(parsed.tableUuid()).isEqualTo(op.tableUuid());
    assertThat(parsed.updates()).hasSize(1);
    assertThat(((MetadataUpdate.AssignUUID) parsed.updates().get(0)).uuid())
        .isEqualTo(((MetadataUpdate.AssignUUID) op.updates().get(0)).uuid());
  }

  @Test
  void registerTableMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"register-table\",\"table-uuid\":\"uuid\"}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"}}"));
  }

  @Test
  void registerTableInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"register-table\",\"identifier\":\"not-an-object\",\"table-uuid\":\"uuid\"}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":123}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":\"not-an-array\"}"));
  }

  @Test
  void registerTableRoundTrip() {
    String json =
        "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);

    String jsonWithUpdates =
        "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(jsonWithUpdates)))
        .isEqualTo(jsonWithUpdates);
  }

  // RenameTable

  @Test
  void renameTableToJsonPretty() {
    CatalogOperation.RenameTable op =
        new CatalogOperation.RenameTable(
            TableIdentifier.of(Namespace.empty(), "s"),
            TableIdentifier.of(Namespace.of("a"), "d"),
            "uuid");
    String expected =
        "{\n"
            + "  \"operation-type\" : \"rename-table\",\n"
            + "  \"table-uuid\" : \"uuid\",\n"
            + "  \"source\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"s\"\n"
            + "  },\n"
            + "  \"destination\" : {\n"
            + "    \"namespace\" : [ \"a\" ],\n"
            + "    \"name\" : \"d\"\n"
            + "  }\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void renameTableMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"rename-table\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"}}"));
  }

  @Test
  void renameTableInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"rename-table\",\"table-uuid\":123,\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":\"not-obj\",\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":123}"));
  }

  @Test
  void renameTableRoundTrip() {
    String json =
        "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);
  }

  // RenameView

  @Test
  void renameViewToJsonPretty() {
    CatalogOperation.RenameView op =
        new CatalogOperation.RenameView(
            TableIdentifier.of(Namespace.empty(), "s"),
            TableIdentifier.of(Namespace.of("a"), "d"),
            "uuid");
    String expected =
        "{\n"
            + "  \"operation-type\" : \"rename-view\",\n"
            + "  \"view-uuid\" : \"uuid\",\n"
            + "  \"source\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"s\"\n"
            + "  },\n"
            + "  \"destination\" : {\n"
            + "    \"namespace\" : [ \"a\" ],\n"
            + "    \"name\" : \"d\"\n"
            + "  }\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void renameViewMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"rename-view\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"}}"));
  }

  @Test
  void renameViewInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"rename-view\",\"view-uuid\":123,\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":\"not-obj\",\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":123}"));
  }

  @Test
  void renameViewRoundTrip() {
    String json =
        "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);
  }

  // UpdateNamespaceProperties

  @Test
  void updateNamespacePropertiesToJsonPretty() {
    CatalogOperation.UpdateNamespaceProperties op =
        new CatalogOperation.UpdateNamespaceProperties(
            Namespace.of("a"), List.of("k2"), List.of("k1"), null);
    String expected =
        "{\n"
            + "  \"operation-type\" : \"update-namespace-properties\",\n"
            + "  \"namespace\" : [ \"a\" ],\n"
            + "  \"updated\" : [ \"k1\" ],\n"
            + "  \"removed\" : [ \"k2\" ]\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void updateNamespacePropertiesWithMissing() {
    CatalogOperation.UpdateNamespaceProperties op =
        new CatalogOperation.UpdateNamespaceProperties(
            Namespace.of("a"), List.of("k2"), List.of("k1"), List.of("k3"));
    String expected =
        "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":[\"k1\"],\"removed\":[\"k2\"],\"missing\":[\"k3\"]}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void updateNamespacePropertiesMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-namespace-properties\",\"updated\":[\"k1\"],\"removed\":[\"k2\"]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"removed\":[\"k2\"]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":[\"k1\"]}"));
  }

  @Test
  void updateNamespacePropertiesInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-namespace-properties\",\"namespace\":\"a\",\"updated\":[\"k1\"],\"removed\":[\"k2\"]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":\"not-array\",\"removed\":[\"k2\"]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":[\"k1\"],\"removed\":123}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":[\"k1\"],\"removed\":[\"k2\"],\"missing\":123}"));
  }

  @Test
  void updateNamespacePropertiesRoundTrip() {
    String json =
        "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":[\"k1\"],\"removed\":[\"k2\"]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);

    String jsonWithMissing =
        "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":[\"k1\"],\"removed\":[\"k2\"],\"missing\":[\"k3\"]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(jsonWithMissing)))
        .isEqualTo(jsonWithMissing);
  }

  // UpdateTable

  @Test
  void updateTableToJsonPretty() {
    CatalogOperation.UpdateTable op =
        new CatalogOperation.UpdateTable(
            TableIdentifier.of(Namespace.empty(), "t"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")),
            null);
    String expected =
        "{\n"
            + "  \"operation-type\" : \"update-table\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"t\"\n"
            + "  },\n"
            + "  \"table-uuid\" : \"uuid\",\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"uuid\"\n"
            + "  } ]\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void updateTableWithRequirements() {
    CatalogOperation.UpdateTable op =
        new CatalogOperation.UpdateTable(
            TableIdentifier.of(Namespace.empty(), "t"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")),
            List.of(new UpdateRequirement.AssertRefSnapshotID("main", 5L)));
    String expected =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}],\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":5}]}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(expected);

    CatalogOperation.UpdateTable parsed =
        (CatalogOperation.UpdateTable) CatalogOperationParser.fromJson(expected);
    assertThat(parsed.identifier()).isEqualTo(op.identifier());
    assertThat(parsed.tableUuid()).isEqualTo(op.tableUuid());
    assertThat(parsed.updates()).hasSize(1);
    assertThat(parsed.requirements()).hasSize(1);
    assertThat(((UpdateRequirement.AssertRefSnapshotID) parsed.requirements().get(0)).refName())
        .isEqualTo("main");
    assertThat(((UpdateRequirement.AssertRefSnapshotID) parsed.requirements().get(0)).snapshotId())
        .isEqualTo(5L);
  }

  @Test
  void updateTableMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-table\",\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\"}"));
  }

  @Test
  void updateTableInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-table\",\"identifier\":\"not-obj\",\"table-uuid\":\"uuid\",\"updates\":[]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":123,\"updates\":[]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":\"not-array\"}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[],\"requirements\":{}}"));
  }

  @Test
  void updateTableRoundTrip() {
    String json =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);

    String jsonWithRequirements =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}],\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":5}]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(jsonWithRequirements)))
        .isEqualTo(jsonWithRequirements);
  }

  // UpdateView

  @Test
  void updateViewToJsonPretty() {
    CatalogOperation.UpdateView op =
        new CatalogOperation.UpdateView(
            TableIdentifier.of(Namespace.empty(), "v"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")),
            null);
    String expected =
        "{\n"
            + "  \"operation-type\" : \"update-view\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"v\"\n"
            + "  },\n"
            + "  \"view-uuid\" : \"uuid\",\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"uuid\"\n"
            + "  } ]\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void updateViewWithRequirements() {
    CatalogOperation.UpdateView op =
        new CatalogOperation.UpdateView(
            TableIdentifier.of(Namespace.empty(), "v"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")),
            List.of(new UpdateRequirement.AssertRefSnapshotID("main", 5L)));
    String expected =
        "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}],\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":5}]}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(expected);

    CatalogOperation.UpdateView parsed =
        (CatalogOperation.UpdateView) CatalogOperationParser.fromJson(expected);
    assertThat(parsed.identifier()).isEqualTo(op.identifier());
    assertThat(parsed.viewUuid()).isEqualTo(op.viewUuid());
    assertThat(parsed.updates()).hasSize(1);
    assertThat(parsed.requirements()).hasSize(1);
    assertThat(((UpdateRequirement.AssertRefSnapshotID) parsed.requirements().get(0)).refName())
        .isEqualTo("main");
    assertThat(((UpdateRequirement.AssertRefSnapshotID) parsed.requirements().get(0)).snapshotId())
        .isEqualTo(5L);
  }

  @Test
  void updateViewMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-view\",\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\"}"));
  }

  @Test
  void updateViewInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-view\",\"identifier\":\"not-obj\",\"view-uuid\":\"uuid\",\"updates\":[]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":123,\"updates\":[]}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\",\"updates\":\"not-array\"}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\",\"updates\":[],\"requirements\":{}}"));
  }

  @Test
  void updateViewRoundTrip() {
    String json =
        "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);

    String jsonWithRequirements =
        "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}],\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":5}]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(jsonWithRequirements)))
        .isEqualTo(jsonWithRequirements);
  }

  // Custom

  @Test
  void customToJsonPretty() {
    CatalogOperation.Custom op =
        new CatalogOperation.Custom(
            new OperationType.CustomOperationType("x-op"),
            null,
            null,
            null,
            null,
            ImmutableMap.of());
    String expected =
        "{\n" + "  \"operation-type\" : \"custom\",\n" + "  \"custom-type\" : \"x-op\"\n" + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void customWithAllOptionalFields() {
    CatalogOperation.Custom op =
        new CatalogOperation.Custom(
            new OperationType.CustomOperationType("x-op"),
            TableIdentifier.of(Namespace.of("a"), "t"),
            Namespace.of("a", "b"),
            "t-uuid",
            "v-uuid",
            ImmutableMap.of());
    String expected =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"namespace\":[\"a\",\"b\"],\"table-uuid\":\"t-uuid\",\"view-uuid\":\"v-uuid\"}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(expected);
    assertThat(CatalogOperationParser.fromJson(expected)).isEqualTo(op);
  }

  @Test
  void customMissingProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson("{\"operation-type\":\"custom\"}"));
  }

  @Test
  void customInvalidProperties() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"custom\",\"custom-type\":123}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"identifier\":{}}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"namespace\":\"a\"}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"table-uuid\":123}"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                CatalogOperationParser.fromJson(
                    "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"view-uuid\":true}"));
  }

  @Test
  void customRoundTrip() {
    String json = "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\"}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);

    String jsonWithAll =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"namespace\":[\"a\",\"b\"],\"table-uuid\":\"t-uuid\",\"view-uuid\":\"v-uuid\"}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(jsonWithAll)))
        .isEqualTo(jsonWithAll);
  }
}
