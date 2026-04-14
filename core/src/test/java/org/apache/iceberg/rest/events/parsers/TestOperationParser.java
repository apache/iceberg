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
package org.apache.iceberg.rest.events.parsers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.events.CatalogOperationParser;
import org.apache.iceberg.rest.events.operations.CatalogOperation;
import org.apache.iceberg.rest.events.operations.OperationType;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

public class TestOperationParser {
  CatalogOperation.CreateNamespace createNamespaceOperation =
      new CatalogOperation.CreateNamespace(Namespace.of("a", "b"), ImmutableMap.of());
  String createNamespaceOperationJson =
      "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}";

  CatalogOperation.CreateTable createTableOperation =
      new CatalogOperation.CreateTable(
          TableIdentifier.of(Namespace.empty(), "table"), "uuid", Lists.newArrayList());
  String createTableOperationJson =
      "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[]}";

  CatalogOperation.CreateView createViewOperation =
      new CatalogOperation.CreateView(
          TableIdentifier.of(Namespace.empty(), "view"), "uuid", Lists.newArrayList());
  String createViewOperationJson =
      "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":[]}";

  CatalogOperation.DropNamespace dropNamespaceOperation =
      new CatalogOperation.DropNamespace(Namespace.of("a", "b"));
  String dropNamespaceOperationJson =
      "{\"operation-type\":\"drop-namespace\",\"namespace\":[\"a\",\"b\"]}";

  CatalogOperation.DropTable dropTableOperation =
      new CatalogOperation.DropTable(TableIdentifier.of(Namespace.empty(), "table"), "uuid", null);
  String dropTableOperationJson =
      "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}";

  CatalogOperation.DropView dropViewOperation =
      new CatalogOperation.DropView(TableIdentifier.of(Namespace.empty(), "view"), "uuid");
  String dropViewOperationJson =
      "{\"operation-type\":\"drop-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\"}";

  CatalogOperation.RegisterTable registerTableOperation =
      new CatalogOperation.RegisterTable(
          TableIdentifier.of(Namespace.empty(), "table"), "uuid", Lists.newArrayList());
  String registerTableOperationJson =
      "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}";

  CatalogOperation.RenameTable renameTableOperation =
      new CatalogOperation.RenameTable(
          TableIdentifier.of(Namespace.empty(), "fromTable"),
          TableIdentifier.of(Namespace.empty(), "toTable"),
          "uuid");
  String renameTableOperationJson =
      "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"fromTable\"},\"destination\":{\"namespace\":[],\"name\":\"toTable\"}}";

  CatalogOperation.RenameView renameViewOperation =
      new CatalogOperation.RenameView(
          TableIdentifier.of(Namespace.empty(), "fromView"),
          TableIdentifier.of(Namespace.empty(), "toView"),
          "uuid");
  String renameViewOperationJson =
      "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"fromView\"},\"destination\":{\"namespace\":[],\"name\":\"toView\"}}";

  CatalogOperation.UpdateNamespaceProperties updateNamespacePropertiesOperation =
      new CatalogOperation.UpdateNamespaceProperties(
          Namespace.of("a", "b"), Lists.newArrayList(), Lists.newArrayList(), null);
  String updateNamespacePropertiesOperationJson =
      "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\",\"b\"],\"updated\":[],\"removed\":[]}";

  CatalogOperation.UpdateTable updateTableOperation =
      new CatalogOperation.UpdateTable(
          TableIdentifier.of(Namespace.empty(), "table"), "uuid", Lists.newArrayList(), null);
  String updateTableOperationJson =
      "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[]}";

  CatalogOperation.UpdateView updateViewOperation =
      new CatalogOperation.UpdateView(
          TableIdentifier.of(Namespace.empty(), "view"), "uuid", Lists.newArrayList(), null);
  String updateViewOperationJson =
      "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":[]}";

  CatalogOperation.Custom customOperation =
      new CatalogOperation.Custom(
          new OperationType.CustomOperationType("x-op"), null, null, null, null, ImmutableMap.of());
  String customOperationJson = "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\"}";

  @Test
  void testToJson() {
    assertThat(
            JsonUtil.generate(
                gen -> CatalogOperationParser.toJson(createNamespaceOperation, gen), false))
        .isEqualTo(createNamespaceOperationJson);

    assertThat(
            JsonUtil.generate(
                gen -> CatalogOperationParser.toJson(createTableOperation, gen), false))
        .isEqualTo(createTableOperationJson);

    assertThat(
            JsonUtil.generate(
                gen -> CatalogOperationParser.toJson(createViewOperation, gen), false))
        .isEqualTo(createViewOperationJson);

    assertThat(
            JsonUtil.generate(
                gen -> CatalogOperationParser.toJson(dropNamespaceOperation, gen), false))
        .isEqualTo(dropNamespaceOperationJson);

    assertThat(
            JsonUtil.generate(gen -> CatalogOperationParser.toJson(dropTableOperation, gen), false))
        .isEqualTo(dropTableOperationJson);

    assertThat(
            JsonUtil.generate(gen -> CatalogOperationParser.toJson(dropViewOperation, gen), false))
        .isEqualTo(dropViewOperationJson);

    assertThat(
            JsonUtil.generate(
                gen -> CatalogOperationParser.toJson(registerTableOperation, gen), false))
        .isEqualTo(registerTableOperationJson);

    assertThat(
            JsonUtil.generate(
                gen -> CatalogOperationParser.toJson(renameTableOperation, gen), false))
        .isEqualTo(renameTableOperationJson);

    assertThat(
            JsonUtil.generate(
                gen -> CatalogOperationParser.toJson(renameViewOperation, gen), false))
        .isEqualTo(renameViewOperationJson);

    assertThat(
            JsonUtil.generate(
                gen -> CatalogOperationParser.toJson(updateNamespacePropertiesOperation, gen),
                false))
        .isEqualTo(updateNamespacePropertiesOperationJson);

    assertThat(
            JsonUtil.generate(
                gen -> CatalogOperationParser.toJson(updateTableOperation, gen), false))
        .isEqualTo(updateTableOperationJson);

    assertThat(
            JsonUtil.generate(
                gen -> CatalogOperationParser.toJson(updateViewOperation, gen), false))
        .isEqualTo(updateViewOperationJson);

    assertThat(JsonUtil.generate(gen -> CatalogOperationParser.toJson(customOperation, gen), false))
        .isEqualTo(customOperationJson);

    assertThatNullPointerException()
        .isThrownBy(() -> CatalogOperationParser.toJson(null, null))
        .withMessage("Invalid operation: null");
  }

  @Test
  void testFromJson() {
    assertThat(
            JsonUtil.parse(
                createNamespaceOperationJson,
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(createNamespaceOperation);

    assertThat(
            JsonUtil.parse(
                createTableOperationJson, (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(createTableOperation);

    assertThat(
            JsonUtil.parse(
                createViewOperationJson, (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(createViewOperation);

    assertThat(
            JsonUtil.parse(
                dropNamespaceOperationJson,
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(dropNamespaceOperation);

    assertThat(
            JsonUtil.parse(
                dropTableOperationJson, (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(dropTableOperation);

    assertThat(
            JsonUtil.parse(
                dropViewOperationJson, (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(dropViewOperation);

    assertThat(
            JsonUtil.parse(
                registerTableOperationJson,
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(registerTableOperation);

    assertThat(
            JsonUtil.parse(
                renameTableOperationJson, (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(renameTableOperation);

    assertThat(
            JsonUtil.parse(
                renameViewOperationJson, (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(renameViewOperation);

    assertThat(
            JsonUtil.parse(
                updateNamespacePropertiesOperationJson,
                (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(updateNamespacePropertiesOperation);

    assertThat(
            JsonUtil.parse(
                updateTableOperationJson, (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(updateTableOperation);

    assertThat(
            JsonUtil.parse(
                updateViewOperationJson, (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(updateViewOperation);

    assertThat(
            JsonUtil.parse(
                customOperationJson, (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .isEqualTo(customOperation);

    assertThatNullPointerException()
        .isThrownBy(() -> CatalogOperationParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse catalog operation from null object");

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> JsonUtil.parse("{}", (JsonNode node) -> CatalogOperationParser.fromJson(node)));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                JsonUtil.parse(
                    "{\"operation-type\":\"unknown\"}",
                    (JsonNode node) -> CatalogOperationParser.fromJson(node)))
        .withMessage("Invalid OperationType: unknown");
  }
}
