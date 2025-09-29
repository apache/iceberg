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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.events.operations.CreateNamespaceOperation;
import org.apache.iceberg.rest.events.operations.CreateTableOperation;
import org.apache.iceberg.rest.events.operations.CreateViewOperation;
import org.apache.iceberg.rest.events.operations.CustomOperation;
import org.apache.iceberg.rest.events.operations.DropNamespaceOperation;
import org.apache.iceberg.rest.events.operations.DropTableOperation;
import org.apache.iceberg.rest.events.operations.DropViewOperation;
import org.apache.iceberg.rest.events.operations.ImmutableCreateNamespaceOperation;
import org.apache.iceberg.rest.events.operations.ImmutableCreateTableOperation;
import org.apache.iceberg.rest.events.operations.ImmutableCreateViewOperation;
import org.apache.iceberg.rest.events.operations.ImmutableCustomOperation;
import org.apache.iceberg.rest.events.operations.ImmutableDropNamespaceOperation;
import org.apache.iceberg.rest.events.operations.ImmutableDropTableOperation;
import org.apache.iceberg.rest.events.operations.ImmutableDropViewOperation;
import org.apache.iceberg.rest.events.operations.ImmutableRegisterTableOperation;
import org.apache.iceberg.rest.events.operations.ImmutableRenameTableOperation;
import org.apache.iceberg.rest.events.operations.ImmutableRenameViewOperation;
import org.apache.iceberg.rest.events.operations.ImmutableUpdateNamespacePropertiesOperation;
import org.apache.iceberg.rest.events.operations.ImmutableUpdateTableOperation;
import org.apache.iceberg.rest.events.operations.ImmutableUpdateViewOperation;
import org.apache.iceberg.rest.events.operations.OperationType;
import org.apache.iceberg.rest.events.operations.RegisterTableOperation;
import org.apache.iceberg.rest.events.operations.RenameTableOperation;
import org.apache.iceberg.rest.events.operations.RenameViewOperation;
import org.apache.iceberg.rest.events.operations.UpdateNamespacePropertiesOperation;
import org.apache.iceberg.rest.events.operations.UpdateTableOperation;
import org.apache.iceberg.rest.events.operations.UpdateViewOperation;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

public class TestOperationParser {
  CreateNamespaceOperation createNamespaceOperation =
      ImmutableCreateNamespaceOperation.builder().namespace(Namespace.of("a", "b")).build();
  String createNamespaceOperationJson =
      "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}";

  CreateTableOperation createTableOperation =
      ImmutableCreateTableOperation.builder()
          .identifier(TableIdentifier.of(Namespace.empty(), "table"))
          .tableUuid("uuid")
          .updates(Lists.newArrayList())
          .build();
  String createTableOperationJson =
      "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[]}";

  CreateViewOperation createViewOperation =
      ImmutableCreateViewOperation.builder()
          .identifier(TableIdentifier.of(Namespace.empty(), "view"))
          .viewUuid("uuid")
          .build();
  String createViewOperationJson =
      "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\"}";

  DropNamespaceOperation dropNamespaceOperation =
      ImmutableDropNamespaceOperation.builder().namespace(Namespace.of("a", "b")).build();
  String dropNamespaceOperationJson =
      "{\"operation-type\":\"drop-namespace\",\"namespace\":[\"a\",\"b\"]}";

  DropTableOperation dropTableOperation =
      ImmutableDropTableOperation.builder()
          .identifier(TableIdentifier.of(Namespace.empty(), "table"))
          .tableUuid("uuid")
          .build();
  String dropTableOperationJson =
      "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}";

  DropViewOperation dropViewOperation =
      ImmutableDropViewOperation.builder()
          .identifier(TableIdentifier.of(Namespace.empty(), "view"))
          .viewUuid("uuid")
          .build();
  String dropViewOperationJson =
      "{\"operation-type\":\"drop-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\"}";

  RegisterTableOperation registerTableOperation =
      ImmutableRegisterTableOperation.builder()
          .identifier(TableIdentifier.of(Namespace.empty(), "table"))
          .tableUuid("uuid")
          .build();
  String registerTableOperationJson =
      "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}";

  RenameTableOperation renameTableOperation =
      ImmutableRenameTableOperation.builder()
          .sourceIdentifier(TableIdentifier.of(Namespace.empty(), "fromTable"))
          .destIdentifier(TableIdentifier.of(Namespace.empty(), "toTable"))
          .tableUuid("uuid")
          .build();
  String renameTableOperationJson =
      "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"fromTable\"},\"destination\":{\"namespace\":[],\"name\":\"toTable\"}}";

  RenameViewOperation renameViewOperation =
      ImmutableRenameViewOperation.builder()
          .sourceIdentifier(TableIdentifier.of(Namespace.empty(), "fromView"))
          .destIdentifier(TableIdentifier.of(Namespace.empty(), "toView"))
          .viewUuid("uuid")
          .build();
  String renameViewOperationJson =
      "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"fromView\"},\"destination\":{\"namespace\":[],\"name\":\"toView\"}}";

  UpdateNamespacePropertiesOperation updateNamespacePropertiesOperation =
      ImmutableUpdateNamespacePropertiesOperation.builder()
          .namespace(Namespace.of("a", "b"))
          .updated(Lists.newArrayList())
          .removed(Lists.newArrayList())
          .build();
  String updateNamespacePropertiesOperationJson =
      "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\",\"b\"],\"updated\":[],\"removed\":[]}";

  UpdateTableOperation updateTableOperation =
      ImmutableUpdateTableOperation.builder()
          .identifier(TableIdentifier.of(Namespace.empty(), "table"))
          .tableUuid("uuid")
          .updates(Lists.newArrayList())
          .build();
  String updateTableOperationJson =
      "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[]}";

  UpdateViewOperation updateViewOperation =
      ImmutableUpdateViewOperation.builder()
          .identifier(TableIdentifier.of(Namespace.empty(), "view"))
          .viewUuid("uuid")
          .updates(Lists.newArrayList())
          .build();
  String updateViewOperationJson =
      "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":[]}";

  CustomOperation customOperation =
      ImmutableCustomOperation.builder()
          .customOperationType(new OperationType.CustomOperationType("x-op"))
          .build();
  String customOperationJson = "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\"}";

  @Test
  void testToJson() {
    assertThat(
            JsonUtil.generate(gen -> OperationParser.toJson(createNamespaceOperation, gen), false))
        .isEqualTo(createNamespaceOperationJson);

    assertThat(JsonUtil.generate(gen -> OperationParser.toJson(createTableOperation, gen), false))
        .isEqualTo(createTableOperationJson);

    assertThat(JsonUtil.generate(gen -> OperationParser.toJson(createViewOperation, gen), false))
        .isEqualTo(createViewOperationJson);

    assertThat(JsonUtil.generate(gen -> OperationParser.toJson(dropNamespaceOperation, gen), false))
        .isEqualTo(dropNamespaceOperationJson);

    assertThat(JsonUtil.generate(gen -> OperationParser.toJson(dropTableOperation, gen), false))
        .isEqualTo(dropTableOperationJson);

    assertThat(JsonUtil.generate(gen -> OperationParser.toJson(dropViewOperation, gen), false))
        .isEqualTo(dropViewOperationJson);

    assertThat(JsonUtil.generate(gen -> OperationParser.toJson(registerTableOperation, gen), false))
        .isEqualTo(registerTableOperationJson);

    assertThat(JsonUtil.generate(gen -> OperationParser.toJson(renameTableOperation, gen), false))
        .isEqualTo(renameTableOperationJson);

    assertThat(JsonUtil.generate(gen -> OperationParser.toJson(renameViewOperation, gen), false))
        .isEqualTo(renameViewOperationJson);

    assertThat(
            JsonUtil.generate(
                gen -> OperationParser.toJson(updateNamespacePropertiesOperation, gen), false))
        .isEqualTo(updateNamespacePropertiesOperationJson);

    assertThat(JsonUtil.generate(gen -> OperationParser.toJson(updateTableOperation, gen), false))
        .isEqualTo(updateTableOperationJson);

    assertThat(JsonUtil.generate(gen -> OperationParser.toJson(updateViewOperation, gen), false))
        .isEqualTo(updateViewOperationJson);

    assertThat(JsonUtil.generate(gen -> OperationParser.toJson(customOperation, gen), false))
        .isEqualTo(customOperationJson);

    assertThatNullPointerException()
        .isThrownBy(() -> OperationParser.toJson(null, null))
        .withMessage("Invalid operation: null");
  }

  @Test
  void testFromJson() {
    assertThat(JsonUtil.parse(createNamespaceOperationJson, OperationParser::fromJson))
        .isEqualTo(createNamespaceOperation);

    assertThat(JsonUtil.parse(createTableOperationJson, OperationParser::fromJson))
        .isEqualTo(createTableOperation);

    assertThat(JsonUtil.parse(createViewOperationJson, OperationParser::fromJson))
        .isEqualTo(createViewOperation);

    assertThat(JsonUtil.parse(dropNamespaceOperationJson, OperationParser::fromJson))
        .isEqualTo(dropNamespaceOperation);

    assertThat(JsonUtil.parse(dropTableOperationJson, OperationParser::fromJson))
        .isEqualTo(dropTableOperation);

    assertThat(JsonUtil.parse(dropViewOperationJson, OperationParser::fromJson))
        .isEqualTo(dropViewOperation);

    assertThat(JsonUtil.parse(registerTableOperationJson, OperationParser::fromJson))
        .isEqualTo(registerTableOperation);

    assertThat(JsonUtil.parse(renameTableOperationJson, OperationParser::fromJson))
        .isEqualTo(renameTableOperation);

    assertThat(JsonUtil.parse(renameViewOperationJson, OperationParser::fromJson))
        .isEqualTo(renameViewOperation);

    assertThat(JsonUtil.parse(updateNamespacePropertiesOperationJson, OperationParser::fromJson))
        .isEqualTo(updateNamespacePropertiesOperation);

    assertThat(JsonUtil.parse(updateTableOperationJson, OperationParser::fromJson))
        .isEqualTo(updateTableOperation);

    assertThat(JsonUtil.parse(updateViewOperationJson, OperationParser::fromJson))
        .isEqualTo(updateViewOperation);

    assertThat(JsonUtil.parse(customOperationJson, OperationParser::fromJson))
        .isEqualTo(customOperation);

    assertThatNullPointerException()
        .isThrownBy(() -> OperationParser.fromJson(null))
        .withMessage("Invalid json object: null");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> JsonUtil.parse("{}", OperationParser::fromJson));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> JsonUtil.parse("{\"operation-type\":\"unknown\"}", OperationParser::fromJson))
        .withMessage("Invalid OperationType: unknown");
  }
}
