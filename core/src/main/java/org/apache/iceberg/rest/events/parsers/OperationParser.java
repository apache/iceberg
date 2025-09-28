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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.rest.events.operations.CreateNamespaceOperation;
import org.apache.iceberg.rest.events.operations.CreateTableOperation;
import org.apache.iceberg.rest.events.operations.CreateViewOperation;
import org.apache.iceberg.rest.events.operations.CustomOperation;
import org.apache.iceberg.rest.events.operations.DropNamespaceOperation;
import org.apache.iceberg.rest.events.operations.DropTableOperation;
import org.apache.iceberg.rest.events.operations.DropViewOperation;
import org.apache.iceberg.rest.events.operations.Operation;
import org.apache.iceberg.rest.events.operations.OperationType;
import org.apache.iceberg.rest.events.operations.RegisterTableOperation;
import org.apache.iceberg.rest.events.operations.RenameTableOperation;
import org.apache.iceberg.rest.events.operations.RenameViewOperation;
import org.apache.iceberg.rest.events.operations.UpdateNamespacePropertiesOperation;
import org.apache.iceberg.rest.events.operations.UpdateTableOperation;
import org.apache.iceberg.rest.events.operations.UpdateViewOperation;
import org.apache.iceberg.util.JsonUtil;

public class OperationParser {
  private OperationParser() {}

  public static void toJson(Operation operation, JsonGenerator gen) throws IOException {
    switch (operation.operationType()) {
      case CREATE_NAMESPACE:
        CreateNamespaceOperationParser.toJson((CreateNamespaceOperation) operation, gen);
        break;
      case CREATE_TABLE:
        CreateTableOperationParser.toJson((CreateTableOperation) operation, gen);
        break;
      case CREATE_VIEW:
        CreateViewOperationParser.toJson((CreateViewOperation) operation, gen);
        break;
      case DROP_NAMESPACE:
        DropNamespaceOperationParser.toJson((DropNamespaceOperation) operation, gen);
        break;
      case DROP_TABLE:
        DropTableOperationParser.toJson((DropTableOperation) operation, gen);
        break;
      case DROP_VIEW:
        DropViewOperationParser.toJson((DropViewOperation) operation, gen);
        break;
      case REGISTER_TABLE:
        RegisterTableOperationParser.toJson((RegisterTableOperation) operation, gen);
        break;
      case RENAME_TABLE:
        RenameTableOperationParser.toJson((RenameTableOperation) operation, gen);
        break;
      case RENAME_VIEW:
        RenameViewOperationParser.toJson((RenameViewOperation) operation, gen);
        break;
      case UPDATE_NAMESPACE_PROPERTIES:
        UpdateNamespacePropertiesOperationParser.toJson(
            (UpdateNamespacePropertiesOperation) operation, gen);
        break;
      case UPDATE_TABLE:
        UpdateTableOperationParser.toJson((UpdateTableOperation) operation, gen);
        break;
      case UPDATE_VIEW:
        UpdateViewOperationParser.toJson((UpdateViewOperation) operation, gen);
        break;
      case CUSTOM:
        CustomOperationParser.toJson((CustomOperation) operation, gen);
        break;
      default:
        throw new IllegalArgumentException("Unknown operation type: " + operation.operationType());
    }
  }

  public static Operation fromJson(JsonNode json) {
    OperationType operationType = OperationType.fromType(JsonUtil.getString("operation-type", json));
    switch (operationType) {
      case CREATE_NAMESPACE:
        return CreateNamespaceOperationParser.fromJson(json);
      case CREATE_TABLE:
        return CreateTableOperationParser.fromJson(json);
      case CREATE_VIEW:
        return CreateViewOperationParser.fromJson(json);
      case DROP_NAMESPACE:
        return DropNamespaceOperationParser.fromJson(json);
      case DROP_TABLE:
        return DropTableOperationParser.fromJson(json);
      case DROP_VIEW:
        return DropViewOperationParser.fromJson(json);
      case REGISTER_TABLE:
        return RegisterTableOperationParser.fromJson(json);
      case RENAME_TABLE:
        return RenameTableOperationParser.fromJson(json);
      case RENAME_VIEW:
        return RenameViewOperationParser.fromJson(json);
      case UPDATE_NAMESPACE_PROPERTIES:
        return UpdateNamespacePropertiesOperationParser.fromJson(json);
      case UPDATE_TABLE:
        return UpdateTableOperationParser.fromJson(json);
      case UPDATE_VIEW:
        return UpdateViewOperationParser.fromJson(json);
      case CUSTOM:
        return CustomOperationParser.fromJson(json);
      default:
        throw new IllegalArgumentException("Unknown operation type: " + operationType);
    }
  }
}
