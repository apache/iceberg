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
package org.apache.iceberg.rest.events.operations;

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class CustomOperation implements Operation {
  private final OperationType operationType = OperationType.CUSTOM;
  private final OperationType.CustomOperationType customOperationType;

  // Common optional properties
  private final TableIdentifier identifier;
  private final Namespace namespace;
  private final String tableUuid;
  private final String viewUuid;
  private final Map<String, String> properties;

  public CustomOperation(
      OperationType.CustomOperationType customOperationType,
      org.apache.iceberg.catalog.TableIdentifier identifier,
      org.apache.iceberg.catalog.Namespace namespace,
      String tableUuid,
      String viewUuid,
      Map<String, String> properties) {
    this.customOperationType = customOperationType;
    this.identifier = identifier;
    this.namespace = namespace;
    this.tableUuid = tableUuid;
    this.viewUuid = viewUuid;
    this.properties = properties;
  }

  public CustomOperation(OperationType.CustomOperationType customOperationType) {
    this(customOperationType, null, null, null, null, null);
  }

  public OperationType operationType() {
    return operationType;
  }

  public OperationType.CustomOperationType customOperationType() {
    return customOperationType;
  }

  public TableIdentifier identifier() {
    return identifier;
  }

  public Namespace namespace() {
    return namespace;
  }

  public String tableUuid() {
    return tableUuid;
  }

  public String viewUuid() {
    return viewUuid;
  }

  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("operationType", operationType)
        .add("customOperationType", customOperationType)
        .add("identifier", identifier)
        .add("namespace", namespace)
        .add("tableUuid", tableUuid)
        .add("viewUuid", viewUuid)
        .add("properties", properties)
        .toString();
  }
}
