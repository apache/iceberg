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
package org.apache.iceberg.rest.operations;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class RenameTableOperation implements Operation {
  private final OperationType operationType = OperationType.RENAME_TABLE;
  private final TableIdentifier sourceIdentifier;
  private final TableIdentifier targetIdentifier;
  private final String tableUuid;

  public RenameTableOperation(
      TableIdentifier sourceIdentifier, TableIdentifier targetIdentifier, String tableUuid) {
    this.sourceIdentifier = sourceIdentifier;
    this.targetIdentifier = targetIdentifier;
    this.tableUuid = tableUuid;
  }

  public OperationType operationType() {
    return operationType;
  }

  public TableIdentifier sourceIdentifier() {
    return sourceIdentifier;
  }

  public TableIdentifier targetIdentifier() {
    return targetIdentifier;
  }

  public String tableUuid() {
    return tableUuid;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("operationType", operationType)
        .add("sourceIdentifier", sourceIdentifier)
        .add("targetIdentifier", targetIdentifier)
        .add("tableUuid", tableUuid)
        .toString();
  }
}
