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

import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class UpdateNamespacePropertiesOperation implements Operation {
  private final OperationType operationType = OperationType.UPDATE_NAMESPACE_PROPERTIES;
  private final Namespace namespace;
  // List of namespace property keys that were removed
  private final List<String> removed;
  // List of namespace property keys that were added or updated
  private final List<String> updated;
  // List of properties that were requested for removal that were not found in the namespace's
  // properties
  private final List<String> missing;

  public UpdateNamespacePropertiesOperation(
      Namespace namespace, List<String> removed, List<String> updated, List<String> missing) {
    this.namespace = namespace;
    this.removed = removed;
    this.updated = updated;
    this.missing = missing;
  }

  public UpdateNamespacePropertiesOperation(
      Namespace namespace, List<String> removed, List<String> updated) {
    this(namespace, removed, updated, null);
  }

  public OperationType operationType() {
    return operationType;
  }

  public Namespace namespace() {
    return namespace;
  }

  public List<String> removed() {
    return removed;
  }

  public List<String> updated() {
    return updated;
  }

  public List<String> missing() {
    return missing;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("operationType", operationType)
        .add("namespace", namespace)
        .add("removed", removed)
        .add("updated", updated)
        .add("missing", missing)
        .toString();
  }
}
