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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.immutables.value.Value;

/** A committed catalog operation carried by an {@link Event}. */
@Value.Enclosing
public interface CatalogOperation {
  String operationType();

  @Value.Immutable
  interface CreateTable extends CatalogOperation {
    TableIdentifier identifier();

    String tableUuid();

    List<MetadataUpdate> updates();

    @Override
    @Value.Derived
    default String operationType() {
      return OperationType.CREATE_TABLE;
    }
  }

  @Value.Immutable
  interface RegisterTable extends CatalogOperation {
    TableIdentifier identifier();

    String tableUuid();

    @Nullable
    List<MetadataUpdate> updates();

    @Override
    @Value.Derived
    default String operationType() {
      return OperationType.REGISTER_TABLE;
    }
  }

  @Value.Immutable
  interface DropTable extends CatalogOperation {
    TableIdentifier identifier();

    String tableUuid();

    @Nullable
    Boolean purge();

    @Override
    @Value.Derived
    default String operationType() {
      return OperationType.DROP_TABLE;
    }
  }

  @Value.Immutable
  interface UpdateTable extends CatalogOperation {
    TableIdentifier identifier();

    String tableUuid();

    List<MetadataUpdate> updates();

    @Nullable
    List<UpdateRequirement> requirements();

    @Override
    @Value.Derived
    default String operationType() {
      return OperationType.UPDATE_TABLE;
    }
  }

  @Value.Immutable
  interface RenameTable extends CatalogOperation {
    TableIdentifier source();

    TableIdentifier destination();

    String tableUuid();

    @Override
    @Value.Derived
    default String operationType() {
      return OperationType.RENAME_TABLE;
    }
  }

  @Value.Immutable
  interface CreateView extends CatalogOperation {
    TableIdentifier identifier();

    String viewUuid();

    List<MetadataUpdate> updates();

    @Override
    @Value.Derived
    default String operationType() {
      return OperationType.CREATE_VIEW;
    }
  }

  @Value.Immutable
  interface DropView extends CatalogOperation {
    TableIdentifier identifier();

    String viewUuid();

    @Override
    @Value.Derived
    default String operationType() {
      return OperationType.DROP_VIEW;
    }
  }

  @Value.Immutable
  interface UpdateView extends CatalogOperation {
    TableIdentifier identifier();

    String viewUuid();

    List<MetadataUpdate> updates();

    @Nullable
    List<UpdateRequirement> requirements();

    @Override
    @Value.Derived
    default String operationType() {
      return OperationType.UPDATE_VIEW;
    }
  }

  @Value.Immutable
  interface RenameView extends CatalogOperation {
    TableIdentifier source();

    TableIdentifier destination();

    String viewUuid();

    @Override
    @Value.Derived
    default String operationType() {
      return OperationType.RENAME_VIEW;
    }
  }

  @Value.Immutable
  interface CreateNamespace extends CatalogOperation {
    Namespace namespace();

    Map<String, String> properties();

    @Override
    @Value.Derived
    default String operationType() {
      return OperationType.CREATE_NAMESPACE;
    }
  }

  @Value.Immutable
  interface UpdateNamespaceProperties extends CatalogOperation {
    Namespace namespace();

    List<String> updated();

    List<String> removed();

    @Nullable
    List<String> missing();

    @Override
    @Value.Derived
    default String operationType() {
      return OperationType.UPDATE_NAMESPACE_PROPERTIES;
    }
  }

  @Value.Immutable
  interface DropNamespace extends CatalogOperation {
    Namespace namespace();

    @Override
    @Value.Derived
    default String operationType() {
      return OperationType.DROP_NAMESPACE;
    }
  }

  /** An operation type that this client does not recognize. */
  @Value.Immutable
  interface Unknown extends CatalogOperation {
    @Override
    String operationType();
  }
}
