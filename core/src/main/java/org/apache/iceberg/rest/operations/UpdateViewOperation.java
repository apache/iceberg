/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.rest.operations;

import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class UpdateViewOperation {
    private final OperationType operationType = OperationType.UPDATE_VIEW;
    private final TableIdentifier identifier;
    private final String viewUuid;
    private final MetadataUpdate[] updates;
    private final UpdateRequirement[] requirements;

    public UpdateViewOperation(TableIdentifier identifier, String viewUuid, MetadataUpdate[] updates) {
        this(identifier, viewUuid, updates, null);
    }

    public UpdateViewOperation(TableIdentifier identifier,
                               String viewUuid, MetadataUpdate[] updates,
                               UpdateRequirement[] requirements) {
        this.identifier = identifier;
        this.viewUuid = viewUuid;
        this.updates = updates;
        this.requirements = requirements;
    }

    public OperationType operationType() {
        return operationType;
    }

    public TableIdentifier identifier() {
        return identifier;
    }

    public String viewUuid() {
        return viewUuid;
    }

    public MetadataUpdate[] updates() {
        return updates;
    }

    public UpdateRequirement[] requirements() {
        return requirements;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("operationType", operationType)
                .add("identifier", identifier)
                .add("viewUuid", viewUuid)
                .add("updates", updates)
                .add("requirements", requirements)
                .toString();
    }
}
