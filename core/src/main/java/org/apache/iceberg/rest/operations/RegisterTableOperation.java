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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class RegisterTableOperation {
    private final OperationType operationType = OperationType.REGISTER_TABLE;
    private final TableIdentifier identifier;
    private final String tableUuid;
    private final MetadataUpdate[] updates;

    public RegisterTableOperation(TableIdentifier identifier, String tableUuid) {
        this(identifier, tableUuid, null);
    }

    public RegisterTableOperation(TableIdentifier identifier, String tableUuid, MetadataUpdate[] updates) {
        this.identifier = identifier;
        this.tableUuid = tableUuid;
        this.updates = updates;
    }

    public OperationType operationType() {
        return operationType;
    }

    public TableIdentifier identifier() {
        return identifier;
    }

    public String tableUuid() {
        return tableUuid;
    }

    public MetadataUpdate[] updates() {
        return updates;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("operationType", operationType)
                .add("identifier", identifier)
                .add("tableUuid", tableUuid)
                .add("updates", updates)
                .toString();
    }
}
