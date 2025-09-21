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

package org.apache.iceberg.catalog;

import java.util.Objects;

/** Identifies a {@link CatalogObject} by UUID. */
public class CatalogObjectUuid {
    private final String uuid;
    private final CatalogObjectType type;

    public CatalogObjectUuid(String uuid, CatalogObjectType type) {
        if (uuid == null || uuid.isEmpty()) {
            throw new IllegalArgumentException("Invalid UUID: null or empty");
        }

        if (type == null) {
            throw new IllegalArgumentException("Invalid CatalogObjectType: null");
        }

        this.uuid = uuid;
        this.type = type;
    }

    public String uuid() {
        return uuid;
    }

    public CatalogObjectType type() {
        return type;
    }

    @Override
    public String toString() {
        return String.format("CatalogObjectUuid{uuid='%s', type=%s}", uuid, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CatalogObjectUuid that = (CatalogObjectUuid) o;
        return uuid.equals(that.uuid) && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, type);
    }
}
