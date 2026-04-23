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
package org.apache.iceberg.rest.restrictions;

import java.util.Optional;

/**
 * Capability interface for table implementations that can carry per-principal {@link
 * ReadRestrictions}.
 *
 * <p>Implemented by tables loaded via a catalog that honors server-specified read restrictions (the
 * REST catalog today). Non-REST catalogs and wrapper implementations (SerializableTable, metadata
 * tables) don't implement this; callers should route through {@code TableUtil.readRestrictions} in
 * iceberg-core instead of raw {@code instanceof} checks so the decision locus stays consistent.
 */
public interface SupportsReadRestrictions {
  default Optional<ReadRestrictions> readRestrictions() {
    return Optional.empty();
  }
}
