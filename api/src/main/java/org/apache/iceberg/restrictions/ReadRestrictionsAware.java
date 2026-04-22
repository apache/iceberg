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
package org.apache.iceberg.restrictions;

import java.util.Optional;

/**
 * Marker interface for table implementations that can carry per-principal {@link ReadRestrictions}.
 *
 * <p>The default returns {@link Optional#empty()} so implementations that never produce
 * restrictions (most non-REST catalogs) need not implement anything.
 */
public interface ReadRestrictionsAware {
  default Optional<ReadRestrictions> readRestrictions() {
    return Optional.empty();
  }
}
