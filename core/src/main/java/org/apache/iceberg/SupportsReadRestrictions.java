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
package org.apache.iceberg;

import java.util.Optional;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;

/**
 * Capability interface for table implementations that can carry per-principal {@link
 * ReadRestrictions}.
 *
 * <p>Currently populated only by the REST catalog. Non-REST catalogs either don't implement this or
 * return {@link Optional#empty()} from {@link #readRestrictions()}. Consumers should route through
 * {@code TableUtil.readRestrictions(Table)} to handle both cases uniformly.
 */
public interface SupportsReadRestrictions {
  default Optional<ReadRestrictions> readRestrictions() {
    return Optional.empty();
  }
}
