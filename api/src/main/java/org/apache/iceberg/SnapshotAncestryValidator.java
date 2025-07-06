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

import javax.annotation.Nonnull;

/**
 * Interface to support validating snapshot ancestry during the commit process.
 *
 * <p>Validation will be called after the table metadata is refreshed to pick up any changes to the
 * table state.
 */
@FunctionalInterface
public interface SnapshotAncestryValidator {

  SnapshotAncestryValidator NON_VALIDATING = baseSnapshots -> true;

  /**
   * Validate the snapshots based on the refreshed table state.
   *
   * @param baseSnapshots ancestry of the base table metadata snapshots
   * @return boolean for whether the update is valid
   */
  boolean validate(Iterable<Snapshot> baseSnapshots);

  /**
   * Validation message that will be included when throwing {@link
   * org.apache.iceberg.exceptions.ValidationException}
   *
   * @return message
   */
  @Nonnull
  default String errorMessage() {
    return "error message not provided";
  }
}
