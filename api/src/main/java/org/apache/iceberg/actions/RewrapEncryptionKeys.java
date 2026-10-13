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
package org.apache.iceberg.actions;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.RewrapEncryptionKeys.KeyRegenerationLevel;

/**
 * An action that re-wraps the encryption keys of a table.
 *
 * <p>Similar to {@link org.apache.iceberg.RewrapEncryptionKeys} but may use a query engine to
 * distribute parts of the work.
 */
public interface RewrapEncryptionKeys
    extends SnapshotUpdate<RewrapEncryptionKeys, RewrapEncryptionKeys.Result> {

  /**
   * Sets the lowest key to regenerate.
   *
   * @param level the lowest key to regenerate
   * @return this for method chaining
   */
  RewrapEncryptionKeys keyRegenerationLevel(KeyRegenerationLevel level);

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** Returns the manifests written with new keys. */
    Iterable<ManifestFile> rewrappedManifests();
  }
}
