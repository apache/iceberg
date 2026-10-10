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

/**
 * An action that re-wraps the encryption keys of a table so that they no longer depend on a
 * superseded version of the table master key.
 *
 * <p>Only the metadata reachable from the current snapshot is re-wrapped. Data and delete files are
 * not rewritten, and older snapshots keep depending on the previous keys until they are expired.
 */
public interface RewrapEncryptionKeys
    extends SnapshotUpdate<RewrapEncryptionKeys, RewrapEncryptionKeys.Result> {

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** Returns the manifests written with new keys. */
    Iterable<ManifestFile> rewrappedManifests();
  }
}
