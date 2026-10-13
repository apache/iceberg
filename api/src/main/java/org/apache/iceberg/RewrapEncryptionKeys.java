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

import java.util.List;

/**
 * API for re-wrapping the encryption keys of a table so that they no longer depend on a superseded
 * version of the table master key.
 *
 * <p>Only the files reachable from the current snapshot are re-wrapped. Older snapshots keep
 * depending on the previous keys until they are expired.
 *
 * <p>{@link #apply()} returns the manifests that will be replaced.
 */
public interface RewrapEncryptionKeys extends PendingUpdate<List<ManifestFile>> {

  /**
   * An enum representing the lowest key that is regenerated. The keys above it are regenerated too,
   * the keys directly below it are re-wrapped under the new ones, and the keys further below are
   * left unchanged.
   */
  enum KeyRegenerationLevel {
    /** Regenerate the key encryption key, rewriting no manifest or data file. */
    KEY_ENCRYPTION_KEY,
    /** Regenerate the manifest keys and the keys above them, rewriting every manifest (default). */
    MANIFEST_KEY
  }

  /**
   * Sets the lowest key to regenerate.
   *
   * <p>Consider {@link KeyRegenerationLevel#KEY_ENCRYPTION_KEY} when rewriting every manifest is
   * too expensive.
   *
   * @param level the lowest key to regenerate
   * @return this for method chaining
   */
  RewrapEncryptionKeys keyRegenerationLevel(KeyRegenerationLevel level);
}
