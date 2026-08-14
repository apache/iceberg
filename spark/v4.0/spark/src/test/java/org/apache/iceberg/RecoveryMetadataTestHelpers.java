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

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;

public final class RecoveryMetadataTestHelpers {
  private RecoveryMetadataTestHelpers() {}

  public static Snapshot newReplaceBridge(
      Snapshot replacementSnapshot,
      long parentSnapshotId,
      long firstRowId,
      long addedRows,
      FileIO io,
      EncryptionManager encryptionManager,
      OutputFile manifestListFile)
      throws IOException {
    ManifestListWriter writer =
        ManifestLists.write(
            3,
            manifestListFile,
            encryptionManager,
            replacementSnapshot.snapshotId(),
            parentSnapshotId,
            replacementSnapshot.sequenceNumber(),
            firstRowId);

    try (writer) {
      writer.addAll(replacementSnapshot.allManifests(io));
    }

    ManifestListFile manifestList = writer.toManifestListFile();
    return new BaseSnapshot(
        replacementSnapshot.sequenceNumber(),
        replacementSnapshot.snapshotId(),
        parentSnapshotId,
        replacementSnapshot.timestampMillis(),
        DataOperations.REPLACE,
        Map.of(),
        replacementSnapshot.schemaId(),
        manifestList.location(),
        firstRowId,
        addedRows,
        manifestList.encryptionKeyID());
  }
}
