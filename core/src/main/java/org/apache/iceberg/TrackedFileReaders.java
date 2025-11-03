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

import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Factory methods for creating {@link TrackedFileReader} instances. */
public class TrackedFileReaders {

  private TrackedFileReaders() {}

  /**
   * Create a reader for a root manifest.
   *
   * @param rootManifestPath path to the root manifest file
   * @param io file IO for reading
   * @param snapshotId snapshot ID for metadata inheritance
   * @param sequenceNumber sequence number for metadata inheritance
   * @param firstRowId starting first row ID for data files (can be null)
   * @return a TrackedFileReader for the root manifest
   */
  public static TrackedFileReader readRoot(
      String rootManifestPath, FileIO io, long snapshotId, long sequenceNumber, Long firstRowId) {
    InputFile inputFile = io.newInputFile(rootManifestPath);
    InheritableTrackedMetadata metadata =
        InheritableTrackedMetadataFactory.create(snapshotId, sequenceNumber);

    return new TrackedFileReader(inputFile, metadata, firstRowId);
  }

  /**
   * Create a reader for a leaf manifest referenced from a root manifest.
   *
   * @param manifestEntry the DATA_MANIFEST or DELETE_MANIFEST entry from root
   * @param io file IO for reading
   * @param specsById map of partition specs by ID
   * @return a TrackedFileReader for the leaf manifest
   */
  public static TrackedFileReader readLeaf(
      TrackedFile<?> manifestEntry, FileIO io, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkArgument(
        manifestEntry.contentType() == FileContent.DATA_MANIFEST
            || manifestEntry.contentType() == FileContent.DELETE_MANIFEST,
        "Can only read manifest entries, got: %s",
        manifestEntry.contentType());

    InputFile inputFile = io.newInputFile(manifestEntry.location());

    InheritableTrackedMetadata metadata =
        InheritableTrackedMetadataFactory.fromTrackedFile(manifestEntry);

    TrackingInfo tracking = manifestEntry.trackingInfo();
    Long firstRowId = tracking != null ? tracking.firstRowId() : null;

    return new TrackedFileReader(inputFile, metadata, firstRowId);
  }
}
