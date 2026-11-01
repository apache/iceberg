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
package org.apache.iceberg.flink.sink;

import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

@Internal
public class DeltaManifests {

  private static final CharSequence[] EMPTY_REF_DATA_FILES = new CharSequence[0];

  private final ManifestFile dataManifest;
  private final List<ManifestFile> deleteManifests;
  private final CharSequence[] referencedDataFiles;

  DeltaManifests(ManifestFile dataManifest, ManifestFile deleteManifest) {
    this(
        dataManifest,
        deleteManifest != null ? ImmutableList.of(deleteManifest) : ImmutableList.of(),
        EMPTY_REF_DATA_FILES);
  }

  DeltaManifests(
      ManifestFile dataManifest,
      List<ManifestFile> deleteManifests,
      CharSequence[] referencedDataFiles) {
    Preconditions.checkNotNull(deleteManifests, "Delete manifests shouldn't be null.");
    Preconditions.checkNotNull(referencedDataFiles, "Referenced data files shouldn't be null.");

    this.dataManifest = dataManifest;
    this.deleteManifests = ImmutableList.copyOf(deleteManifests);
    this.referencedDataFiles = referencedDataFiles;
  }

  ManifestFile dataManifest() {
    return dataManifest;
  }

  List<ManifestFile> deleteManifests() {
    return deleteManifests;
  }

  CharSequence[] referencedDataFiles() {
    return referencedDataFiles;
  }

  public List<ManifestFile> manifests() {
    List<ManifestFile> manifests = Lists.newArrayListWithCapacity(1 + deleteManifests.size());
    if (dataManifest != null) {
      manifests.add(dataManifest);
    }

    manifests.addAll(deleteManifests);
    return manifests;
  }
}
