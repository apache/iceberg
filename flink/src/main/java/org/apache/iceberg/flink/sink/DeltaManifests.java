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

import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.jetbrains.annotations.NotNull;

class DeltaManifests implements Iterable<ManifestFile> {

  private final ManifestFile dataManifest;
  private final ManifestFile deleteManifest;

  DeltaManifests(ManifestFile dataManifest, ManifestFile deleteManifest) {
    this.dataManifest = dataManifest;
    this.deleteManifest = deleteManifest;
  }

  ManifestFile dataManifest() {
    return dataManifest;
  }

  ManifestFile deleteManifest() {
    return deleteManifest;
  }

  @NotNull
  @Override
  public Iterator<ManifestFile> iterator() {
    List<ManifestFile> manifests = Lists.newArrayListWithCapacity(2);
    if (dataManifest != null) {
      manifests.add(dataManifest);
    }

    if (deleteManifest != null) {
      manifests.add(deleteManifest);
    }

    return manifests.iterator();
  }
}
