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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestManifestFilterManager extends TestBase {

  @TestTemplate
  public void obsoleteDeleteFilesAreFoundWithoutRemovedDataFiles() throws IOException {
    assumeThat(formatVersion).as("delete files require v2+").isGreaterThanOrEqualTo(2);

    // a manifest holding an obsolete delete file must be read even when no data files are removed
    // in the same commit
    ManifestEntry<DeleteFile> entry =
        manifestEntry(ManifestEntry.Status.EXISTING, 1L, 5L, 5L, FILE_B_DELETES);
    ManifestFile manifestB = writeManifest(1L, entry);

    CountingFilterManager filterManager = new CountingFilterManager();
    filterManager.dropDeleteFilesOlderThan(6L);

    filterManager.filterManifests(SCHEMA, ImmutableList.of(manifestB));

    assertThat(filterManager.opened)
        .as("A manifest that can hold a delete file below the sequence number must still be read")
        .containsExactly(manifestB.path());
  }

  @TestTemplate
  public void manifestsAtOrAboveTheSequenceNumberAreNotOpened() throws IOException {
    assumeThat(formatVersion).as("delete files require v2+").isGreaterThanOrEqualTo(2);

    // a manifest whose minimum data sequence number is already at or above the threshold cannot
    // contain an obsolete delete file, so it should not be opened
    ManifestEntry<DeleteFile> entry =
        manifestEntry(ManifestEntry.Status.EXISTING, 1L, 5L, 5L, FILE_B_DELETES);
    ManifestFile manifestB = writeManifest(1L, entry);

    CountingFilterManager filterManager = new CountingFilterManager();
    filterManager.dropDeleteFilesOlderThan(5L);

    List<ManifestFile> filtered =
        filterManager.filterManifests(SCHEMA, ImmutableList.of(manifestB));

    assertThat(filterManager.opened)
        .as("A manifest that cannot hold an obsolete delete file should not be read")
        .isEmpty();
    assertThat(filtered).containsExactly(manifestB);
  }

  @TestTemplate
  public void danglingDVsAreFoundWhenDeleteFilesAreAlsoRemoved() throws IOException {
    assumeThat(formatVersion).as("DVs are only written in v3 and later").isGreaterThanOrEqualTo(3);

    ManifestFile manifestA = writeDeleteManifest(formatVersion, 1L, newDV(FILE_A));

    // FILE_B_DELETES has no manifest location, so canTrustManifestReferences is false
    // This tests only tests the non-trusted manifest path
    assertThat(FILE_B_DELETES.manifestLocation()).isNull();

    CountingFilterManager filterManager = new CountingFilterManager();
    filterManager.delete(FILE_B_DELETES);
    filterManager.removeDanglingDeletesFor(ImmutableSet.of(FILE_A));
    filterManager.filterManifests(SCHEMA, ImmutableList.of(manifestA));

    assertThat(filterManager.opened)
        .as(
            "A dangling DV must be found even when an unrelated delete file in a different "
                + "partition is also explicitly removed in the same commit")
        .containsExactly(manifestA.path());
  }

  /** A delete-manifest filter manager that records every manifest it opens. */
  private class CountingFilterManager extends ManifestFilterManager<DeleteFile> {
    final Set<String> opened = ConcurrentHashMap.newKeySet();

    CountingFilterManager() {
      super(table.specs(), ThreadPools::getWorkerPool);
    }

    @Override
    protected void deleteFile(String location) {}

    @Override
    protected ManifestWriter<DeleteFile> newManifestWriter(PartitionSpec spec) {
      OutputFile outputFile =
          Files.localOutput(
              manifestFormat()
                  .addExtension(temp.resolve("filtered" + System.nanoTime()).toFile().toString()));
      return ManifestFiles.writeDeleteManifest(formatVersion, spec, outputFile, 2L);
    }

    @Override
    protected ManifestReader<DeleteFile> newManifestReader(ManifestFile manifest) {
      opened.add(manifest.path());
      return ManifestFiles.readDeleteManifest(manifest, FILE_IO, table.specs());
    }

    @Override
    protected Set<DeleteFile> newFileSet() {
      return DeleteFileSet.create();
    }
  }
}
