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
import java.util.Set;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteTablePathUtil extends TestBase {

  @Test
  public void testStagingPathPreservesDirectoryStructure() {
    String sourcePrefix = "/source/table";
    String stagingDir = "/staging/";

    // Two files with same name but different paths
    String file1 = "/source/table/hash1/delete_0_0_0.parquet";
    String file2 = "/source/table/hash2/delete_0_0_0.parquet";

    String stagingPath1 = RewriteTablePathUtil.stagingPath(file1, sourcePrefix, stagingDir);
    String stagingPath2 = RewriteTablePathUtil.stagingPath(file2, sourcePrefix, stagingDir);

    // Should preserve directory structure to avoid conflicts
    assertThat(stagingPath1)
        .startsWith(stagingDir)
        .isEqualTo("/staging/hash1/delete_0_0_0.parquet")
        .isNotEqualTo(stagingPath2);
    assertThat(stagingPath2)
        .startsWith(stagingDir)
        .isEqualTo("/staging/hash2/delete_0_0_0.parquet");
  }

  @Test
  public void testStagingPathBackwardCompatibility() {
    // Test that the deprecated method still works
    String originalPath = "/some/path/file.parquet";
    String stagingDir = "/staging/";

    String result = RewriteTablePathUtil.stagingPath(originalPath, stagingDir);

    assertThat(result).isEqualTo("/staging/file.parquet");
  }

  @Test
  public void testStagingPathWithComplexPaths() {
    String sourcePrefix = "/warehouse/db/table";
    String stagingDir = "/tmp/staging/";

    String filePath = "/warehouse/db/table/data/year=2023/month=01/part-00001.parquet";
    String result = RewriteTablePathUtil.stagingPath(filePath, sourcePrefix, stagingDir);

    assertThat(result).isEqualTo("/tmp/staging/data/year=2023/month=01/part-00001.parquet");
  }

  @Test
  public void testStagingPathWithNoMiddlePart() {
    // Test case where file is directly under source prefix (no middle directory structure)
    String sourcePrefix = "/source/table";
    String stagingDir = "/staging/";
    String fileDirectlyUnderPrefix = "/source/table/file.parquet";

    // Test new method
    String newMethodResult =
        RewriteTablePathUtil.stagingPath(fileDirectlyUnderPrefix, sourcePrefix, stagingDir);

    // Test old deprecated method
    String oldMethodResult = RewriteTablePathUtil.stagingPath(fileDirectlyUnderPrefix, stagingDir);

    // Both methods should behave the same when there's no middle part
    assertThat(newMethodResult).isEqualTo("/staging/file.parquet");
    assertThat(oldMethodResult).isEqualTo("/staging/file.parquet");
    assertThat(newMethodResult).isEqualTo(oldMethodResult);
  }

  // A position delete entry that is not rewritten (e.g. a DELETED entry not copied to the target)
  // is absent from the measured-size map and must keep its original file_size_in_bytes.
  @TestTemplate
  public void testRewriteDeleteManifestFallsBackToOriginalSizeForDeletedEntries()
      throws IOException {
    assumeThat(formatVersion)
        .as("Delete files only work for format version 2+")
        .isGreaterThanOrEqualTo(2);

    String sourcePrefix = "/path/to/";
    String targetPrefix = "/path/new/";
    String stagingDir = "/staging/";

    // FILE_A_DELETES is live, so its rewritten size is measured and supplied; FILE_B_DELETES is a
    // DELETED entry, absent from the size map.
    ManifestFile manifest = deleteManifestWithLiveAndDeletedEntry(FILE_A_DELETES, FILE_B_DELETES);

    long measuredSizeForA = 9999L;
    OutputFile output =
        Files.localOutput(
            FileFormat.AVRO.addExtension(
                temp.resolve("junit" + System.nanoTime()).toFile().toString()));
    RewriteTablePathUtil.rewriteDeleteManifest(
        manifest,
        Set.of(1000L),
        output,
        table.io(),
        formatVersion,
        table.specs(),
        sourcePrefix,
        targetPrefix,
        stagingDir,
        ImmutableMap.of(FILE_A_DELETES.location(), measuredSizeForA));

    InputFile rewrittenInput = output.toInputFile();
    ManifestFile rewritten =
        new GenericManifestFile(
            rewrittenInput.location(),
            rewrittenInput.getLength(),
            SPEC.specId(),
            ManifestContent.DELETES,
            0L,
            0L,
            1000L,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    int seen = 0;
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(rewritten, table.io(), table.specs())) {
      for (ManifestEntry<DeleteFile> entry : reader.entries()) {
        seen++;
        if (entry.status() == ManifestEntry.Status.DELETED) {
          assertThat(entry.file().fileSizeInBytes())
              .as("DELETED entry should fall back to its original size")
              .isEqualTo(FILE_B_DELETES.fileSizeInBytes());
        } else {
          assertThat(entry.file().fileSizeInBytes())
              .as("Live entry should use the measured rewritten size")
              .isEqualTo(measuredSizeForA);
        }
      }
    }

    assertThat(seen).as("Both the live and deleted entries should be present").isEqualTo(2);
  }

  private ManifestFile deleteManifestWithLiveAndDeletedEntry(DeleteFile live, DeleteFile deleted)
      throws IOException {
    OutputFile manifestFile =
        Files.localOutput(
            FileFormat.AVRO.addExtension(
                temp.resolve("junit" + System.nanoTime()).toFile().toString()));
    ManifestWriter<DeleteFile> writer =
        ManifestFiles.writeDeleteManifest(formatVersion, SPEC, manifestFile, 1000L);
    try {
      writer.add(live);
      writer.delete(deleted, 1, null);
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }
}
