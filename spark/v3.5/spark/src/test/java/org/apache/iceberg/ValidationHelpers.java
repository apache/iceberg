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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class ValidationHelpers {

  private ValidationHelpers() {}

  public static List<Long> dataSeqs(Long... seqs) {
    return Arrays.asList(seqs);
  }

  public static List<Long> fileSeqs(Long... seqs) {
    return Arrays.asList(seqs);
  }

  public static List<Long> snapshotIds(Long... ids) {
    return Arrays.asList(ids);
  }

  public static List<String> files(ContentFile<?>... files) {
    return Arrays.stream(files).map(file -> file.path().toString()).collect(Collectors.toList());
  }

  public static void validateDataManifest(
      Table table,
      ManifestFile manifest,
      List<Long> dataSeqs,
      List<Long> fileSeqs,
      List<Long> snapshotIds,
      List<String> files) {

    List<Long> actualDataSeqs = Lists.newArrayList();
    List<Long> actualFileSeqs = Lists.newArrayList();
    List<Long> actualSnapshotIds = Lists.newArrayList();
    List<String> actualFiles = Lists.newArrayList();

    for (ManifestEntry<DataFile> entry : ManifestFiles.read(manifest, table.io()).entries()) {
      actualDataSeqs.add(entry.dataSequenceNumber());
      actualFileSeqs.add(entry.fileSequenceNumber());
      actualSnapshotIds.add(entry.snapshotId());
      actualFiles.add(entry.file().path().toString());
    }

    assertSameElements("data seqs", actualDataSeqs, dataSeqs);
    assertSameElements("file seqs", actualFileSeqs, fileSeqs);
    assertSameElements("snapshot IDs", actualSnapshotIds, snapshotIds);
    assertSameElements("files", actualFiles, files);
  }

  private static <T> void assertSameElements(String context, List<T> actual, List<T> expected) {
    String errorMessage = String.format("%s must match", context);
    assertThat(actual).as(errorMessage).hasSameElementsAs(expected);
  }
}
