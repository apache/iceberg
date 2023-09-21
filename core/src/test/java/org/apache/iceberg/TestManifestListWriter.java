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
import org.apache.avro.file.DataFileConstants;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.io.InputFile;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestManifestListWriter extends TableTestBase {

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestManifestListWriter(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testWriteManifestListWithCompression() throws IOException {
    validateManifestListCompressionCodec(false);
  }

  @Test
  public void testWriteDeleteManifestListWithCompression() throws IOException {
    Assumptions.assumeThat(formatVersion).isGreaterThan(1);
    validateManifestListCompressionCodec(true);
  }

  void validateManifestListCompressionCodec(boolean testDeletes) throws IOException {
    for (Map.Entry<String, String> entry : AVRO_CODEC_NAME_MAPPING.entrySet()) {
      String codec = entry.getKey();
      String expectedCodecValue = entry.getValue();

      ManifestFile manifest = writeManifest(EXAMPLE_SNAPSHOT_ID, codec, FILE_A);
      ManifestFile deleteManifest =
          testDeletes
              ? writeDeleteManifest(formatVersion, EXAMPLE_SNAPSHOT_ID, codec, FILE_A_DELETES)
              : null;
      InputFile manifestList =
          testDeletes
              ? writeManifestList(codec, manifest, deleteManifest)
              : writeManifestList(codec, manifest);
      try (AvroIterable<ManifestFile> reader = ManifestLists.manifestFileIterable(manifestList)) {
        Assertions.assertThat(reader.getMetadata())
            .containsEntry(DataFileConstants.CODEC, expectedCodecValue);
      }
    }
  }
}
