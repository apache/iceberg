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
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.io.InputFile;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Assume;
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
    validateManifestListCompressionCodec(
        compressionCodec -> {
          ManifestFile manifest = writeManifest(SNAPSHOT_ID, compressionCodec, FILE_A);
          return writeManifestList(compressionCodec, manifest);
        });
  }

  @Test
  public void testWriteDeleteManifestListWithCompression() throws IOException {
    Assume.assumeThat(formatVersion, Matchers.is(2));
    validateManifestListCompressionCodec(
        compressionCodec -> {
          ManifestFile manifest = writeManifest(SNAPSHOT_ID, compressionCodec, FILE_A);
          ManifestFile deleteManifest =
              writeDeleteManifest(formatVersion, SNAPSHOT_ID, compressionCodec, FILE_A_DELETES);
          return writeManifestList(compressionCodec, manifest, deleteManifest);
        });
  }

  void validateManifestListCompressionCodec(
      CheckedFunction<String, InputFile> createManifestListFunc) throws IOException {
    for (Map.Entry<String, String> entry : CODEC_METADATA_MAPPING.entrySet()) {
      String codec = entry.getKey();
      String expectedCodecValue = entry.getValue();

      InputFile manifestList = createManifestListFunc.apply(codec);
      try (AvroIterable<ManifestFile> reader = ManifestLists.manifestFileIterable(manifestList)) {
        Map<String, String> metadata = reader.getMetadata();
        Assert.assertEquals(
            "Manifest list codec value must match",
            expectedCodecValue,
            metadata.get(AVRO_CODEC_KEY));
      }
    }
  }
}
