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
package org.apache.iceberg.gcp.gcs;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.stream.StreamSupport;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GCSFileIOTest {
  private static final String TEST_BUCKET = "TEST_BUCKET";
  private final Random random = new Random(1);

  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private GCSFileIO io;

  @Before
  public void before() {
    io = new GCSFileIO(() -> storage, new GCPProperties());
  }

  @Test
  public void newInputFile() throws IOException {
    String location = format("gs://%s/path/to/file.txt", TEST_BUCKET);
    byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    InputFile in = io.newInputFile(location);
    assertFalse(in.exists());

    OutputFile out = io.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtils.write(expected, os);
    }

    assertThat(in.exists()).isTrue();
    byte[] actual = new byte[1024 * 1024];

    try (InputStream is = in.newStream()) {
      IOUtils.readFully(is, actual);
    }

    assertThat(expected).isEqualTo(actual);

    io.deleteFile(in);

    assertThat(io.newInputFile(location).exists()).isFalse();
  }

  @Test
  public void testDelete() {
    String path = "delete/path/data.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path).build());

    // There should be one blob in the bucket
    assertThat(
            StreamSupport.stream(storage.list(TEST_BUCKET).iterateAll().spliterator(), false)
                .count())
        .isEqualTo(1);

    io.deleteFile(format("gs://%s/%s", TEST_BUCKET, path));

    // The bucket should now be empty
    assertThat(
            StreamSupport.stream(storage.list(TEST_BUCKET).iterateAll().spliterator(), false)
                .count())
        .isZero();
  }

  @Test
  public void testGCSFileIOKryoSerialization() throws IOException {
    FileIO testGCSFileIO = new GCSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testGCSFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testGCSFileIO);

    Assert.assertEquals(testGCSFileIO.properties(), roundTripSerializedFileIO.properties());
  }

  @Test
  public void testGCSFileIOJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testGCSFileIO = new GCSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testGCSFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testGCSFileIO);

    Assert.assertEquals(testGCSFileIO.properties(), roundTripSerializedFileIO.properties());
  }
}
