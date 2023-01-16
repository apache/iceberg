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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.Test;

public class GCSInputStreamTest {

  private final Random random = new Random(1);

  private final GCPProperties gcpProperties = new GCPProperties();
  private final Storage storage = LocalStorageHelper.getOptions().getService();

  @Test
  public void testRead() throws Exception {
    BlobId uri = BlobId.fromGsUtilUri("gs://bucket/path/to/read.dat");
    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);

    writeGCSData(uri, data);

    try (SeekableInputStream in =
        new GCSInputStream(storage, uri, gcpProperties, MetricsContext.nullMetrics())) {
      int readSize = 1024;
      byte[] actual = new byte[readSize];

      readAndCheck(in, in.getPos(), readSize, data, false);
      readAndCheck(in, in.getPos(), readSize, data, true);

      // Seek forward in current stream
      int seekSize = 1024;
      readAndCheck(in, in.getPos() + seekSize, readSize, data, false);
      readAndCheck(in, in.getPos() + seekSize, readSize, data, true);

      // Buffered read
      readAndCheck(in, in.getPos(), readSize, data, true);
      readAndCheck(in, in.getPos(), readSize, data, false);

      // Seek with new stream
      long seekNewStreamPosition = 2 * 1024 * 1024;
      readAndCheck(in, in.getPos() + seekNewStreamPosition, readSize, data, true);
      readAndCheck(in, in.getPos() + seekNewStreamPosition, readSize, data, false);

      // Backseek and read
      readAndCheck(in, 0, readSize, data, true);
      readAndCheck(in, 0, readSize, data, false);
    }
  }

  private void readAndCheck(
      SeekableInputStream in, long rangeStart, int size, byte[] original, boolean buffered)
      throws IOException {
    in.seek(rangeStart);
    assertEquals(rangeStart, in.getPos());

    long rangeEnd = rangeStart + size;
    byte[] actual = new byte[size];

    if (buffered) {
      IOUtils.readFully(in, actual);
    } else {
      int read = 0;
      while (read < size) {
        actual[read++] = (byte) in.read();
      }
    }

    assertEquals(rangeEnd, in.getPos());
    assertArrayEquals(Arrays.copyOfRange(original, (int) rangeStart, (int) rangeEnd), actual);
  }

  @Test
  public void testClose() throws Exception {
    BlobId blobId = BlobId.fromGsUtilUri("gs://bucket/path/to/closed.dat");
    SeekableInputStream closed =
        new GCSInputStream(storage, blobId, gcpProperties, MetricsContext.nullMetrics());
    closed.close();
    assertThrows(IllegalStateException.class, () -> closed.seek(0));
  }

  @Test
  public void testSeek() throws Exception {
    BlobId blobId = BlobId.fromGsUtilUri("gs://bucket/path/to/seek.dat");
    byte[] data = randomData(1024 * 1024);

    writeGCSData(blobId, data);

    try (SeekableInputStream in =
        new GCSInputStream(storage, blobId, gcpProperties, MetricsContext.nullMetrics())) {
      in.seek(data.length / 2);
      byte[] actual = new byte[data.length / 2];

      IOUtils.readFully(in, actual, 0, data.length / 2);

      byte[] expected = Arrays.copyOfRange(data, data.length / 2, data.length);
      assertArrayEquals(expected, actual);
    }
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private void writeGCSData(BlobId blobId, byte[] data) throws IOException {
    storage.createFrom(BlobInfo.newBuilder(blobId).build(), new ByteArrayInputStream(data));
  }
}
