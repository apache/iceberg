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
package org.apache.iceberg.aliyun.oss;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;

public class TestOSSInputStream extends AliyunOSSTestBase {
  private final Random random = ThreadLocalRandom.current();

  @Test
  public void testRead() throws Exception {
    OSSURI uri = new OSSURI(location("read.dat"));
    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);

    writeOSSData(uri, data);

    try (SeekableInputStream in = new OSSInputStream(ossClient().get(), uri)) {
      int readSize = 1024;

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
    assertThat(in.getPos()).as("Should have the correct position").isEqualTo(rangeStart);

    long rangeEnd = rangeStart + size;
    byte[] actual = new byte[size];

    if (buffered) {
      ByteStreams.readFully(in, actual);
    } else {
      int read = 0;
      while (read < size) {
        actual[read++] = (byte) in.read();
      }
    }

    assertThat(in.getPos()).as("Should have the correct position").isEqualTo(rangeEnd);

    assertThat(actual)
        .as("Should have expected range data")
        .isEqualTo(Arrays.copyOfRange(original, (int) rangeStart, (int) rangeEnd));
  }

  @Test
  public void testClose() throws Exception {
    OSSURI uri = new OSSURI(location("closed.dat"));
    SeekableInputStream closed = new OSSInputStream(ossClient().get(), uri);
    closed.close();
    assertThatThrownBy(() -> closed.seek(0))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot seek: already closed");
  }

  @Test
  public void testSeek() throws Exception {
    OSSURI uri = new OSSURI(location("seek.dat"));
    byte[] expected = randomData(1024 * 1024);

    writeOSSData(uri, expected);

    try (SeekableInputStream in = new OSSInputStream(ossClient().get(), uri)) {
      in.seek(expected.length / 2);
      byte[] actual = new byte[expected.length / 2];
      ByteStreams.readFully(in, actual);
      assertThat(actual)
          .as("Should have expected seeking stream")
          .isEqualTo(Arrays.copyOfRange(expected, expected.length / 2, expected.length));
    }
  }

  @Test
  public void testReadFully() throws Exception {
    int dataSize = 1024 * 1024;
    byte[] data = randomData(dataSize);
    OSSInputStream in = newInputStream(data);

    int offset = 100;
    int length = 256;
    byte[] buffer = new byte[length];
    try (in) {
      ((RangeReadable) in).readFully(offset, buffer, 0, length);
    }

    assertThat(buffer).isEqualTo(Arrays.copyOfRange(data, offset, offset + length));
  }

  @Test
  public void testReadTail() throws Exception {
    int dataSize = 1024 * 1024;
    byte[] data = randomData(dataSize);
    OSSInputStream in = newInputStream(data);

    int length = 256;
    byte[] buffer = new byte[length];
    int bytesRead;
    try (in) {
      bytesRead = ((RangeReadable) in).readTail(buffer, 0, length);
    }

    assertThat(bytesRead).isEqualTo(length);
    assertThat(buffer).isEqualTo(Arrays.copyOfRange(data, dataSize - length, dataSize));
  }

  @Test
  public void testReadTailSmallFile() throws Exception {
    int dataSize = 100;
    byte[] data = randomData(dataSize);
    OSSInputStream in = newInputStream(data);

    int length = 256;
    byte[] buffer = new byte[length];
    int bytesRead;
    try (in) {
      bytesRead = ((RangeReadable) in).readTail(buffer, 0, length);
    }

    assertThat(bytesRead).isEqualTo(dataSize);
    assertThat(Arrays.copyOfRange(buffer, 0, bytesRead)).isEqualTo(data);
  }

  @Test
  public void testReadFullyDoesNotAffectStreamPosition() throws Exception {
    int dataSize = 1024 * 1024;
    byte[] data = randomData(dataSize);
    OSSInputStream in = newInputStream(data);

    try (in) {
      // read some bytes to advance position
      int readSize = 128;
      byte[] buf = new byte[readSize];
      ByteStreams.readFully(in, buf);
      long posBeforeRangeRead = in.getPos();
      assertThat(posBeforeRangeRead).isEqualTo(readSize);

      // readFully at a different position
      byte[] rangeBuffer = new byte[64];
      ((RangeReadable) in).readFully(5000, rangeBuffer, 0, 64);

      // stream position unchanged
      assertThat(in.getPos()).isEqualTo(posBeforeRangeRead);

      // subsequent sequential read continues from original position
      byte[] nextBuf = new byte[readSize];
      ByteStreams.readFully(in, nextBuf);
      assertThat(nextBuf)
          .isEqualTo(Arrays.copyOfRange(data, (int) posBeforeRangeRead, readSize * 2));
    }
  }

  @Test
  public void testSingleByteReadAtEOF() throws Exception {
    int dataSize = 128;
    byte[] data = randomData(dataSize);
    OSSInputStream in = newInputStream(data);

    try (in) {
      in.seek(dataSize);
      long posBefore = in.getPos();
      assertThat(in.read()).isEqualTo(-1);
      assertThat(in.getPos()).isEqualTo(posBefore);
    }
  }

  @Test
  public void testBulkReadAtEOF() throws Exception {
    int dataSize = 128;
    byte[] data = randomData(dataSize);
    OSSInputStream in = newInputStream(data);

    try (in) {
      in.seek(dataSize);
      long posBefore = in.getPos();
      byte[] buf = new byte[64];
      assertThat(in.read(buf, 0, buf.length)).isEqualTo(-1);
      assertThat(in.getPos()).isEqualTo(posBefore);
    }
  }

  @Test
  public void testReadAfterClose() throws Exception {
    OSSInputStream in = newInputStream(randomData(128));
    in.close();

    assertThatThrownBy(() -> in.read())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot read: already closed");
  }

  @Test
  public void testSeekNegativePosition() throws Exception {
    try (OSSInputStream in = newInputStream(randomData(128))) {
      assertThatThrownBy(() -> in.seek(-1))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Position is negative");
    }
  }

  @Test
  public void testRetryOnReadFailure() throws Exception {
    byte[] data = randomData(256);
    OSSURI uri = randomURI();

    OSSObject failObject = mockOSSObject(socketTimeoutStream());
    OSSObject goodObject = mockOSSObject(new ByteArrayInputStream(data));

    OSS mockClient = mock(OSS.class);
    when(mockClient.getObject(any(GetObjectRequest.class)))
        .thenReturn(failObject)
        .thenReturn(goodObject);

    try (OSSInputStream in = new OSSInputStream(mockClient, uri)) {
      byte[] buf = new byte[data.length];
      ByteStreams.readFully(in, buf);
      assertThat(buf).isEqualTo(data);
    }

    verify(mockClient, times(2)).getObject(any(GetObjectRequest.class));
  }

  @Test
  public void testSkipFailureFallback() throws Exception {
    byte[] data = randomData(1024);
    int readSize = 64;
    OSSURI uri = randomURI();

    InputStream failOnSkipStream =
        new FilterInputStream(new ByteArrayInputStream(data)) {
          @Override
          public long skip(long n) throws IOException {
            throw new IOException("skip failed");
          }
        };

    OSSObject firstObject = mockOSSObject(failOnSkipStream);
    OSSObject secondObject =
        mockOSSObject(
            new ByteArrayInputStream(Arrays.copyOfRange(data, readSize + 1, data.length)));

    OSS mockClient = mock(OSS.class);
    when(mockClient.getObject(any(GetObjectRequest.class)))
        .thenReturn(firstObject)
        .thenReturn(secondObject);

    try (OSSInputStream in = new OSSInputStream(mockClient, uri)) {
      byte[] buf = new byte[readSize];
      ByteStreams.readFully(in, buf);
      assertThat(buf).isEqualTo(Arrays.copyOfRange(data, 0, readSize));

      // small forward seek triggers skip, which fails and falls back to new stream
      in.seek(readSize + 1);
      assertThat(in.read()).isEqualTo(Byte.toUnsignedInt(data[readSize + 1]));
    }
  }

  @Test
  public void testCloseStreamQuietlyOnRetry() throws Exception {
    byte[] data = randomData(256);
    OSSURI uri = randomURI();

    OSSObject failObject = mockOSSObject(socketTimeoutStream());
    doThrow(new IOException("close failed")).when(failObject).close();

    OSSObject goodObject = mockOSSObject(new ByteArrayInputStream(data));

    OSS mockClient = mock(OSS.class);
    when(mockClient.getObject(any(GetObjectRequest.class)))
        .thenReturn(failObject)
        .thenReturn(goodObject);

    // close() throws during retry but closeQuietly=true swallows it
    try (OSSInputStream in = new OSSInputStream(mockClient, uri)) {
      byte[] buf = new byte[data.length];
      ByteStreams.readFully(in, buf);
      assertThat(buf).isEqualTo(data);
    }
  }

  @Test
  public void testSetSkipSize() throws Exception {
    byte[] data = randomData(1024);
    int readSize = 64;
    OSSURI uri = randomURI();

    InputStream noAvailableStream =
        new FilterInputStream(new ByteArrayInputStream(data)) {
          @Override
          public int available() {
            return 0;
          }
        };

    OSSObject firstObject = mockOSSObject(noAvailableStream);
    OSSObject secondObject =
        mockOSSObject(
            new ByteArrayInputStream(Arrays.copyOfRange(data, readSize + 1, data.length)));

    OSS mockClient = mock(OSS.class);
    when(mockClient.getObject(any(GetObjectRequest.class)))
        .thenReturn(firstObject)
        .thenReturn(secondObject);

    try (OSSInputStream in = new OSSInputStream(mockClient, uri)) {
      in.setSkipSize(0);
      byte[] buf = new byte[readSize];
      ByteStreams.readFully(in, buf);
      assertThat(buf).isEqualTo(Arrays.copyOfRange(data, 0, readSize));

      // skipSize=0 and available()=0 force new stream open for any forward seek
      in.seek(readSize + 1);
      assertThat(in.read()).isEqualTo(Byte.toUnsignedInt(data[readSize + 1]));
      verify(mockClient, times(2)).getObject(any(GetObjectRequest.class));
    }
  }

  @Test
  public void testRetryExhausted() throws Exception {
    OSSURI uri = randomURI();
    OSSObject failObject = mockOSSObject(socketTimeoutStream());

    OSS mockClient = mock(OSS.class);
    when(mockClient.getObject(any(GetObjectRequest.class))).thenReturn(failObject);

    try (OSSInputStream in = new OSSInputStream(mockClient, uri)) {
      // single-byte read exhausts all retries
      assertThatThrownBy(() -> in.read()).isInstanceOf(SocketTimeoutException.class);

      // bulk read also exhausts all retries
      byte[] buf = new byte[64];
      assertThatThrownBy(() -> in.read(buf, 0, buf.length))
          .isInstanceOf(SocketTimeoutException.class);
    }
  }

  @Test
  public void testForcedCloseException() throws Exception {
    byte[] data = randomData(128);
    OSSURI uri = randomURI();

    OSSObject object = mockOSSObject(new ByteArrayInputStream(data));
    doThrow(new IOException("forcedClose failed")).when(object).forcedClose();

    OSS mockClient = mock(OSS.class);
    when(mockClient.getObject(any(GetObjectRequest.class))).thenReturn(object);

    // forcedClose failure is swallowed, close succeeds
    try (OSSInputStream in = new OSSInputStream(mockClient, uri)) {
      assertThat(in.read()).isEqualTo(Byte.toUnsignedInt(data[0]));
    }
  }

  @Test
  public void testCloseThrowsInNonQuietMode() throws Exception {
    byte[] data = randomData(128);
    OSSURI uri = randomURI();

    OSSObject object = mockOSSObject(new ByteArrayInputStream(data));
    doThrow(new IOException("close failed")).when(object).close();

    OSS mockClient = mock(OSS.class);
    when(mockClient.getObject(any(GetObjectRequest.class))).thenReturn(object);

    OSSInputStream in = new OSSInputStream(mockClient, uri);
    in.read();
    assertThatThrownBy(in::close).isInstanceOf(IOException.class).hasMessage("close failed");
  }

  private OSSURI randomURI() {
    return new OSSURI(location(UUID.randomUUID() + ".dat"));
  }

  private OSSInputStream newInputStream(byte[] data) {
    OSSURI uri = randomURI();
    writeOSSData(uri, data);
    return new OSSInputStream(ossClient().get(), uri);
  }

  private static OSSObject mockOSSObject(InputStream stream) {
    OSSObject object = mock(OSSObject.class);
    when(object.getObjectContent()).thenReturn(stream);
    return object;
  }

  private static InputStream socketTimeoutStream() {
    return new InputStream() {
      @Override
      public int read() throws IOException {
        throw new SocketTimeoutException("timeout");
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        throw new SocketTimeoutException("timeout");
      }
    };
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private void writeOSSData(OSSURI uri, byte[] data) {
    ossClient().get().putObject(uri.bucket(), uri.key(), new ByteArrayInputStream(data));
  }
}
