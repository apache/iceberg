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
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestOSSOutputStream extends AliyunOSSTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestOSSOutputStream.class);
  private static final int PART_SIZE = 5 * 1024 * 1024;
  private static final Random RANDOM = ThreadLocalRandom.current();

  private final OSS ossClient = ossClient().get();
  private final OSS ossMock = mock(OSS.class, delegatesTo(ossClient));
  private final Path tmpDir = Files.createTempDirectory("oss-file-io-test-");

  private final AliyunProperties props =
      new AliyunProperties(
          ImmutableMap.of(AliyunProperties.OSS_STAGING_DIRECTORY, tmpDir.toString()));

  public TestOSSOutputStream() throws IOException {}

  // --- Small file tests using mock HTTP server ---

  @Test
  public void testWrite() throws IOException {
    OSSURI uri = randomURI();

    for (int i = 0; i < 2; i++) {
      boolean arrayWrite = i % 2 == 0;
      // Write small file.
      writeAndVerify(ossMock, uri, data256(), arrayWrite);
      verify(ossMock, times(1)).putObject(any());
      reset(ossMock);

      // Write large file.
      writeAndVerify(ossMock, uri, randomData(32 * 1024), arrayWrite);
      verify(ossMock, times(1)).putObject(any());
      reset(ossMock);
    }
  }

  private void writeAndVerify(OSS mock, OSSURI uri, byte[] data, boolean arrayWrite)
      throws IOException {
    LOG.info(
        "Write and verify for arguments uri: {}, data length: {}, arrayWrite: {}",
        uri,
        data.length,
        arrayWrite);

    try (OSSOutputStream out =
        new OSSOutputStream(mock, uri, props, MetricsContext.nullMetrics())) {
      if (arrayWrite) {
        out.write(data);
        assertThat(out.getPos()).as("OSSOutputStream position").isEqualTo(data.length);
      } else {
        for (int i = 0; i < data.length; i++) {
          out.write(data[i]);
          assertThat(out.getPos()).as("OSSOutputStream position").isEqualTo(i + 1);
        }
      }
    }

    assertThat(ossClient.doesObjectExist(uri.bucket(), uri.key()))
        .as("OSS object should exist")
        .isTrue();
    assertThat(ossClient.getObject(uri.bucket(), uri.key()).getObjectMetadata().getContentLength())
        .as("Object length")
        .isEqualTo(data.length);

    byte[] actual = ossDataContent(uri, data.length);
    assertThat(actual).as("Object content").isEqualTo(data);

    // Verify all staging files are cleaned up.
    assertThat(Files.list(Paths.get(props.ossStagingDirectory())).count())
        .as("Staging files should clean up")
        .isEqualTo(0);
  }

  // --- Multipart upload tests using pure Mockito (no HTTP server) ---

  @Test
  public void testSmallFileUsesPutObject() throws IOException {
    OSS client = multipartMockClient();

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.5)) {
      out.write(new byte[1024]);
    }

    verify(client).putObject(any(PutObjectRequest.class));
    verify(client, never()).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
  }

  @Test
  public void testEmptyFileSkipsUpload() throws IOException {
    OSS client = multipartMockClient();

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.5)) {
      // write nothing
    }

    verify(client, never()).putObject(any(PutObjectRequest.class));
    verify(client, never()).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
  }

  @Test
  public void testMultipartUpload() throws IOException {
    OSS client = multipartMockClient();
    // threshold = 5MB * 1.5 = 7.5MB, write 8MB to trigger multipart
    byte[] data = new byte[8 * 1024 * 1024];

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.5)) {
      out.write(data);
    }

    verify(client).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    verify(client, times(2)).uploadPart(any(UploadPartRequest.class));

    ArgumentCaptor<CompleteMultipartUploadRequest> captor =
        ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
    verify(client).completeMultipartUpload(captor.capture());
    assertThat(captor.getValue().getPartETags()).hasSize(2);
    verify(client, never()).putObject(any(PutObjectRequest.class));
  }

  @Test
  public void testWriteAcrossPartBoundary() throws IOException {
    OSS client = multipartMockClient();
    // Write 2.5 parts worth of data in one call
    byte[] data = new byte[PART_SIZE * 2 + PART_SIZE / 2];

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.0)) {
      out.write(data);
    }

    verify(client).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    verify(client, times(3)).uploadPart(any(UploadPartRequest.class));
    verify(client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void testMultipartAbortOnFailure() {
    OSS client = multipartMockClient();
    when(client.uploadPart(any(UploadPartRequest.class)))
        .thenThrow(new OSSException("upload failed"));

    byte[] data = new byte[PART_SIZE + 1];
    assertThatThrownBy(
            () -> {
              try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.0)) {
                out.write(data);
              }
            })
        .hasCauseInstanceOf(OSSException.class);

    verify(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
  }

  @Test
  public void testAbortFailurePreservesOriginalException() {
    OSS client = multipartMockClient();
    when(client.uploadPart(any(UploadPartRequest.class)))
        .thenThrow(new OSSException("upload failed"));
    when(client.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
        .thenThrow(new OSSException("abort failed"));

    byte[] data = new byte[PART_SIZE + 1];
    assertThatThrownBy(
            () -> {
              try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.0)) {
                out.write(data);
              }
            })
        .hasCauseInstanceOf(OSSException.class)
        .hasMessageContaining("upload failed");
  }

  @Test
  public void testPositionTracking() throws IOException {
    OSS client = multipartMockClient();

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.5)) {
      assertThat(out.getPos()).isEqualTo(0);

      out.write(42);
      assertThat(out.getPos()).isEqualTo(1);

      out.write(new byte[99]);
      assertThat(out.getPos()).isEqualTo(100);

      out.write(new byte[200], 10, 50);
      assertThat(out.getPos()).isEqualTo(150);
    }
  }

  @Test
  public void testSingleByteWriteTriggersMultipart() throws IOException {
    OSS client = multipartMockClient();

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.0)) {
      for (int i = 0; i < PART_SIZE + 1; i++) {
        out.write(0);
      }
    }

    verify(client).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    verify(client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void testMultipleCloseIsIdempotent() throws IOException {
    OSS client = multipartMockClient();

    OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.5);
    out.write(new byte[100]);
    out.close();
    out.close();

    verify(client, times(1)).putObject(any(PutObjectRequest.class));
  }

  @Test
  public void testPartNumbersAreSequential() throws IOException {
    OSS client = multipartMockClient();
    byte[] data = new byte[PART_SIZE * 3];

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.0)) {
      out.write(data);
    }

    ArgumentCaptor<UploadPartRequest> captor = ArgumentCaptor.forClass(UploadPartRequest.class);
    verify(client, times(3)).uploadPart(captor.capture());

    assertThat(captor.getAllValues())
        .extracting(UploadPartRequest::getPartNumber)
        .containsExactlyInAnyOrder(1, 2, 3);
  }

  @Test
  public void testUploadIdPassedCorrectly() throws IOException {
    OSS client = multipartMockClient();
    byte[] data = new byte[PART_SIZE + 1];

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.0)) {
      out.write(data);
    }

    ArgumentCaptor<UploadPartRequest> partCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);
    verify(client, times(2)).uploadPart(partCaptor.capture());
    assertThat(partCaptor.getAllValues()).allMatch(r -> "test-upload-id".equals(r.getUploadId()));

    ArgumentCaptor<CompleteMultipartUploadRequest> completeCaptor =
        ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
    verify(client).completeMultipartUpload(completeCaptor.capture());
    assertThat(completeCaptor.getValue().getUploadId()).isEqualTo("test-upload-id");
  }

  @Test
  public void testBucketAndKeyPassedCorrectly() throws IOException {
    OSS client = multipartMockClient();
    OSSURI uri = randomURI();

    try (OSSOutputStream out =
        new OSSOutputStream(
            client, uri, multipartProps(PART_SIZE, 1.5), MetricsContext.nullMetrics())) {
      out.write(new byte[100]);
    }

    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(client).putObject(captor.capture());
    assertThat(captor.getValue().getBucketName()).isEqualTo(uri.bucket());
    assertThat(captor.getValue().getKey()).isEqualTo(uri.key());
  }

  // --- Data integrity tests ---

  @Test
  public void testSmallFileDataIntegrity() throws IOException {
    OSS client = multipartMockClient();
    byte[] data = randomData(1024);

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.5)) {
      out.write(data);
    }

    assertPutObjectDataEquals(client, data);
  }

  @Test
  public void testFileBetweenPartSizeAndThresholdDataIntegrity() throws IOException {
    // partSize=5MB, threshold=5MB*1.5=7.5MB, write 6MB → 2 staging files, putObject path
    OSS client = multipartMockClient();
    int dataSize = PART_SIZE + PART_SIZE / 5;
    byte[] data = randomData(dataSize);

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.5)) {
      out.write(data);
    }

    verify(client, never()).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));

    assertPutObjectDataEquals(client, data);
  }

  @Test
  public void testMultipartDataIntegrity() throws IOException {
    // partSize=5MB, threshold=5MB*1.5=7.5MB, write 8MB → multipart
    OSS client = multipartMockClient();
    byte[] data = randomData(8 * 1024 * 1024);

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.5)) {
      out.write(data);
    }

    verify(client).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    assertMultipartDataEquals(client, 2, data);
  }

  @Test
  public void testExactPartSizeBoundaryDataIntegrity() throws IOException {
    // Write exactly 2 * partSize → 2 full parts via multipart (threshold=1.0)
    OSS client = multipartMockClient();
    byte[] data = randomData(PART_SIZE * 2);

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.0)) {
      out.write(data);
    }

    assertMultipartDataEquals(client, 2, data);
  }

  @Test
  public void testSingleByteWriteDataIntegrity() throws IOException {
    // Write byte-by-byte across part boundary, verify data still intact
    OSS client = multipartMockClient();
    int dataSize = PART_SIZE + 100;
    byte[] data = randomData(dataSize);

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.0)) {
      for (byte b : data) {
        out.write(b);
      }
    }

    assertMultipartDataEquals(client, 2, data);
  }

  // --- Boundary tests ---

  @Test
  public void testExactPartSizeUsesPutObject() throws IOException {
    // partSize=5MB, threshold=5MB*1.5=7.5MB, write exactly 5MB → 1 staging file, putObject
    OSS client = multipartMockClient();
    byte[] data = randomData(PART_SIZE);

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.5)) {
      out.write(data);
    }

    verify(client, never()).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));

    assertPutObjectDataEquals(client, data);
  }

  @Test
  public void testExactThresholdTriggersMultipart() throws IOException {
    // partSize=5MB, threshold=5MB*1.5=7.5MB, write exactly 7.5MB → multipart
    OSS client = multipartMockClient();
    int thresholdSize = (int) (PART_SIZE * 1.5);
    byte[] data = randomData(thresholdSize);

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.5)) {
      out.write(data);
    }

    verify(client).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    assertMultipartDataEquals(client, 2, data);
  }

  @Test
  public void testManyPartsDataIntegrity() throws IOException {
    // Write 5 parts worth of data, verify all parts have correct content
    OSS client = multipartMockClient();
    byte[] data = randomData(PART_SIZE * 5);

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.0)) {
      out.write(data);
    }

    assertMultipartDataEquals(client, 5, data);
  }

  // --- Write method tests ---

  @Test
  public void testWriteWithOffsetDataIntegrity() throws IOException {
    // write(byte[], off, len) with non-zero offset
    OSS client = multipartMockClient();
    byte[] buffer = randomData(2048);
    int offset = 100;
    int length = 1024;

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.5)) {
      out.write(buffer, offset, length);
      assertThat(out.getPos()).isEqualTo(length);
    }

    byte[] expected = new byte[length];
    System.arraycopy(buffer, offset, expected, 0, length);
    assertPutObjectDataEquals(client, expected);
  }

  @Test
  public void testMixedWriteDataIntegrity() throws IOException {
    // Mix array writes and single-byte writes across part boundary
    OSS client = multipartMockClient();
    ByteArrayOutputStream expectedStream = new ByteArrayOutputStream();

    try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.0)) {
      // Array write: first half of partSize
      byte[] chunk1 = randomData(PART_SIZE / 2);
      out.write(chunk1);
      expectedStream.write(chunk1);

      // Single-byte writes: fill the rest of first part + overflow into second
      byte[] chunk2 = randomData(PART_SIZE / 2 + 200);
      for (byte b : chunk2) {
        out.write(b);
      }
      expectedStream.write(chunk2);

      // Array write with offset: add more data
      byte[] chunk3 = randomData(500);
      out.write(chunk3, 50, 300);
      expectedStream.write(chunk3, 50, 300);
    }

    byte[] expected = expectedStream.toByteArray();

    assertMultipartDataEquals(client, 2, expected);
  }

  // --- Staging directory and cleanup tests ---

  @Test
  public void testStagingDirectoryAutoCreated() throws IOException {
    OSS client = multipartMockClient();
    Path nestedDir = tmpDir.resolve("sub1").resolve("sub2");

    AliyunProperties nestedProps =
        new AliyunProperties(
            ImmutableMap.of(
                AliyunProperties.OSS_STAGING_DIRECTORY,
                nestedDir.toString(),
                AliyunProperties.OSS_MULTIPART_SIZE,
                Integer.toString(PART_SIZE),
                AliyunProperties.OSS_MULTIPART_THRESHOLD_FACTOR,
                "1.5",
                AliyunProperties.OSS_MULTIPART_UPLOAD_THREADS,
                "2"));

    assertThat(Files.exists(nestedDir)).isFalse();

    try (OSSOutputStream out =
        new OSSOutputStream(
            client,
            new OSSURI("oss://test-bucket/test-key"),
            nestedProps,
            MetricsContext.nullMetrics())) {
      out.write(new byte[100]);
    }

    assertThat(Files.exists(nestedDir)).isTrue();
    // cleanup
    Files.delete(nestedDir);
    Files.delete(nestedDir.getParent());
  }

  @Test
  public void testStagingFilesCleanedUpAfterFailure() throws IOException {
    OSS client = multipartMockClient();
    when(client.uploadPart(any(UploadPartRequest.class)))
        .thenThrow(new OSSException("upload failed"));

    byte[] data = new byte[PART_SIZE + 1];

    try {
      try (OSSOutputStream out = newOutputStream(client, PART_SIZE, 1.0)) {
        out.write(data);
      }
    } catch (Exception e) {
      // expected
    }

    assertThat(Files.list(tmpDir).filter(p -> p.toString().contains("oss-file-io-")).count())
        .as("Staging files should be cleaned up after failure")
        .isEqualTo(0);
  }

  // --- Helpers ---

  private OSSURI randomURI() {
    return new OSSURI(location(String.format("%s.dat", UUID.randomUUID())));
  }

  private byte[] data256() {
    byte[] data = new byte[256];
    for (int i = 0; i < 256; i++) {
      data[i] = (byte) i;
    }
    return data;
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    RANDOM.nextBytes(data);
    return data;
  }

  private byte[] ossDataContent(OSSURI uri, int dataSize) throws IOException {
    try (InputStream is = ossClient.getObject(uri.bucket(), uri.key()).getObjectContent()) {
      byte[] actual = new byte[dataSize];
      ByteStreams.readFully(is, actual);
      return actual;
    }
  }

  private static void assertPutObjectDataEquals(OSS client, byte[] expected) throws IOException {
    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(client).putObject(captor.capture());
    byte[] uploaded = readAllBytes(captor.getValue().getInputStream());
    assertThat(uploaded).hasSize(expected.length).isEqualTo(expected);
  }

  private static void assertMultipartDataEquals(OSS client, int expectedParts, byte[] expected)
      throws IOException {
    ArgumentCaptor<UploadPartRequest> captor = ArgumentCaptor.forClass(UploadPartRequest.class);
    verify(client, times(expectedParts)).uploadPart(captor.capture());

    List<UploadPartRequest> parts = captor.getAllValues();
    parts.sort(Comparator.comparingInt(UploadPartRequest::getPartNumber));
    ByteArrayOutputStream combined = new ByteArrayOutputStream();
    for (UploadPartRequest req : parts) {
      combined.write(readAllBytes(req.getInputStream()));
    }

    assertThat(combined.size()).isEqualTo(expected.length);
    assertThat(combined.toByteArray()).isEqualTo(expected);
  }

  private static byte[] readAllBytes(InputStream in) throws IOException {
    try (in) {
      return ByteStreams.toByteArray(in);
    }
  }

  private AliyunProperties multipartProps(int partSize, double thresholdFactor) {
    return new AliyunProperties(
        ImmutableMap.of(
            AliyunProperties.OSS_STAGING_DIRECTORY,
            tmpDir.toString(),
            AliyunProperties.OSS_MULTIPART_SIZE,
            Integer.toString(partSize),
            AliyunProperties.OSS_MULTIPART_THRESHOLD_FACTOR,
            Double.toString(thresholdFactor),
            AliyunProperties.OSS_MULTIPART_UPLOAD_THREADS,
            "2"));
  }

  private OSSOutputStream newOutputStream(OSS client, int partSize, double thresholdFactor)
      throws IOException {
    return new OSSOutputStream(
        client,
        new OSSURI("oss://test-bucket/test-key"),
        multipartProps(partSize, thresholdFactor),
        MetricsContext.nullMetrics());
  }

  private static OSS multipartMockClient() {
    OSS client = mock(OSS.class);

    InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
    initResult.setUploadId("test-upload-id");
    when(client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
        .thenReturn(initResult);

    AtomicInteger partCounter = new AtomicInteger(0);
    when(client.uploadPart(any(UploadPartRequest.class)))
        .thenAnswer(
            invocation -> {
              UploadPartRequest req = invocation.getArgument(0);
              UploadPartResult result = new UploadPartResult();
              result.setPartNumber(req.getPartNumber());
              result.setETag("etag-" + partCounter.incrementAndGet());
              return result;
            });

    when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenReturn(new CompleteMultipartUploadResult());
    when(client.putObject(any(PutObjectRequest.class))).thenReturn(new PutObjectResult());

    return client;
  }
}
