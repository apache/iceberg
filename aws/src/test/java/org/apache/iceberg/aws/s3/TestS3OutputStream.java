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
package org.apache.iceberg.aws.s3;

import static org.apache.iceberg.metrics.MetricsContext.nullMetrics;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.utils.BinaryUtils;

@RunWith(MockitoJUnitRunner.class)
public class TestS3OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(TestS3OutputStream.class);
  private static final String BUCKET = "test-bucket";
  private static final int FIVE_MBS = 5 * 1024 * 1024;

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  private final S3Client s3 = S3_MOCK_RULE.createS3ClientV2();
  private final S3Client s3mock = mock(S3Client.class, delegatesTo(s3));
  private final Random random = new Random(1);
  private final Path tmpDir = Files.createTempDirectory("s3fileio-test-");
  private final String newTmpDirectory = "/tmp/newStagingDirectory";

  private final AwsProperties properties =
      new AwsProperties(
          ImmutableMap.of(
              AwsProperties.S3FILEIO_MULTIPART_SIZE,
              Integer.toString(5 * 1024 * 1024),
              AwsProperties.S3FILEIO_STAGING_DIRECTORY,
              tmpDir.toString(),
              "s3.write.tags.abc",
              "123",
              "s3.write.tags.def",
              "789",
              "s3.delete.tags.xyz",
              "456"));

  public TestS3OutputStream() throws IOException {}

  @Before
  public void before() {
    properties.setS3ChecksumEnabled(false);
    s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
  }

  @After
  public void after() {
    File newStagingDirectory = new File(newTmpDirectory);
    if (newStagingDirectory.exists()) {
      newStagingDirectory.delete();
    }
  }

  @Test
  public void testWrite() {
    writeTest();
  }

  @Test
  public void testAbortAfterFailedPartUpload() {
    doThrow(new RuntimeException())
        .when(s3mock)
        .uploadPart((UploadPartRequest) any(), (RequestBody) any());

    try (S3OutputStream stream =
        new S3OutputStream(s3mock, randomURI(), properties, nullMetrics())) {
      stream.write(randomData(10 * 1024 * 1024));
    } catch (Exception e) {
      verify(s3mock, atLeastOnce()).abortMultipartUpload((AbortMultipartUploadRequest) any());
    }
  }

  @Test
  public void testAbortMultipart() {
    doThrow(new RuntimeException())
        .when(s3mock)
        .completeMultipartUpload((CompleteMultipartUploadRequest) any());

    try (S3OutputStream stream =
        new S3OutputStream(s3mock, randomURI(), properties, nullMetrics())) {
      stream.write(randomData(10 * 1024 * 1024));
    } catch (Exception e) {
      verify(s3mock).abortMultipartUpload((AbortMultipartUploadRequest) any());
    }
  }

  @Test
  public void testMultipleClose() throws IOException {
    S3OutputStream stream = new S3OutputStream(s3, randomURI(), properties, nullMetrics());
    stream.close();
    stream.close();
  }

  @Test
  public void testStagingDirectoryCreation() throws IOException {
    AwsProperties newStagingDirectoryAwsProperties =
        new AwsProperties(
            ImmutableMap.of(AwsProperties.S3FILEIO_STAGING_DIRECTORY, newTmpDirectory));
    S3OutputStream stream =
        new S3OutputStream(s3, randomURI(), newStagingDirectoryAwsProperties, nullMetrics());
    stream.close();
  }

  @Test
  public void testWriteWithChecksumEnabled() {
    properties.setS3ChecksumEnabled(true);
    writeTest();
  }

  private void writeTest() {
    // Run tests for both byte and array write paths
    Stream.of(true, false)
        .forEach(
            arrayWrite -> {
              // Test small file write (less than multipart threshold)
              byte[] data = randomData(1024);
              writeAndVerify(s3mock, randomURI(), data, arrayWrite);
              ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor =
                  ArgumentCaptor.forClass(PutObjectRequest.class);
              verify(s3mock, times(1))
                  .putObject(putObjectRequestArgumentCaptor.capture(), (RequestBody) any());
              checkPutObjectRequestContent(data, putObjectRequestArgumentCaptor);
              checkTags(putObjectRequestArgumentCaptor);
              reset(s3mock);

              // Test file larger than part size but less than multipart threshold
              data = randomData(6 * 1024 * 1024);
              writeAndVerify(s3mock, randomURI(), data, arrayWrite);
              putObjectRequestArgumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
              verify(s3mock, times(1))
                  .putObject(putObjectRequestArgumentCaptor.capture(), (RequestBody) any());
              checkPutObjectRequestContent(data, putObjectRequestArgumentCaptor);
              checkTags(putObjectRequestArgumentCaptor);
              reset(s3mock);

              // Test file large enough to trigger multipart upload
              data = randomData(10 * 1024 * 1024);
              writeAndVerify(s3mock, randomURI(), data, arrayWrite);
              ArgumentCaptor<UploadPartRequest> uploadPartRequestArgumentCaptor =
                  ArgumentCaptor.forClass(UploadPartRequest.class);
              verify(s3mock, times(2))
                  .uploadPart(uploadPartRequestArgumentCaptor.capture(), (RequestBody) any());
              checkUploadPartRequestContent(data, uploadPartRequestArgumentCaptor);
              reset(s3mock);

              // Test uploading many parts
              data = randomData(22 * 1024 * 1024);
              writeAndVerify(s3mock, randomURI(), data, arrayWrite);
              uploadPartRequestArgumentCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);
              verify(s3mock, times(5))
                  .uploadPart(uploadPartRequestArgumentCaptor.capture(), (RequestBody) any());
              checkUploadPartRequestContent(data, uploadPartRequestArgumentCaptor);
              reset(s3mock);
            });
  }

  private void checkUploadPartRequestContent(
      byte[] data, ArgumentCaptor<UploadPartRequest> uploadPartRequestArgumentCaptor) {
    if (properties.isS3ChecksumEnabled()) {
      List<UploadPartRequest> uploadPartRequests =
          uploadPartRequestArgumentCaptor.getAllValues().stream()
              .sorted(Comparator.comparingInt(UploadPartRequest::partNumber))
              .collect(Collectors.toList());
      for (int i = 0; i < uploadPartRequests.size(); ++i) {
        int offset = i * FIVE_MBS;
        int len = (i + 1) * FIVE_MBS - 1 > data.length ? data.length - offset : FIVE_MBS;
        assertEquals(getDigest(data, offset, len), uploadPartRequests.get(i).contentMD5());
      }
    }
  }

  private void checkPutObjectRequestContent(
      byte[] data, ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor) {
    if (properties.isS3ChecksumEnabled()) {
      List<PutObjectRequest> putObjectRequests = putObjectRequestArgumentCaptor.getAllValues();
      assertEquals(getDigest(data, 0, data.length), putObjectRequests.get(0).contentMD5());
    }
  }

  private void checkTags(ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor) {
    if (properties.isS3ChecksumEnabled()) {
      List<PutObjectRequest> putObjectRequests = putObjectRequestArgumentCaptor.getAllValues();
      String tagging = putObjectRequests.get(0).tagging();
      assertEquals(getTags(properties.s3WriteTags()), tagging);
    }
  }

  private String getTags(Set<Tag> objectTags) {
    return objectTags.stream().map(e -> e.key() + "=" + e.value()).collect(Collectors.joining("&"));
  }

  private String getDigest(byte[] data, int offset, int length) {
    try {
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      md5.update(data, offset, length);
      return BinaryUtils.toBase64(md5.digest());
    } catch (NoSuchAlgorithmException e) {
      fail(String.format("Failed to get MD5 MessageDigest. %s", e));
    }
    return null;
  }

  private void writeAndVerify(S3Client client, S3URI uri, byte[] data, boolean arrayWrite) {
    try (S3OutputStream stream = new S3OutputStream(client, uri, properties, nullMetrics())) {
      if (arrayWrite) {
        stream.write(data);
        assertEquals(data.length, stream.getPos());
      } else {
        for (int i = 0; i < data.length; i++) {
          stream.write(data[i]);
          assertEquals(i + 1, stream.getPos());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    byte[] actual = readS3Data(uri);
    assertArrayEquals(data, actual);

    // Verify all staging files are cleaned up
    try {
      assertEquals(0, Files.list(tmpDir).count());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private byte[] readS3Data(S3URI uri) {
    ResponseBytes<GetObjectResponse> data =
        s3.getObject(
            GetObjectRequest.builder().bucket(uri.bucket()).key(uri.key()).build(),
            ResponseTransformer.toBytes());

    return data.asByteArray();
  }

  private byte[] randomData(int size) {
    byte[] result = new byte[size];
    random.nextBytes(result);
    return result;
  }

  private S3URI randomURI() {
    return new S3URI(String.format("s3://%s/data/%s.dat", BUCKET, UUID.randomUUID()));
  }
}
