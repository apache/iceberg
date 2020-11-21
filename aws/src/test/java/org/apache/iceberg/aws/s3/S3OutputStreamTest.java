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

import com.adobe.testing.s3mock.junit4.S3MockRule;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
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
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@RunWith(MockitoJUnitRunner.class)
public class S3OutputStreamTest {
  private static final Logger LOG = LoggerFactory.getLogger(S3OutputStreamTest.class);
  private static final String BUCKET = "test-bucket";

  @ClassRule
  public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  private final S3Client s3 = S3_MOCK_RULE.createS3ClientV2();
  private final S3Client s3mock = mock(S3Client.class, delegatesTo(s3));
  private final Random random = new Random(1);
  private final Path tmpDir = Files.createTempDirectory("s3fileio-test-");

  private final AwsProperties properties = new AwsProperties(ImmutableMap.of(
      AwsProperties.S3FILEIO_MULTIPART_SIZE, Integer.toString(5 * 1024 * 1024),
      AwsProperties.S3FILEIO_STAGING_DIRECTORY, tmpDir.toString()));

  public S3OutputStreamTest() throws IOException {
  }

  @Before
  public void before() {
    s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
  }

  @Test
  public void testWrite() {
    // Run tests for both byte and array write paths
    Stream.of(true, false).forEach(arrayWrite -> {
      // Test small file write (less than multipart threshold)
      writeAndVerify(s3mock, randomURI(), randomData(1024), arrayWrite);
      verify(s3mock, times(1)).putObject((PutObjectRequest) any(), (RequestBody) any());
      reset(s3mock);

      // Test file larger than part size but less than multipart threshold
      writeAndVerify(s3mock, randomURI(), randomData(6 * 1024 * 1024), arrayWrite);
      verify(s3mock, times(1)).putObject((PutObjectRequest) any(), (RequestBody) any());
      reset(s3mock);

      // Test file large enough to trigger multipart upload
      writeAndVerify(s3mock, randomURI(), randomData(10 * 1024 * 1024), arrayWrite);
      verify(s3mock, times(2)).uploadPart((UploadPartRequest) any(), (RequestBody) any());
      reset(s3mock);

      // Test uploading many parts
      writeAndVerify(s3mock, randomURI(), randomData(22 * 1024 * 1024), arrayWrite);
      verify(s3mock, times(5)).uploadPart((UploadPartRequest) any(), (RequestBody) any());
      reset(s3mock);
    });
  }

  @Test
  public void testAbortAfterFailedPartUpload() {
    doThrow(new RuntimeException()).when(s3mock).uploadPart((UploadPartRequest) any(), (RequestBody) any());

    try (S3OutputStream stream = new S3OutputStream(s3mock, randomURI(), properties)) {
      stream.write(randomData(10 * 1024 * 1024));
    } catch (Exception e) {
      verify(s3mock, atLeastOnce()).abortMultipartUpload((AbortMultipartUploadRequest) any());
    }
  }

  @Test
  public void testAbortMultipart() {
    doThrow(new RuntimeException()).when(s3mock).completeMultipartUpload((CompleteMultipartUploadRequest) any());

    try (S3OutputStream stream = new S3OutputStream(s3mock, randomURI(), properties)) {
      stream.write(randomData(10 * 1024 * 1024));
    } catch (Exception e) {
      verify(s3mock).abortMultipartUpload((AbortMultipartUploadRequest) any());
    }
  }

  @Test
  public void testMultipleClose() throws IOException {
    S3OutputStream stream = new S3OutputStream(s3, randomURI(), properties);
    stream.close();
    stream.close();
  }

  private void writeAndVerify(S3Client client, S3URI uri, byte [] data, boolean arrayWrite) {
    try (S3OutputStream stream = new S3OutputStream(client, uri, properties)) {
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
        s3.getObject(GetObjectRequest.builder().bucket(uri.bucket()).key(uri.key()).build(),
        ResponseTransformer.toBytes());

    return data.asByteArray();
  }

  private byte[] randomData(int size) {
    byte [] result = new byte[size];
    random.nextBytes(result);
    return result;
  }

  private S3URI randomURI() {
    return new S3URI(String.format("s3://%s/data/%s.dat", BUCKET, UUID.randomUUID()));
  }
}
