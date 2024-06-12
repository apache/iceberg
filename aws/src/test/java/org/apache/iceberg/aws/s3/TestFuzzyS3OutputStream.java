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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.net.ssl.SSLException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

public class TestFuzzyS3OutputStream extends TestS3OutputStream {

  public TestFuzzyS3OutputStream() throws IOException {}

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testWriteWithFuzzyClientRetrySucceed(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    S3FileIOProperties awsProperties = new S3FileIOProperties();
    awsProperties.setS3WriteRetryNumRetries(2);
    awsProperties.setS3WriteRetryMinWaitMs(100);
    byte[] data = randomData(10 * 1024 * 1024);
    S3Client s3 = mock(S3Client.class);
    S3Client s3Client = fuzzyClient(s3, new AtomicInteger(3), exception);
    testWrite(data, s3Client, randomURI(), awsProperties, nullMetrics());
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testWriteWithFuzzyClientRetryFailure(Exception exception, boolean shouldRetry)
      throws IOException {
    Assume.assumeTrue(shouldRetry);
    S3FileIOProperties awsProperties = new S3FileIOProperties();
    awsProperties.setS3WriteRetryNumRetries(1);
    awsProperties.setS3WriteRetryMinWaitMs(100);
    byte[] data = randomData(10 * 1024 * 1024);
    S3Client s3 = mock(S3Client.class);
    S3Client s3Client = fuzzyClient(s3, new AtomicInteger(3), exception);

    try {
      testWrite(data, s3Client, randomURI(), awsProperties, nullMetrics());
    } catch (Exception ex) {
      Assert.assertEquals(ex.getCause().getClass(), exception.getClass());
      Assert.assertEquals(ex.getCause().getMessage(), exception.getMessage());
    } finally {
      verify(s3Client, times(2)).putObject((PutObjectRequest) any(), (RequestBody) any());
    }
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testWriteExceptionShouldNotRetry(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeFalse(shouldRetry);
    S3FileIOProperties awsProperties = new S3FileIOProperties();
    awsProperties.setS3WriteRetryNumRetries(4);
    awsProperties.setS3WriteRetryMinWaitMs(100);
    byte[] data = randomData(10 * 1024 * 1024);
    S3Client s3 = mock(S3Client.class);
    S3Client s3Client = fuzzyClient(s3, new AtomicInteger(3), exception);

    try {
      testWrite(data, s3Client, randomURI(), awsProperties, nullMetrics());
    } catch (Exception ex) {
      Throwable causeException = ex.getCause() == null ? ex : ex.getCause();
      Assert.assertEquals(causeException.getClass(), exception.getClass());
      Assert.assertEquals(causeException.getMessage(), exception.getMessage());
    } finally {
      verify(s3Client, times(1)).putObject((PutObjectRequest) any(), (RequestBody) any());
    }
  }

  private S3Client fuzzyClient(S3Client s3Client, AtomicInteger counter, Exception failure) {
    S3Client fuzzyClient = spy(new TestFuzzyS3OutputStream.S3ClientWrapper(s3Client));
    int round = counter.get();
    // for every round of n invocations, only the last call succeeds
    doAnswer(
            invocation -> {
              if (counter.decrementAndGet() == 0) {
                counter.set(round);
                return invocation.callRealMethod();
              } else {
                throw failure;
              }
            })
        .when(fuzzyClient)
        .uploadPart(any(UploadPartRequest.class), any(RequestBody.class));

    doAnswer(
            invocation -> {
              if (counter.decrementAndGet() == 0) {
                counter.set(round);
                return invocation.callRealMethod();
              } else {
                throw failure;
              }
            })
        .when(fuzzyClient)
        .putObject(any(PutObjectRequest.class), any(RequestBody.class));

    return fuzzyClient;
  }

  private static Stream<Arguments> provideExceptionAndShouldRetry() {
    return Stream.of(
        Arguments.of(new IOException("random failure"), true),
        Arguments.of(new SSLException("client connection reset"), true),
        Arguments.of(new SocketTimeoutException("client connection reset"), true),
        Arguments.of(new EOFException("failure"), true),
        Arguments.of(
            AwsServiceException.builder().statusCode(403).message("failure").build(), false),
        Arguments.of(
            AwsServiceException.builder().statusCode(400).message("failure").build(), false),
        Arguments.of(S3Exception.builder().statusCode(404).message("failure").build(), false),
        Arguments.of(S3Exception.builder().statusCode(416).message("failure").build(), false),
        Arguments.of(
            SdkServiceException.builder().statusCode(403).message("Access Denied").build(), false));
  }

  /** Wrapper for S3 client, used to mock the final class DefaultS3Client */
  public static class S3ClientWrapper implements S3Client {

    private final S3Client delegate;

    public S3ClientWrapper(S3Client delegate) {
      this.delegate = delegate;
    }

    @Override
    public String serviceName() {
      return delegate.serviceName();
    }

    @Override
    public void close() {
      delegate.close();
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody)
        throws AwsServiceException, SdkClientException, S3Exception {
      return delegate.putObject(putObjectRequest, requestBody);
    }

    @Override
    public AbortMultipartUploadResponse abortMultipartUpload(
        AbortMultipartUploadRequest abortMultipartUploadRequest)
        throws AwsServiceException, SdkClientException {
      return delegate.abortMultipartUpload(abortMultipartUploadRequest);
    }
  }
}
