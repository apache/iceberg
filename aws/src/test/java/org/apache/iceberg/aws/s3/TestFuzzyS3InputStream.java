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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import dev.failsafe.FailsafeException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.net.ssl.SSLException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class TestFuzzyS3InputStream extends TestS3InputStream {

  private static final int DATA_SIZE = 3 * 1024 * 1024;

  @ParameterizedTest
  @MethodSource("retryableExceptions")
  public void testReadWithFuzzyStreamRetrySucceed(IOException exception) throws Exception {
    testRead(
        fuzzyStreamClient(new AtomicInteger(3), exception), new S3FileIOProperties(), DATA_SIZE);
  }

  @ParameterizedTest
  @MethodSource("retryableExceptions")
  public void testReadWithFuzzyStreamExhaustedRetries(IOException exception) {
    assertThatThrownBy(
            () ->
                testRead(
                    fuzzyStreamClient(new AtomicInteger(5), exception),
                    new S3FileIOProperties(),
                    DATA_SIZE))
        .isInstanceOf(FailsafeException.class)
        .hasCause(exception);
  }

  @ParameterizedTest
  @MethodSource("nonRetryableExceptions")
  public void testReadWithFuzzyStreamNonRetryableException(IOException exception) {
    assertThatThrownBy(
            () ->
                testRead(
                    fuzzyStreamClient(new AtomicInteger(3), exception),
                    new S3FileIOProperties(),
                    DATA_SIZE))
        .isInstanceOf(FailsafeException.class)
        .hasCause(exception);
  }

  private static Stream<Arguments> retryableExceptions() {
    return Stream.of(
        Arguments.of(
            new SocketTimeoutException("socket timeout exception"),
            new SSLException("some ssl exception")));
  }

  private static Stream<Arguments> nonRetryableExceptions() {
    return Stream.of(Arguments.of(new IOException("some generic non-retryable IO exception")));
  }

  private S3Client fuzzyStreamClient(AtomicInteger counter, IOException failure) {
    S3Client fuzzyClient = spy(new S3ClientWrapper(s3Client()));
    doAnswer(
            invocation ->
                new FuzzyResponseInputStream(invocation.callRealMethod(), counter, failure))
        .when(fuzzyClient)
        .getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
    return fuzzyClient;
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
    public <ReturnT> ReturnT getObject(
        GetObjectRequest getObjectRequest,
        ResponseTransformer<GetObjectResponse, ReturnT> responseTransformer)
        throws AwsServiceException, SdkClientException {
      return delegate.getObject(getObjectRequest, responseTransformer);
    }

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest)
        throws AwsServiceException, SdkClientException {
      return delegate.headObject(headObjectRequest);
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody)
        throws AwsServiceException, SdkClientException {
      return delegate.putObject(putObjectRequest, requestBody);
    }

    @Override
    public CreateBucketResponse createBucket(CreateBucketRequest createBucketRequest)
        throws AwsServiceException, SdkClientException {
      return delegate.createBucket(createBucketRequest);
    }
  }

  static class FuzzyResponseInputStream extends InputStream {

    private final ResponseInputStream<GetObjectResponse> delegate;
    private final AtomicInteger counter;
    private final int round;
    private final IOException exception;

    public FuzzyResponseInputStream(
        Object invocationResponse, AtomicInteger counter, IOException exception) {
      this.delegate = (ResponseInputStream<GetObjectResponse>) invocationResponse;
      this.counter = counter;
      this.round = counter.get();
      this.exception = exception;
    }

    private void checkCounter() throws IOException {
      // for every round of n invocations, only the last call succeeds
      if (counter.decrementAndGet() == 0) {
        counter.set(round);
      } else {
        throw exception;
      }
    }

    @Override
    public int read() throws IOException {
      checkCounter();
      return delegate.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
      checkCounter();
      return delegate.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      checkCounter();
      return delegate.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
      return delegate.skip(n);
    }

    @Override
    public int available() throws IOException {
      return delegate.available();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
      delegate.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
      delegate.reset();
    }

    @Override
    public boolean markSupported() {
      return delegate.markSupported();
    }
  }
}
