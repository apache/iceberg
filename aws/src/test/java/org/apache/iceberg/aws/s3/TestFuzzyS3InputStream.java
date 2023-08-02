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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import javax.net.ssl.SSLException;
import org.apache.iceberg.AssertHelpers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class TestFuzzyS3InputStream extends TestS3InputStream {

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testReadWithFuzzyClientRetrySucceed(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    S3FileIOProperties awsProperties = new S3FileIOProperties();
    awsProperties.setS3ReadRetryNumRetries(4);
    awsProperties.setS3ReadRetryMinWaitMs(100);
    testRead(fuzzyClient(new AtomicInteger(3), exception), awsProperties);
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testReadWithFuzzyStreamRetrySucceed(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    S3FileIOProperties awsProperties = new S3FileIOProperties();
    awsProperties.setS3ReadRetryNumRetries(4);
    awsProperties.setS3ReadRetryMinWaitMs(100);
    testRead(fuzzyStreamClient(new AtomicInteger(3), (IOException) exception), awsProperties);
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testReadWithFuzzyClientRetryFail(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    testRetryFail(
        (counter, awsProperties) -> {
          try {
            testRead(fuzzyClient(counter, exception), awsProperties);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        exception,
        shouldRetry);
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testReadWithFuzzyStreamRetryFail(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    testRetryFail(
        (counter, awsProperties) -> {
          try {
            testRead(fuzzyStreamClient(counter, (IOException) exception), awsProperties);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        exception,
        shouldRetry);
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testSeekWithFuzzyClientRetrySucceed(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    S3FileIOProperties awsProperties = new S3FileIOProperties();
    awsProperties.setS3ReadRetryNumRetries(4);
    awsProperties.setS3ReadRetryMinWaitMs(100);
    testSeek(fuzzyClient(new AtomicInteger(3), exception), awsProperties);
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testSeekWithFuzzyStreamRetrySucceed(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    S3FileIOProperties awsProperties = new S3FileIOProperties();
    awsProperties.setS3ReadRetryNumRetries(4);
    awsProperties.setS3ReadRetryMinWaitMs(100);
    testSeek(fuzzyStreamClient(new AtomicInteger(3), (IOException) exception), awsProperties);
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testSeekWithFuzzyClientRetryFail(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    testRetryFail(
        (counter, awsProperties) -> {
          try {
            testSeek(fuzzyClient(counter, exception), awsProperties);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        exception,
        shouldRetry);
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testSeekWithFuzzyStreamRetryFail(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    testRetryFail(
        (counter, awsProperties) -> {
          try {
            testSeek(fuzzyStreamClient(counter, (IOException) exception), awsProperties);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        exception,
        shouldRetry);
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testRangeReadWithFuzzyClientRetrySucceed(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    S3FileIOProperties awsProperties = new S3FileIOProperties();
    awsProperties.setS3ReadRetryNumRetries(4);
    awsProperties.setS3ReadRetryMinWaitMs(100);
    testRangeRead(fuzzyClient(new AtomicInteger(3), exception), awsProperties);
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testRangeReadWithFuzzyStreamRetrySucceed(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    S3FileIOProperties awsProperties = new S3FileIOProperties();
    awsProperties.setS3ReadRetryNumRetries(4);
    awsProperties.setS3ReadRetryMinWaitMs(100);
    testRangeRead(fuzzyStreamClient(new AtomicInteger(3), (IOException) exception), awsProperties);
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testRangeReadWithFuzzyClientRetryFail(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    testRetryFail(
        (counter, awsProperties) -> {
          try {
            testRangeRead(fuzzyClient(counter, exception), awsProperties);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        exception,
        shouldRetry);
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testRangeReadWithFuzzyStreamRetryFail(Exception exception, boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    testRetryFail(
        (counter, awsProperties) -> {
          try {
            testRangeRead(fuzzyStreamClient(counter, (IOException) exception), awsProperties);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        exception,
        shouldRetry);
  }

  @ParameterizedTest
  @MethodSource("provideExceptionAndShouldRetry")
  public void testExceptionNoRetry(Exception exception, boolean shouldRetry) throws Exception {
    Assume.assumeFalse(shouldRetry);
    testReadExceptionShouldNotRetry(exception);
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
        Arguments.of(S3Exception.builder().statusCode(416).message("failure").build(), false));
  }

  private void testRetryFail(
      BiConsumer<AtomicInteger, S3FileIOProperties> runnable,
      Exception exception,
      boolean shouldRetry)
      throws Exception {
    Assume.assumeTrue(shouldRetry);
    S3FileIOProperties awsProperties = new S3FileIOProperties();
    awsProperties.setS3ReadRetryNumRetries(2);
    awsProperties.setS3ReadRetryMinWaitMs(100);
    AtomicInteger counter = new AtomicInteger(4);
    AssertHelpers.assertThrowsCause(
        "Should fail after retries",
        exception.getClass(),
        exception.getMessage(),
        () -> runnable.accept(counter, awsProperties));
    Assert.assertEquals(
        "Should have 3 invocations (1 initial call, 2 retries)", 3, 4 - counter.get());
  }

  private void testReadExceptionShouldNotRetry(Exception exception) throws Exception {
    S3FileIOProperties awsProperties = new S3FileIOProperties();
    awsProperties.setS3ReadRetryNumRetries(4);
    awsProperties.setS3ReadRetryMinWaitMs(100);
    AtomicInteger counter = new AtomicInteger(3);
    AssertHelpers.assertThrowsCause(
        "Should fail without retry",
        exception.getClass(),
        exception.getMessage(),
        () -> {
          try {
            testRead(fuzzyClient(counter, exception), awsProperties);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    Assert.assertEquals(
        "Should have 1 invocation (1 initial call, no retry)", 1, 3 - counter.get());
  }

  private S3Client fuzzyClient(AtomicInteger counter, Exception failure) {
    S3Client fuzzyClient = spy(new S3ClientWrapper(s3Client()));
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
        .getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
    return fuzzyClient;
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
        throws NoSuchKeyException, InvalidObjectStateException, AwsServiceException,
            SdkClientException, S3Exception {
      return delegate.getObject(getObjectRequest, responseTransformer);
    }

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest)
        throws NoSuchKeyException, AwsServiceException, SdkClientException, S3Exception {
      return delegate.headObject(headObjectRequest);
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody)
        throws AwsServiceException, SdkClientException, S3Exception {
      return delegate.putObject(putObjectRequest, requestBody);
    }

    @Override
    public CreateBucketResponse createBucket(CreateBucketRequest createBucketRequest)
        throws BucketAlreadyExistsException, BucketAlreadyOwnedByYouException, AwsServiceException,
            SdkClientException, S3Exception {
      return delegate.createBucket(createBucketRequest);
    }
  }

  public static class FuzzyResponseInputStream extends InputStream {

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
