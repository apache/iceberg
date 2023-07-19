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
package org.apache.iceberg.aws.s3.signer;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.s3.MinioContainer;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.internal.AbstractAws4Signer;
import software.amazon.awssdk.auth.signer.internal.AbstractAwsS3V4Signer;
import software.amazon.awssdk.auth.signer.internal.Aws4SignerRequestParams;
import software.amazon.awssdk.auth.signer.internal.SignerConstant;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.core.checksums.SdkChecksum;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

public class TestS3RestSigner {

  private static final Region REGION = Region.US_WEST_2;
  private static final String BUCKET = "iceberg-s3-signer-test";
  static final AwsCredentialsProvider CREDENTIALS_PROVIDER =
      StaticCredentialsProvider.create(
          AwsBasicCredentials.create("accessKeyId", "secretAccessKey"));
  private static final MinioContainer MINIO_CONTAINER =
      new MinioContainer(CREDENTIALS_PROVIDER.resolveCredentials());

  private static Server httpServer;
  private static ValidatingSigner validatingSigner;
  private S3Client s3;

  @BeforeAll
  public static void beforeClass() throws Exception {
    if (null == httpServer) {
      httpServer = initHttpServer();
    }

    validatingSigner =
        new ValidatingSigner(
            ImmutableS3V4RestSignerClient.builder()
                .properties(
                    ImmutableMap.of(
                        S3V4RestSignerClient.S3_SIGNER_URI,
                        httpServer.getURI().toString(),
                        OAuth2Properties.CREDENTIAL,
                        "catalog:12345"))
                .build(),
            new CustomAwsS3V4Signer());
  }

  @AfterAll
  public static void afterClass() throws Exception {
    assertThat(validatingSigner.icebergSigner.tokenRefreshExecutor())
        .isInstanceOf(ScheduledThreadPoolExecutor.class);

    ScheduledThreadPoolExecutor executor =
        ((ScheduledThreadPoolExecutor) validatingSigner.icebergSigner.tokenRefreshExecutor());
    // token expiration is set to 100s so there should be exactly one token scheduled for refresh
    assertThat(executor.getPoolSize()).isEqualTo(1);
    assertThat(executor.getQueue())
        .as("should only have a single token scheduled for refresh")
        .hasSize(1);
    assertThat(executor.getActiveCount())
        .as("should not have any token being refreshed")
        .isEqualTo(0);
    assertThat(executor.getCompletedTaskCount())
        .as("should not have any expired token that required a refresh")
        .isEqualTo(0);

    if (null != httpServer) {
      httpServer.stop();
    }
  }

  @BeforeEach
  public void before() throws Exception {
    MINIO_CONTAINER.start();
    s3 =
        S3Client.builder()
            .region(REGION)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .applyMutation(
                s3ClientBuilder ->
                    s3ClientBuilder.httpClientBuilder(
                        software.amazon.awssdk.http.apache.ApacheHttpClient.builder()))
            .endpointOverride(MINIO_CONTAINER.getURI())
            .forcePathStyle(true) // OSX won't resolve subdomains
            .overrideConfiguration(
                c -> c.putAdvancedOption(SdkAdvancedClientOption.SIGNER, validatingSigner))
            .build();

    s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET).key("random/key").build(),
        Paths.get("/etc/hosts"));
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET).key("encoded/key=value/file").build(),
        Paths.get("/etc/hosts"));

    s3.createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(BUCKET).key("random/multipart-key").build());
  }

  private static Server initHttpServer() throws Exception {
    S3SignerServlet servlet = new S3SignerServlet(S3ObjectMapper.mapper());
    ServletContextHandler servletContext =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContext.setContextPath("/");
    ServletHolder servletHolder = new ServletHolder(servlet);
    servletHolder.setInitParameter("javax.ws.rs.Application", "ServiceListPublic");
    servletContext.addServlet(servletHolder, "/*");
    servletContext.setVirtualHosts(null);
    servletContext.setGzipHandler(new GzipHandler());

    Server server = new Server(0);
    server.setHandler(servletContext);
    server.start();
    return server;
  }

  @Test
  public void validateGetObject() {
    s3.getObject(GetObjectRequest.builder().bucket(BUCKET).key("random/key").build());
    // signer caching should kick in when repeating the same request
    s3.getObject(GetObjectRequest.builder().bucket(BUCKET).key("random/key").build());
  }

  @Test
  public void validatePutObject() {
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET).key("some/key").build(), Paths.get("/etc/hosts"));
  }

  @Test
  public void validateListPrefix() {
    s3.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET).prefix("some/prefix/").build());
  }

  @Test
  public void validateEncodedGetObject() {
    s3.getObject(GetObjectRequest.builder().bucket(BUCKET).key("encoded/key=value/file").build());
    // signer caching should kick in when repeating the same request
    s3.getObject(GetObjectRequest.builder().bucket(BUCKET).key("encoded/key=value/file").build());
  }

  @Test
  public void validatedCreateMultiPartUpload() {
    s3.createMultipartUpload(
        CreateMultipartUploadRequest.builder().bucket(BUCKET).key("some/multipart-key").build());
  }

  @AfterEach
  public void after() {
    MINIO_CONTAINER.stop();
  }

  @Test
  public void validatedUploadPart() {
    String multipartUploadId =
        s3.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                    .bucket(BUCKET)
                    .key("some/multipart-key")
                    .build())
            .uploadId();
    s3.uploadPart(
        UploadPartRequest.builder()
            .bucket(BUCKET)
            .key("some/multipart-key")
            .uploadId(multipartUploadId)
            .partNumber(1)
            .build(),
        RequestBody.fromString("content"));
  }

  /**
   * A signer that compares the Authorization header after signing the request with the {@link
   * S3V4RestSignerClient} and with the {@link AbstractAwsS3V4Signer}
   */
  private static class ValidatingSigner
      extends AbstractAws4Signer<AwsS3V4SignerParams, Aws4PresignerParams> {
    private final S3V4RestSignerClient icebergSigner;
    private final AbstractAwsS3V4Signer awsSigner;

    private ValidatingSigner(S3V4RestSignerClient icebergSigner, AbstractAwsS3V4Signer awsSigner) {
      this.icebergSigner = icebergSigner;
      this.awsSigner = awsSigner;
    }

    @Override
    protected void processRequestPayload(
        SdkHttpFullRequest.Builder mutableRequest,
        byte[] signature,
        byte[] signingKey,
        Aws4SignerRequestParams signerRequestParams,
        AwsS3V4SignerParams signerParams) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void processRequestPayload(
        SdkHttpFullRequest.Builder mutableRequest,
        byte[] signature,
        byte[] signingKey,
        Aws4SignerRequestParams signerRequestParams,
        AwsS3V4SignerParams signerParams,
        SdkChecksum sdkChecksum) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected String calculateContentHashPresign(
        SdkHttpFullRequest.Builder mutableRequest, Aws4PresignerParams signerParams) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SdkHttpFullRequest presign(
        SdkHttpFullRequest request, ExecutionAttributes executionAttributes) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SdkHttpFullRequest sign(
        SdkHttpFullRequest request, ExecutionAttributes executionAttributes) {

      AwsS3V4SignerParams signerParams =
          extractSignerParams(AwsS3V4SignerParams.builder(), executionAttributes)
              .signingClockOverride(S3SignerServlet.SIGNING_CLOCK)
              .enableChunkedEncoding(false)
              .timeOffset(0)
              .doubleUrlEncode(false)
              .enablePayloadSigning(false)
              .signingName("s3")
              .build();

      SdkHttpFullRequest icebergResult = icebergSigner.sign(request, executionAttributes);

      SdkHttpFullRequest awsResult = signWithAwsSigner(request, signerParams);

      assertThat(awsResult.headers().get("Authorization"))
          .isEqualTo(icebergResult.headers().get("Authorization"));

      assertThat(awsResult.headers()).isEqualTo(icebergResult.headers());
      return awsResult;
    }

    @NotNull
    private SdkHttpFullRequest signWithAwsSigner(
        SdkHttpFullRequest request, AwsS3V4SignerParams signerParams) {
      // we need to filter out the unsigned headers for the AWS signer and re-append those headers
      // back after signing
      Map<String, List<String>> unsignedHeaders =
          request.headers().entrySet().stream()
              .filter(e -> S3SignerServlet.UNSIGNED_HEADERS.contains(e.getKey().toLowerCase()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      SdkHttpFullRequest.Builder builder = request.toBuilder();
      for (String unsignedHeader : S3SignerServlet.UNSIGNED_HEADERS) {
        builder.removeHeader(unsignedHeader);
      }

      SdkHttpFullRequest awsResult = awsSigner.sign(builder.build(), signerParams);
      // append the unsigned headers back
      SdkHttpFullRequest.Builder mutableResult = awsResult.toBuilder();
      unsignedHeaders.forEach(mutableResult::putHeader);
      return mutableResult.build();
    }
  }

  /**
   * A custom AWS Signer that overrides {@link
   * AbstractAwsS3V4Signer#calculateContentHash(SdkHttpFullRequest.Builder, AwsS3V4SignerParams,
   * SdkChecksum)} because we don't want to sign the payload. We disabled payload signing via {@link
   * AwsS3V4SignerParams#enablePayloadSigning()} but the <a
   * href="https://github.com/aws/aws-sdk-java-v2/blob/master/core/auth/src/main/java/software/amazon/awssdk/auth/signer/internal/AbstractAwsS3V4Signer.java#L206-L255">original
   * code</a> looks at the used protocol and if it's not <b>https</b> it will by default enable
   * payload signing <a
   * href="https://github.com/aws/aws-sdk-java-v2/blob/ee30e19bf6618462a9a5ec1b3beac1e29013379b/core/auth/src/main/java/software/amazon/awssdk/auth/signer/internal/AbstractAwsS3V4Signer.java#L281">here</a>.
   *
   * <p>However, we run Minio with <b>http</b> and don't have a means to disable payload signing in
   * order to achieve the same signature in the {@link ValidatingSigner#sign(SdkHttpFullRequest,
   * ExecutionAttributes)} check above.
   */
  private static class CustomAwsS3V4Signer extends AbstractAwsS3V4Signer {

    @Override
    protected String calculateContentHash(
        SdkHttpFullRequest.Builder mutableRequest,
        AwsS3V4SignerParams signerParams,
        SdkChecksum contentFlexibleChecksum) {
      boolean isUnsignedStreamingTrailer =
          mutableRequest
              .firstMatchingHeader(SignerConstant.X_AMZ_CONTENT_SHA256)
              .map(STREAMING_UNSIGNED_PAYLOAD_TRAILER::equals)
              .orElse(false);

      if (!isUnsignedStreamingTrailer) {
        // To be consistent with other service clients using sig-v4,
        // we just set the header as "required", and AWS4Signer.sign() will be
        // notified to pick up the header value returned by this method.
        mutableRequest.putHeader(SignerConstant.X_AMZ_CONTENT_SHA256, "required");
      }
      return S3V4RestSignerClient.UNSIGNED_PAYLOAD;
    }
  }
}
