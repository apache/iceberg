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
package org.apache.iceberg.aws;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPHeaders;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.auth.AuthSession;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4FamilyHttpSigner;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.regions.Region;

/**
 * An AuthSession that signs requests with SigV4.
 *
 * <p>The request is first authenticated by the delegate AuthSession, then signed with SigV4. In
 * case of conflicting headers, the Authorization header set by delegate AuthSession will be
 * relocated, then included in the canonical headers to sign.
 *
 * <p>See <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html">Signing AWS
 * API requests</a> for details about the SigV4 protocol.
 */
public class RESTSigV4AuthSession implements AuthSession {

  static final String EMPTY_BODY_SHA256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  static final String RELOCATED_HEADER_PREFIX = "Original-";

  private final AwsV4HttpSigner signer;
  private final AuthSession delegate;
  private final Region signingRegion;
  private final String signingName;
  private final AwsCredentialsProvider credentialsProvider;

  public RESTSigV4AuthSession(
      AwsV4HttpSigner aws4Signer, AuthSession delegateAuthSession, AwsProperties awsProperties) {
    this.signer = Preconditions.checkNotNull(aws4Signer, "Invalid signer: null");
    this.delegate = Preconditions.checkNotNull(delegateAuthSession, "Invalid delegate: null");
    Preconditions.checkNotNull(awsProperties, "Invalid AWS properties: null");
    this.signingRegion = awsProperties.restSigningRegion();
    this.signingName = awsProperties.restSigningName();
    this.credentialsProvider = awsProperties.restCredentialsProvider();
  }

  @Override
  public HTTPRequest authenticate(HTTPRequest request) {
    return sign(delegate.authenticate(request));
  }

  @Override
  public void close() {
    delegate.close();
  }

  private HTTPRequest sign(HTTPRequest request) {
    URI uri = request.requestUri();
    String body = request.encodedBody();

    SdkHttpRequest sdkHttpRequest =
        SdkHttpRequest.builder()
            .method(SdkHttpMethod.fromValue(request.method().name()))
            .protocol(uri.getScheme())
            .uri(uri)
            .headers(convertHeaders(request.headers()))
            .build();

    SignRequest.Builder<AwsCredentials> signRequestBuilder =
        SignRequest.builder(credentialsProvider.resolveCredentials())
            .request(sdkHttpRequest)
            .putProperty(AwsV4FamilyHttpSigner.CHECKSUM_ALGORITHM, DefaultChecksumAlgorithm.SHA256)
            .putProperty(AwsV4FamilyHttpSigner.SERVICE_SIGNING_NAME, signingName)
            .putProperty(AwsV4HttpSigner.REGION_NAME, signingRegion.toString());

    if (body == null) {
      signRequestBuilder.payload(null);
    } else {
      signRequestBuilder.payload(
          ContentStreamProvider.fromInputStream(
              IOUtils.toInputStream(body, StandardCharsets.UTF_8)));
    }
    SignedRequest signedRequest = signer.sign(signRequestBuilder.build());
    HTTPHeaders newHeaders =
        updateRequestHeaders(request.headers(), signedRequest.request().headers());
    return ImmutableHTTPRequest.builder().from(request).headers(newHeaders).build();
  }

  private Map<String, List<String>> convertHeaders(HTTPHeaders headers) {
    return headers.entries().stream()
        .collect(
            Collectors.groupingBy(
                // Relocate Authorization header as SigV4 takes precedence
                header ->
                    header.name().equalsIgnoreCase("Authorization")
                        ? RELOCATED_HEADER_PREFIX + header.name()
                        : header.name(),
                Collectors.mapping(HTTPHeader::value, Collectors.toList())));
  }

  private HTTPHeaders updateRequestHeaders(
      HTTPHeaders originalHeaders, Map<String, List<String>> signedHeaders) {
    ImmutableHTTPHeaders.Builder newHeaders = ImmutableHTTPHeaders.builder();
    signedHeaders.forEach(
        (name, signedValues) -> {
          if (originalHeaders.contains(name)) {
            for (HTTPHeader originalHeader : originalHeaders.entries(name)) {
              // Relocate headers if there is a conflict with signed headers
              if (!signedValues.contains(originalHeader.value())) {
                newHeaders.addEntry(
                    HTTPHeader.of(RELOCATED_HEADER_PREFIX + name, originalHeader.value()));
              }
            }
          }

          signedValues.forEach(value -> newHeaders.addEntry(HTTPHeader.of(name, value)));
        });

    return newHeaders.build();
  }
}
