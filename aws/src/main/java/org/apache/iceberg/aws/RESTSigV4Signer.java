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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.iceberg.exceptions.RESTException;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.internal.SignerConstant;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.auth.signer.params.SignerChecksumParams;
import software.amazon.awssdk.core.checksums.Algorithm;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

/**
 * Provides a request interceptor for use with the HTTPClient that calculates the required signature
 * for the SigV4 protocol and adds the necessary headers for all requests created by the client.
 *
 * <p>See <a
 * href="https://docs.aws.amazon.com/general/latest/gr/signing-aws-api-requests.html">Signing AWS
 * API requests</a> for details about the protocol.
 */
public class RESTSigV4Signer implements HttpRequestInterceptor {
  static final String EMPTY_BODY_SHA256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  static final String RELOCATED_HEADER_PREFIX = "Original-";

  private final Aws4Signer signer = Aws4Signer.create();
  private AwsCredentialsProvider credentialsProvider;

  private String signingName;
  private Region signingRegion;

  public void initialize(Map<String, String> properties) {
    AwsProperties awsProperties = new AwsProperties(properties);

    this.signingRegion = awsProperties.restSigningRegion();
    this.signingName = awsProperties.restSigningName();
    this.credentialsProvider = awsProperties.restCredentialsProvider();
  }

  @Override
  public void process(HttpRequest request, EntityDetails entity, HttpContext context) {
    URI requestUri;

    try {
      requestUri = request.getUri();
    } catch (URISyntaxException e) {
      throw new RESTException(e, "Invalid uri for request: %s", request);
    }

    Aws4SignerParams params =
        Aws4SignerParams.builder()
            .signingName(signingName)
            .signingRegion(signingRegion)
            .awsCredentials(credentialsProvider.resolveCredentials())
            .checksumParams(
                SignerChecksumParams.builder()
                    .algorithm(Algorithm.SHA256)
                    .isStreamingRequest(false)
                    .checksumHeaderName(SignerConstant.X_AMZ_CONTENT_SHA256)
                    .build())
            .build();

    SdkHttpFullRequest.Builder sdkRequestBuilder = SdkHttpFullRequest.builder();

    sdkRequestBuilder
        .method(SdkHttpMethod.fromValue(request.getMethod()))
        .protocol(request.getScheme())
        .uri(requestUri)
        .headers(convertHeaders(request.getHeaders()));

    if (entity == null) {
      // This is a workaround for the signer implementation incorrectly producing
      // an invalid content checksum for empty body requests.
      sdkRequestBuilder.putHeader(SignerConstant.X_AMZ_CONTENT_SHA256, EMPTY_BODY_SHA256);
    } else if (entity instanceof StringEntity) {
      sdkRequestBuilder.contentStreamProvider(
          () -> {
            try {
              return ((StringEntity) entity).getContent();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    } else {
      throw new UnsupportedOperationException("Unsupported entity type: " + entity.getClass());
    }

    SdkHttpFullRequest signedSdkRequest = signer.sign(sdkRequestBuilder.build(), params);
    updateRequestHeaders(request, signedSdkRequest.headers());
  }

  private Map<String, List<String>> convertHeaders(Header[] headers) {
    return Arrays.stream(headers)
        .collect(
            Collectors.groupingBy(
                // Relocate Authorization header as SigV4 takes precedence
                header ->
                    HttpHeaders.AUTHORIZATION.equals(header.getName())
                        ? RELOCATED_HEADER_PREFIX + header.getName()
                        : header.getName(),
                Collectors.mapping(Header::getValue, Collectors.toList())));
  }

  private void updateRequestHeaders(HttpRequest request, Map<String, List<String>> headers) {
    headers.forEach(
        (name, values) -> {
          if (request.containsHeader(name)) {
            Header[] original = request.getHeaders(name);
            request.removeHeaders(name);
            Arrays.asList(original)
                .forEach(
                    header -> {
                      // Relocate headers if there is a conflict with signed headers
                      if (!values.contains(header.getValue())) {
                        request.addHeader(RELOCATED_HEADER_PREFIX + name, header.getValue());
                      }
                    });
          }

          values.forEach(value -> request.setHeader(name, value));
        });
  }
}
