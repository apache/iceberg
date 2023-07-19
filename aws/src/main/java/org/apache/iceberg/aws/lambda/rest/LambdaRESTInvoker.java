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
package org.apache.iceberg.aws.lambda.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.BaseRESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.ErrorResponse;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

/**
 * A client to fulfill REST catalog request and response using AWS Lambda's Invoke API.
 *
 * <p>This is useful for situations where an HTTP connection cannot be established against a REST
 * endpoint. For example, when the endpoint is in an isolated private subnet, a Lambda can be placed
 * within the subnet as a proxy for communication. See {@link LambdaRESTRequest} and {@link
 * LambdaRESTResponse} for request-response contract
 */
public class LambdaRESTInvoker extends BaseRESTClient {

  private LambdaClient lambda;
  private String functionArn;
  private Map<String, String> baseHeaders;

  public LambdaRESTInvoker() {}

  @VisibleForTesting
  LambdaRESTInvoker(
      String baseUri,
      ObjectMapper objectMapper,
      Map<String, String> baseHeaders,
      LambdaClient lambda,
      String functionArn) {
    super.initialize(baseUri, objectMapper, baseHeaders, ImmutableMap.of());
    this.baseHeaders = baseHeaders;
    this.lambda = lambda;
    this.functionArn = functionArn;
  }

  @Override
  protected <T> T execute(
      Method method,
      String path,
      Map<String, String> queryParams,
      Object requestBody,
      Class<T> responseType,
      Map<String, String> inputHeaders,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    validateInputPath(path);
    URI uri = buildUri(path, queryParams);

    Map<String, String> headers;
    String entity = null;
    if (requestBody instanceof Map) {
      headers = requestHeaders(inputHeaders, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
      entity = RESTUtil.encodeFormData((Map<?, ?>) requestBody);
    } else if (requestBody != null) {
      headers = requestHeaders(inputHeaders, ContentType.APPLICATION_JSON.getMimeType());
      entity = super.toJsonString(requestBody);
    } else {
      headers = requestHeaders(inputHeaders, ContentType.APPLICATION_JSON.getMimeType());
    }

    LambdaRESTRequest request =
        ImmutableLambdaRESTRequest.builder()
            .method(method.name())
            .uri(uri)
            .headers(headers)
            .entity(entity)
            .build();

    InvokeResponse lambdaResponse;
    try {
      lambdaResponse =
          lambda.invoke(
              InvokeRequest.builder()
                  .functionName(functionArn)
                  .payload(SdkBytes.fromUtf8String(LambdaRESTRequestParser.toJson(request)))
                  .build());
    } catch (AwsServiceException e) {
      throw new RESTException(e, "Error occurred while processing %s request", method);
    }

    LambdaRESTResponse response =
        LambdaRESTResponseParser.fromJsonStream(lambdaResponse.payload().asInputStream());
    responseHeaders.accept(response.headers());

    if (response.code() == HttpStatus.SC_NO_CONTENT
        || (responseType == null && isSuccessful(response.code()))) {
      return null;
    }

    if (!isSuccessful(response.code())) {
      throwFailure(response.code(), response.entity(), response.reason(), errorHandler);
    }

    if (response.entity() == null) {
      throw new RESTException(
          "Invalid (null) response body for request (expected %s): method=%s, path=%s, status=%d",
          responseType.getSimpleName(), method.name(), path, response.code());
    }

    return parseResponse(response.entity(), responseType, response.code());
  }

  @Override
  public void close() throws IOException {
    lambda.close();
  }

  @Override
  public void initialize(
      String baseUri,
      ObjectMapper objectMapper,
      Map<String, String> headers,
      Map<String, String> properties) {
    super.initialize(baseUri, objectMapper, headers, properties);
    LambdaRESTInvokerProperties lambdaRESTInvokerProperties =
        new LambdaRESTInvokerProperties(properties);
    this.functionArn = lambdaRESTInvokerProperties.functionArn();
    this.lambda = LambdaRESTInvokerAwsClientFactories.from(properties).lambda();
    this.baseHeaders = headers;
    // TODO: support sigv4 signer
  }

  private Map<String, String> requestHeaders(
      Map<String, String> inputHeaders, String bodyMimeType) {
    return ImmutableMap.<String, String>builder()
        .putAll(baseHeaders)
        .putAll(inputHeaders)
        .put(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType())
        .put(HttpHeaders.CONTENT_TYPE, bodyMimeType)
        .buildKeepingLast();
  }
}
