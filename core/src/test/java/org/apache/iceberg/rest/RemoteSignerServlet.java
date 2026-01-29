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
package org.apache.iceberg.rest;

import static java.lang.String.format;
import static org.apache.iceberg.rest.RESTCatalogAdapter.castRequest;
import static org.apache.iceberg.rest.RESTCatalogAdapter.castResponse;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base servlet for remote signing tests. This servlet handles OAuth token requests and delegates
 * signing to subclasses. It does not handle any other requests.
 *
 * <p>Subclasses must implement {@link #signRequest(RemoteSignRequest)} to provide the actual
 * signing logic.
 */
public abstract class RemoteSignerServlet extends HttpServlet {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteSignerServlet.class);
  private static final String POST = "POST";

  private static final String CACHE_CONTROL = "Cache-Control";
  private static final String CACHE_CONTROL_PRIVATE = "private";
  private static final String CACHE_CONTROL_NO_CACHE = "no-cache";

  private static final Set<HttpMethod> CACHEABLE_METHODS =
      EnumSet.of(HttpMethod.GET, HttpMethod.HEAD);

  private static final Map<String, String> RESPONSE_HEADERS =
      ImmutableMap.of(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

  private final String signEndpoint;

  protected RemoteSignerServlet(String signEndpoint) {
    this.signEndpoint = signEndpoint;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    execute(request, response);
  }

  @Override
  protected void doHead(HttpServletRequest request, HttpServletResponse response) {
    execute(request, response);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    execute(request, response);
  }

  @Override
  protected void doDelete(HttpServletRequest request, HttpServletResponse response) {
    execute(request, response);
  }

  /**
   * Sign the given request and return the signed response.
   *
   * @param request the remote sign request
   * @return the signed response
   */
  protected abstract RemoteSignResponse signRequest(RemoteSignRequest request);

  /**
   * Called after a sign request is parsed but before signing. Subclasses can override to add
   * additional validation.
   *
   * @param request the remote sign request
   */
  protected void validateSignRequest(RemoteSignRequest request) {
    // no-op by default
  }

  /**
   * Called after signing to allow subclasses to add response headers (e.g., cache control). By
   * default, this method adds cache control headers based on the request method.
   *
   * @param request the original sign request
   * @param response the HTTP response to add headers to
   */
  protected void addSignResponseHeaders(RemoteSignRequest request, HttpServletResponse response) {
    if (CACHEABLE_METHODS.contains(HttpMethod.valueOf(request.method()))) {
      // tell the client this can be cached
      response.setHeader(CACHE_CONTROL, CACHE_CONTROL_PRIVATE);
    } else {
      response.setHeader(CACHE_CONTROL, CACHE_CONTROL_NO_CACHE);
    }
  }

  private OAuthTokenResponse handleOAuth(Map<String, String> requestMap) {
    String grantType = requestMap.get("grant_type");
    switch (grantType) {
      case "client_credentials":
        return castResponse(
            OAuthTokenResponse.class,
            OAuthTokenResponse.builder()
                .withToken("client-credentials-token:sub=" + requestMap.get("client_id"))
                .withIssuedTokenType("urn:ietf:params:oauth:token-type:access_token")
                .withTokenType("Bearer")
                .setExpirationInSeconds(10000)
                .build());

      case "urn:ietf:params:oauth:grant-type:token-exchange":
        String actor = requestMap.get("actor_token");
        String token =
            String.format(
                "token-exchange-token:sub=%s%s",
                requestMap.get("subject_token"), actor != null ? ",act=" + actor : "");
        return castResponse(
            OAuthTokenResponse.class,
            OAuthTokenResponse.builder()
                .withToken(token)
                .withIssuedTokenType("urn:ietf:params:oauth:token-type:access_token")
                .withTokenType("Bearer")
                .setExpirationInSeconds(10000)
                .build());

      default:
        throw new UnsupportedOperationException("Unsupported grant_type: " + grantType);
    }
  }

  protected void execute(HttpServletRequest request, HttpServletResponse response) {
    response.setStatus(HttpServletResponse.SC_OK);
    RESPONSE_HEADERS.forEach(response::setHeader);

    String path = request.getRequestURI().substring(1);
    Object requestBody;
    try {
      if (POST.equals(request.getMethod()) && signEndpoint.equals(path)) {
        RemoteSignRequest signRequest =
            castRequest(
                RemoteSignRequest.class,
                RESTObjectMapper.mapper().readValue(request.getReader(), RemoteSignRequest.class));
        validateSignRequest(signRequest);
        RemoteSignResponse signResponse = signRequest(signRequest);
        addSignResponseHeaders(signRequest, response);
        RESTObjectMapper.mapper().writeValue(response.getWriter(), signResponse);
      } else if (POST.equals(request.getMethod()) && ResourcePaths.tokens().equals(path)) {
        try (Reader reader = new InputStreamReader(request.getInputStream())) {
          requestBody = RESTUtil.decodeFormData(CharStreams.toString(reader));
        }

        @SuppressWarnings("unchecked")
        OAuthTokenResponse oAuthTokenResponse =
            handleOAuth((Map<String, String>) castRequest(Map.class, requestBody));
        RESTObjectMapper.mapper().writeValue(response.getWriter(), oAuthTokenResponse);
      } else {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        RESTObjectMapper.mapper()
            .writeValue(
                response.getWriter(),
                org.apache.iceberg.rest.responses.ErrorResponse.builder()
                    .responseCode(400)
                    .withType("BadRequestException")
                    .withMessage(format("No route for request: %s %s", request.getMethod(), path))
                    .build());
      }
    } catch (RESTException e) {
      LOG.error("Error processing REST request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (Exception e) {
      LOG.error("Unexpected exception when processing REST request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }
}
