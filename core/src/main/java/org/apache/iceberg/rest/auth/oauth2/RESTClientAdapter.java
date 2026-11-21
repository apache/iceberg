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
package org.apache.iceberg.rest.auth.oauth2;

import com.nimbusds.oauth2.sdk.http.HTTPRequestSender;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.http.ReadOnlyHTTPRequest;
import com.nimbusds.oauth2.sdk.http.ReadOnlyHTTPResponse;
import java.io.IOException;
import java.util.function.Supplier;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpRequest;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RESTClient;

/**
 * Sends Nimbus {@link ReadOnlyHTTPRequest requests} through an Iceberg {@link RESTClient}, so that
 * requests to the authorization server share the catalog's HTTP client configuration (proxies, TLS,
 * timeouts, retries).
 *
 * <p>This adapter requires a {@link RESTClient} that {@linkplain RESTClient#unwrap(Class) unwraps}
 * to an Apache {@link HttpClient}, such as {@link org.apache.iceberg.rest.HTTPClient}.
 */
final class RESTClientAdapter implements HTTPRequestSender {

  private final Supplier<RESTClient> restClientSupplier;

  RESTClientAdapter(Supplier<RESTClient> restClientSupplier) {
    this.restClientSupplier = restClientSupplier;
  }

  @Override
  public ReadOnlyHTTPResponse send(ReadOnlyHTTPRequest httpRequest) throws IOException {
    RESTClient restClient = restClientSupplier.get();
    HttpClient httpClient = restClient.unwrap(HttpClient.class);
    Preconditions.checkArgument(
        httpClient != null, "RESTClient does not unwrap to Apache HttpClient: %s", restClient);

    BasicClassicHttpRequest request =
        new BasicClassicHttpRequest(httpRequest.getMethod().name(), httpRequest.getURI());
    httpRequest
        .getHeaderMap()
        .forEach((name, values) -> values.forEach(value -> request.addHeader(name, value)));

    String requestBody = httpRequest.getBody();
    if (requestBody != null) {
      request.setEntity(new StringEntity(requestBody));
    }

    return httpClient.execute(
        request,
        response -> {
          HTTPResponse httpResponse = new HTTPResponse(response.getCode());
          if (response.getEntity() != null) {
            try {
              String body = EntityUtils.toString(response.getEntity());
              if (!body.isEmpty()) {
                httpResponse.setBody(body);
              }
            } catch (ParseException e) {
              throw new IOException(e);
            }
          }

          for (Header header : response.getHeaders()) {
            httpResponse
                .getHeaderMap()
                .computeIfAbsent(header.getName(), name -> Lists.newArrayList())
                .add(header.getValue());
          }

          return httpResponse;
        });
  }
}
