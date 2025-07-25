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
package org.apache.iceberg.rest.auth.oauth2.http;

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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthSession;

/**
 * An adapter that allows using an Iceberg {@link RESTClient} as a Nimbus {@link HTTPRequestSender}.
 *
 * <p>Note: this adapter is only compatible with {@link RESTClient} instances that unwrap to an
 * Apache {@link HttpClient}, e.g. {@link org.apache.iceberg.rest.HTTPClient}.
 */
public class RESTClientAdapter implements HTTPRequestSender {

  private final Supplier<RESTClient> restClientSupplier;

  public RESTClientAdapter(Supplier<RESTClient> restClientSupplier) {
    this.restClientSupplier = restClientSupplier;
  }

  @Override
  public ReadOnlyHTTPResponse send(ReadOnlyHTTPRequest httpRequest) throws IOException {

    RESTClient restClient = restClientSupplier.get().withAuthSession(AuthSession.EMPTY);

    HttpClient httpClient =
        restClient
            .unwrap(HttpClient.class)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "RESTClient does not unwrap to Apache HttpClient"));

    BasicClassicHttpRequest request =
        new BasicClassicHttpRequest(httpRequest.getMethod().name(), httpRequest.getURI());

    httpRequest.getHeaderMap().forEach((k, v) -> v.forEach(vv -> request.addHeader(k, vv)));

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
                .computeIfAbsent(header.getName(), k -> Lists.newArrayList())
                .add(header.getValue());
          }

          return httpResponse;
        });
  }
}
