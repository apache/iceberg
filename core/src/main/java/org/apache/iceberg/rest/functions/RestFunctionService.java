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
package org.apache.iceberg.rest.functions;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** Minimal REST client to list and fetch UDF function specs for the POC. */
public class RestFunctionService {

  private final String baseUri; // e.g., http://localhost:8181
  private final String authHeader; // optional bearer token or header value
  private final HttpClient client;

  public RestFunctionService(String baseUri, String authHeader) {
    this.baseUri = baseUri.endsWith("/") ? baseUri.substring(0, baseUri.length() - 1) : baseUri;
    this.authHeader = authHeader;
    this.client =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .version(HttpClient.Version.HTTP_1_1)
            .build();
  }

  public List<String> listFunctions(String[] namespace) {
    try {
      String ns = String.join(".", namespace);
      String url = baseUri + "/v1/functions?namespace=" + urlEncode(ns);
      HttpRequest.Builder req = HttpRequest.newBuilder().uri(URI.create(url)).GET();
      if (authHeader != null && !authHeader.isEmpty()) {
        req.header("Authorization", authHeader);
      }

      HttpResponse<String> resp = client.send(req.build(), HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() == 200) {
        // Expect newline or comma separated list for POC; be permissive
        String body = resp.body().trim();
        if (body.isEmpty()) {
          return Lists.newArrayList();
        }
        if (body.startsWith("[") && body.endsWith("]")) {
          // naive JSON array parsing (no external deps here)
          String inner = body.substring(1, body.length() - 1).trim();
          if (inner.isEmpty()) {
            return Lists.newArrayList();
          }
          Iterable<String> parts = Splitter.on(',').split(inner);
          List<String> out = Lists.newArrayList();
          for (String p : parts) {
            String s = stripQuotes(p.trim());
            if (!s.isEmpty()) {
              out.add(s);
            }
          }
          return out;
        }
        // fallback: newline separated
        List<String> out = Lists.newArrayList();
        for (String line : Splitter.on('\n').split(body)) {
          String s = line.trim();
          if (!s.isEmpty()) {
            out.add(s);
          }
        }
        return out;
      }
      throw new RuntimeException("Failed to list functions: HTTP " + resp.statusCode());
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("Failed to list functions", e);
    }
  }

  public String getFunctionSpecJson(String[] namespace, String name) {
    try {
      String ns = String.join(".", namespace);
      if (ns.isEmpty()) {
        ns = "default"; // POC fallback
      }
      String url = baseUri + "/v1/functions/" + urlEncode(ns) + "/" + urlEncode(name);
      HttpRequest.Builder req = HttpRequest.newBuilder().uri(URI.create(url)).GET();
      if (authHeader != null && !authHeader.isEmpty()) {
        req.header("Authorization", authHeader);
      }
      HttpResponse<String> resp = client.send(req.build(), HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() == 200) {
        return resp.body();
      }
      throw new RuntimeException(
          "Failed to get function spec: HTTP " + resp.statusCode() + " for " + name);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("Failed to get function spec", e);
    }
  }

  private static String urlEncode(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }

  private static String stripQuotes(String s) {
    if ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'"))) {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }
}


