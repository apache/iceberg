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
package org.apache.iceberg.io.http;

import java.io.Serializable;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.io.CloseMode;
import org.apache.iceberg.io.InputFile;

/**
 * A helper that a {@link org.apache.iceberg.io.FileIO} can delegate to for reading a file directly
 * over HTTP(S) instead of its normal, credentialed read path.
 *
 * <p>Intended for catalogs that vend a pre-signed object-store URL directly as a file's location
 * (e.g. a scan task's {@code file-path}). The location is used unchanged as the fetch URL, so {@link
 * InputFile#location()} equals the location passed to {@link #newInputFile(String)}.
 */
public class HttpUrlSupport implements Serializable {

  private transient volatile CloseableHttpClient httpClient;

  /** Returns {@code true} if {@code location} is an HTTP(S) URL. */
  public static boolean isHttpUrl(String location) {
    return location != null
        && (location.regionMatches(true, 0, "https://", 0, 8)
            || location.regionMatches(true, 0, "http://", 0, 7));
  }

  /**
   * Returns an {@link InputFile} that reads {@code location} directly over HTTP(S).
   *
   * @param location an HTTP(S) URL; see {@link #isHttpUrl(String)}
   */
  public InputFile newInputFile(String location) {
    return new HTTPInputFile(httpClient(), location, location);
  }

  /**
   * Returns an {@link InputFile} of the given {@code length} that reads {@code location} directly
   * over HTTP(S).
   *
   * @param location an HTTP(S) URL; see {@link #isHttpUrl(String)}
   */
  public InputFile newInputFile(String location, long length) {
    return new HTTPInputFile(httpClient(), location, location, length);
  }

  public void close() {
    synchronized (this) {
      if (httpClient != null) {
        httpClient.close(CloseMode.GRACEFUL);
        httpClient = null;
      }
    }
  }

  private CloseableHttpClient httpClient() {
    if (httpClient == null) {
      synchronized (this) {
        if (httpClient == null) {
          this.httpClient = HttpClients.custom().useSystemProperties().build();
        }
      }
    }

    return httpClient;
  }
}
