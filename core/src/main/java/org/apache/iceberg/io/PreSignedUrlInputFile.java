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
package org.apache.iceberg.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link InputFile} addressed by a pre-signed URL: an {@code http} or {@code https} URL carrying
 * its own authorization. {@link #location()} is the URL itself.
 *
 * <p>All instances share one HTTP client.
 */
public class PreSignedUrlInputFile implements InputFile {

  private static final Logger LOG = LoggerFactory.getLogger(PreSignedUrlInputFile.class);
  private static final String DEFAULT_METRICS_IMPL =
      "org.apache.iceberg.hadoop.HadoopMetricsContext";
  private static final String METRICS_PREFIX = "presigned-url";

  // pool size, in total and per host
  private static final int MAX_CONNECTIONS = 100;
  // the AWS SDK's HTTP defaults, as in S3FileIO; HttpClient's own socket timeout is unbounded
  private static final long CONNECT_TIMEOUT_MS = 2_000;
  private static final int SOCKET_TIMEOUT_MS = 30_000;

  private static volatile CloseableHttpClient http;
  private static volatile MetricsContext metrics;

  private final String url;
  private Long length;

  private PreSignedUrlInputFile(String url, long length) {
    this.url = url;
    this.length = length > 0 ? length : null;
  }

  /** Whether {@code location} is an {@code http} or {@code https} URL. */
  public static boolean isHttpUrl(String location) {
    String lower = location.toLowerCase(Locale.ROOT);
    return lower.startsWith("https://") || lower.startsWith("http://");
  }

  /**
   * Returns an input file that reads {@code url} as given.
   *
   * @param url an {@code http} or {@code https} URL
   * @param length the file length if known, otherwise {@code 0}
   */
  public static InputFile of(String url, long length) {
    Preconditions.checkArgument(isHttpUrl(url), "Not an http or https URL: %s", url);
    return new PreSignedUrlInputFile(url, length);
  }

  /**
   * The length comes from the caller; {@code 0} means unknown, as in {@code S3InputFile}, and is
   * then read from {@code Content-Range} on a single-byte range GET. HEAD is not an option, as the
   * method is part of the signature and pre-signed URLs are signed for GET.
   */
  @Override
  public long getLength() {
    if (length == null) {
      this.length = probeLength();
    }

    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new PreSignedUrlInputStream(http(), url, metrics());
  }

  @Override
  public String location() {
    return url;
  }

  @Override
  public boolean exists() {
    try {
      long probed = probeLength();
      if (length == null) {
        this.length = probed;
      }

      return true;
    } catch (NotFoundException e) {
      return false;
    }
  }

  /** One single-byte range GET; the same request answers both the length and existence. */
  private long probeLength() {
    HttpGet get = new HttpGet(url);
    get.setHeader("Range", "bytes=0-0");
    try {
      ClassicHttpResponse response = http().executeOpen(null, get, null);
      int code = response.getCode();
      if (code != HttpStatus.SC_PARTIAL_CONTENT
          && code != HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
        throw PreSignedUrlInputStream.failure("Length probe", url, response);
      }

      // Content-Range: bytes 0-0/<length>, or bytes */<length> for an empty object
      Header contentRange = response.getFirstHeader("Content-Range");
      PreSignedUrlInputStream.discard(response);
      if (contentRange == null || !contentRange.getValue().contains("/")) {
        throw new IOException("Length probe of " + url + " returned no Content-Range");
      }

      String value = contentRange.getValue();
      return Long.parseLong(value.substring(value.lastIndexOf('/') + 1).trim());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static CloseableHttpClient http() {
    if (null == http) {
      synchronized (PreSignedUrlInputFile.class) {
        if (null == http) {
          ConnectionConfig connectionConfig =
              ConnectionConfig.custom()
                  .setConnectTimeout(CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                  .setSocketTimeout(SOCKET_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                  .build();
          // the signed request is the whole request: no redirects, no content coding, no cookies
          http =
              HttpClients.custom()
                  .setConnectionManager(
                      PoolingHttpClientConnectionManagerBuilder.create()
                          .setMaxConnTotal(MAX_CONNECTIONS)
                          .setMaxConnPerRoute(MAX_CONNECTIONS)
                          .setDefaultConnectionConfig(connectionConfig)
                          .build())
                  .disableRedirectHandling()
                  .disableContentCompression()
                  .disableCookieManagement()
                  .disableAuthCaching()
                  .build();
        }
      }
    }

    return http;
  }

  @SuppressWarnings("CatchBlockLogException")
  private static MetricsContext metrics() {
    if (null == metrics) {
      synchronized (PreSignedUrlInputFile.class) {
        if (null == metrics) {
          try {
            DynConstructors.Ctor<MetricsContext> ctor =
                DynConstructors.builder(MetricsContext.class)
                    .hiddenImpl(DEFAULT_METRICS_IMPL, String.class)
                    .buildChecked();
            MetricsContext context = ctor.newInstance(METRICS_PREFIX);
            context.initialize(Map.of());
            metrics = context;
          } catch (NoClassDefFoundError | NoSuchMethodException | ClassCastException e) {
            LOG.warn(
                "Unable to load metrics class: '{}', falling back to null metrics",
                DEFAULT_METRICS_IMPL);
            metrics = MetricsContext.nullMetrics();
          }
        }
      }
    }

    return metrics;
  }
}
