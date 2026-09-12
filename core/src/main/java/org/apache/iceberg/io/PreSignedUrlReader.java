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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates input files for locations that are pre-signed URLs: {@code http} or {@code https} URLs
 * carrying their own authorization.
 */
public class PreSignedUrlReader implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(PreSignedUrlReader.class);
  private static final String DEFAULT_METRICS_IMPL =
      "org.apache.iceberg.hadoop.HadoopMetricsContext";
  private static final String METRICS_PREFIX = "presigned-url";

  /** Pool size, in total and per host. */
  public static final String MAX_CONNECTIONS = "presigned-url.max-connections";

  public static final int MAX_CONNECTIONS_DEFAULT = 100;

  // the AWS SDK's HTTP defaults, as in S3FileIO; HttpClient's own socket timeout is unbounded
  private static final long CONNECT_TIMEOUT_MS = 2_000;
  private static final int SOCKET_TIMEOUT_MS = 30_000;

  private final Map<String, String> properties;
  private final MetricsContext metrics;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile CloseableHttpClient http;

  public PreSignedUrlReader(Map<String, String> properties) {
    this.properties = Collections.unmodifiableMap(Maps.newHashMap(properties));
    this.metrics = loadMetrics(this.properties);
  }

  /** Whether {@code location} is a URL this reader handles rather than a storage location. */
  public static boolean handles(String location) {
    String lower = location.toLowerCase(Locale.ROOT);
    return lower.startsWith("https://") || lower.startsWith("http://");
  }

  /**
   * Returns an input file that reads {@code location} as given.
   *
   * @param location an {@code http} or {@code https} URL
   * @param length the file length if known, otherwise {@code 0}; when unknown it is determined by a
   *     single-byte range GET on first use
   */
  public InputFile newInputFile(String location, long length) {
    Preconditions.checkArgument(handles(location), "Not an http or https URL: %s", location);
    Preconditions.checkState(!closed.get(), "Cannot read %s: reader is closed", location);
    return new PreSignedUrlInputFile(http(), location, length, metrics);
  }

  private CloseableHttpClient http() {
    if (null == http) {
      synchronized (this) {
        if (null == http) {
          int maxConnections =
              PropertyUtil.propertyAsInt(properties, MAX_CONNECTIONS, MAX_CONNECTIONS_DEFAULT);
          ConnectionConfig connectionConfig =
              ConnectionConfig.custom()
                  .setConnectTimeout(CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                  .setSocketTimeout(SOCKET_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                  .build();
          // the signed request is the whole request: no redirects, no content coding, no cookies
          this.http =
              HttpClients.custom()
                  .setConnectionManager(
                      PoolingHttpClientConnectionManagerBuilder.create()
                          .setMaxConnTotal(maxConnections)
                          .setMaxConnPerRoute(maxConnections)
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
  private static MetricsContext loadMetrics(Map<String, String> props) {
    try {
      DynConstructors.Ctor<MetricsContext> ctor =
          DynConstructors.builder(MetricsContext.class)
              .hiddenImpl(DEFAULT_METRICS_IMPL, String.class)
              .buildChecked();
      MetricsContext context = ctor.newInstance(METRICS_PREFIX);
      context.initialize(props);
      return context;
    } catch (NoClassDefFoundError | NoSuchMethodException | ClassCastException e) {
      LOG.warn(
          "Unable to load metrics class: '{}', falling back to null metrics", DEFAULT_METRICS_IMPL);
      return MetricsContext.nullMetrics();
    }
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true) && http != null) {
      try {
        http.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
