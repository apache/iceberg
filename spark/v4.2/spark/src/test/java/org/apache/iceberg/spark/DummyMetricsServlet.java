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
package org.apache.iceberg.spark;

import com.codahale.metrics.MetricRegistry;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.metrics.sink.MetricsServlet;
import org.sparkproject.jetty.servlet.ServletContextHandler;

/**
 * A dummy implementation of {@link MetricsServlet} that does not start a server or report metrics.
 * This is used in tests to avoid conflicts with Spark's jetty dependencies.
 */
public class DummyMetricsServlet extends MetricsServlet {

  /**
   * Constructor required by Spark's reflection-based instantiation.
   *
   * @param properties Metrics properties
   * @param registry Metric registry
   */
  public DummyMetricsServlet(Properties properties, MetricRegistry registry) {
    super(properties, registry);
  }

  @Override
  public ServletContextHandler[] getHandlers(SparkConf conf) {
    return new ServletContextHandler[] {};
  }

  @Override
  public void start() {
    // No-op for tests
  }

  @Override
  public void stop() {
    // No-op for tests
  }

  @Override
  public void report() {
    // No-op for tests
  }
}
