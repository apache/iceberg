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
package org.apache.iceberg.metrics;

import java.io.Closeable;
import java.util.Map;

/** This interface defines the basic API for reporting metrics for operations to a Table. */
@FunctionalInterface
public interface MetricsReporter extends Closeable {

  /**
   * A custom MetricsReporter implementation must have a no-arg constructor, which will be called
   * first. {@link MetricsReporter#initialize(Map properties)} is called to complete the
   * initialization.
   *
   * @param properties properties
   */
  default void initialize(Map<String, String> properties) {}

  /**
   * Indicates that an operation is done by reporting a {@link MetricsReport}. A {@link
   * MetricsReport} is usually directly derived from a {@link MetricsReport} instance.
   *
   * @param report The {@link MetricsReport} to report.
   */
  void report(MetricsReport report);

  @Override
  default void close() {}
}
