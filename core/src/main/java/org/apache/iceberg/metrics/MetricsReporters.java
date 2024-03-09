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

import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class that allows combining two given {@link MetricsReporter} instances. */
public class MetricsReporters {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsReporters.class);

  private MetricsReporters() {}

  public static MetricsReporter combine(MetricsReporter first, MetricsReporter second) {
    if (null == first) {
      return second;
    } else if (null == second || first == second) {
      return first;
    }

    Set<MetricsReporter> reporters = Sets.newIdentityHashSet();

    if (first instanceof CompositeMetricsReporter) {
      reporters.addAll(((CompositeMetricsReporter) first).reporters());
    } else {
      reporters.add(first);
    }

    if (second instanceof CompositeMetricsReporter) {
      reporters.addAll(((CompositeMetricsReporter) second).reporters());
    } else {
      reporters.add(second);
    }

    return new CompositeMetricsReporter(reporters);
  }

  @VisibleForTesting
  static class CompositeMetricsReporter implements MetricsReporter {
    private final Set<MetricsReporter> reporters;

    private CompositeMetricsReporter(Set<MetricsReporter> reporters) {
      this.reporters = reporters;
    }

    @Override
    public void report(MetricsReport report) {
      for (MetricsReporter reporter : reporters) {
        try {
          reporter.report(report);
        } catch (Exception e) {
          LOG.warn(
              "Could not report {} to {}",
              report.getClass().getName(),
              reporter.getClass().getName(),
              e);
        }
      }
    }

    Set<MetricsReporter> reporters() {
      return Collections.unmodifiableSet(reporters);
    }
  }
}
