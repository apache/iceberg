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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class TestMetricsReporters {
  @Test
  public void combineWithNullReporter() {
    MetricsReporter reporter = report -> {};
    assertThat(MetricsReporters.combine(null, null)).isNull();
    assertThat(MetricsReporters.combine(null, reporter)).isSameAs(reporter);
    assertThat(MetricsReporters.combine(reporter, null)).isSameAs(reporter);
  }

  @Test
  public void combineSameInstances() {
    MetricsReporter reporter = LoggingMetricsReporter.instance();
    assertThat(MetricsReporters.combine(reporter, reporter)).isSameAs(reporter);
  }

  @Test
  public void combineSameClassButDifferentInstances() {
    MetricsReporter first = LoggingMetricsReporter.instance();
    MetricsReporter second = new LoggingMetricsReporter();

    MetricsReporter combined = MetricsReporters.combine(first, second);
    assertThat(combined).isInstanceOf(MetricsReporters.CompositeMetricsReporter.class);
    assertThat(((MetricsReporters.CompositeMetricsReporter) combined).reporters())
        .hasSize(2)
        .containsExactlyInAnyOrder(first, second);
  }

  @Test
  public void combineSimpleReporters() {
    MetricsReporter first = report -> {};
    MetricsReporter second = report -> {};

    MetricsReporter combined = MetricsReporters.combine(first, second);
    assertThat(combined).isInstanceOf(MetricsReporters.CompositeMetricsReporter.class);
    assertThat(((MetricsReporters.CompositeMetricsReporter) combined).reporters())
        .hasSize(2)
        .containsExactlyInAnyOrder(first, second);
  }

  @Test
  public void combineComposites() {
    MetricsReporter one = report -> {};
    MetricsReporter two = report -> {};
    MetricsReporter firstComposite =
        MetricsReporters.combine(one, LoggingMetricsReporter.instance());
    MetricsReporter secondComposite =
        MetricsReporters.combine(two, LoggingMetricsReporter.instance());

    MetricsReporter combined = MetricsReporters.combine(firstComposite, two);
    assertThat(combined).isInstanceOf(MetricsReporters.CompositeMetricsReporter.class);
    assertThat(((MetricsReporters.CompositeMetricsReporter) combined).reporters())
        .hasSize(3)
        .containsExactlyInAnyOrder(one, two, LoggingMetricsReporter.instance());

    combined = MetricsReporters.combine(firstComposite, secondComposite);
    assertThat(combined).isInstanceOf(MetricsReporters.CompositeMetricsReporter.class);
    assertThat(((MetricsReporters.CompositeMetricsReporter) combined).reporters())
        .hasSize(3)
        .containsExactlyInAnyOrder(one, two, LoggingMetricsReporter.instance());
  }

  @Test
  public void reportWithMultipleMetricsReporters() {
    AtomicInteger counter = new AtomicInteger();
    MetricsReporter combined =
        MetricsReporters.combine(
            report -> counter.incrementAndGet(), report -> counter.incrementAndGet());

    combined.report(new MetricsReport() {});

    assertThat(combined).isInstanceOf(MetricsReporters.CompositeMetricsReporter.class);
    assertThat(((MetricsReporters.CompositeMetricsReporter) combined).reporters()).hasSize(2);
    assertThat(counter.get()).isEqualTo(2);
  }

  @Test
  public void reportWithMultipleMetricsReportersOneFails() {
    AtomicInteger counter = new AtomicInteger();
    MetricsReporter combined =
        MetricsReporters.combine(
            MetricsReporters.combine(
                report -> counter.incrementAndGet(),
                report -> {
                  throw new RuntimeException("invalid report");
                }),
            report -> counter.incrementAndGet());

    combined.report(new MetricsReport() {});

    assertThat(combined).isInstanceOf(MetricsReporters.CompositeMetricsReporter.class);
    assertThat(((MetricsReporters.CompositeMetricsReporter) combined).reporters()).hasSize(3);
    assertThat(counter.get()).isEqualTo(2);
  }
}
