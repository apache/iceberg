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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import org.apache.iceberg.metrics.LoggingMetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.labels.ImmutableFieldLabels;
import org.apache.iceberg.rest.labels.ImmutableLabels;
import org.apache.iceberg.rest.labels.Labels;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestBaseTableLabels {

  private static final String TABLE_NAME = "tbl";

  @TempDir private File temp;

  // Only the labels accessor is exercised here, not table operations, so no metadata is set up.
  @Test
  public void labelsDefaultToEmptyWhenNotProvided() {
    BaseTable table =
        new BaseTable(new TestTables.TestTableOperations(TABLE_NAME, temp), TABLE_NAME);

    assertThat(table).isInstanceOf(SupportsLabels.class);
    assertThat(table.labels().isEmpty()).isTrue();
  }

  @Test
  public void labelsAreExposedWhenProvided() {
    Labels labels =
        ImmutableLabels.builder()
            .objectLabels(ImmutableMap.of("owner", "team-a"))
            .addFields(
                ImmutableFieldLabels.builder()
                    .fieldId(1)
                    .labels(ImmutableMap.of("classification", "pii"))
                    .build())
            .build();

    BaseTable table =
        new BaseTable(
            new TestTables.TestTableOperations(TABLE_NAME, temp),
            TABLE_NAME,
            LoggingMetricsReporter.instance(),
            labels);

    assertThat(table.labels()).isEqualTo(labels);
    assertThat(table.labels().objectLabels()).containsEntry("owner", "team-a");
    assertThat(table.labels().fields()).hasSize(1);
  }
}
