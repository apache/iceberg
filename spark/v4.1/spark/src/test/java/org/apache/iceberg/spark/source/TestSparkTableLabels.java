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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ImmutableFieldLabel;
import org.apache.iceberg.ImmutableLabels;
import org.apache.iceberg.Labels;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.metrics.LoggingMetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestSparkTableLabels {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File temp;

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  @Test
  public void labelsAreSurfacedUnderTheirNaturalKeys() {
    Labels labels =
        ImmutableLabels.builder()
            .objectLabels(ImmutableMap.of("owner", "team-a"))
            .addFields(
                ImmutableFieldLabel.builder()
                    .fieldId(1)
                    .labels(ImmutableMap.of("classification", "pii"))
                    .build())
            .build();

    Map<String, String> properties = sparkProperties(ImmutableMap.of(), labels);

    assertThat(properties)
        .containsEntry("labels.object.owner", "team-a")
        .containsEntry("labels.field.1.classification", "pii");
  }

  @Test
  public void collidingLabelsAndPropertiesRemainBothVisible() {
    Map<String, String> stored =
        ImmutableMap.of(
            "labels.object.owner", "stored-owner",
            "labels.field.1.classification", "stored-pii");

    Labels labels =
        ImmutableLabels.builder()
            .objectLabels(ImmutableMap.of("owner", "catalog-owner", "team", "catalog-team"))
            .addFields(
                ImmutableFieldLabel.builder()
                    .fieldId(1)
                    .labels(ImmutableMap.of("classification", "catalog-pii"))
                    .build())
            .build();

    Map<String, String> properties = sparkProperties(stored, labels);

    // stored table properties keep their original keys
    assertThat(properties)
        .containsEntry("labels.object.owner", "stored-owner")
        .containsEntry("labels.field.1.classification", "stored-pii");

    // colliding catalog labels are surfaced under de-conflicted keys, for both prefixes
    assertThat(properties)
        .containsEntry("labels.object.owner.catalog", "catalog-owner")
        .containsEntry("labels.field.1.classification.catalog", "catalog-pii");

    // a non-colliding label still uses its natural key
    assertThat(properties).containsEntry("labels.object.team", "catalog-team");
  }

  private Map<String, String> sparkProperties(Map<String, String> stored, Labels labels) {
    TestTables.TestTable created =
        TestTables.create(temp, "tbl", SCHEMA, PartitionSpec.unpartitioned(), 2);
    if (!stored.isEmpty()) {
      UpdateProperties update = created.updateProperties();
      stored.forEach(update::set);
      update.commit();
    }

    BaseTable table =
        new BaseTable(created.operations(), "tbl", LoggingMetricsReporter.instance(), labels);
    return new LabelsSparkTable(table).properties();
  }

  /** Minimal concrete {@link BaseSparkTable} to exercise {@link BaseSparkTable#properties()}. */
  private static class LabelsSparkTable extends BaseSparkTable {
    LabelsSparkTable(Table table) {
      super(table, table.schema());
    }

    @Override
    public Set<TableCapability> capabilities() {
      return ImmutableSet.of();
    }
  }
}
