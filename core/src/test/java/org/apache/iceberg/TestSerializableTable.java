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

import static org.apache.iceberg.TestHelpers.roundTripSerialize;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSerializableTable {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          required(3, "date", Types.StringType.get()),
          optional(4, "double", Types.DoubleType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("date").build();

  private static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("id").build();

  @TempDir private File temp;

  @AfterAll
  public static void clean() {
    TestTables.clearTables();
  }

  @Test
  public void testSerializableTableWithMetricsReporter()
      throws IOException, ClassNotFoundException {
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.METRICS_REPORTER_IMPL,
            TestMetricsReporter.class.getName(),
            "key1",
            "value1");
    MetricsReporter reporter = CatalogUtil.loadMetricsReporter(properties);
    Table table = TestTables.create(temp, "tbl_A", SCHEMA, SPEC, SORT_ORDER, 2, reporter);
    Table serializableTable = roundTripSerialize(SerializableTable.copyOf(table));
    assertSerializedMetricsReporter(reporter, serializableTable.metricsReporter());

    serializableTable = TestHelpers.KryoHelpers.roundTripSerialize(SerializableTable.copyOf(table));
    assertSerializedMetricsReporter(reporter, serializableTable.metricsReporter());
  }

  private void assertSerializedMetricsReporter(MetricsReporter expected, MetricsReporter actual) {
    Assertions.assertThat(actual).isNotNull().isInstanceOf(TestMetricsReporter.class);
    Assertions.assertThat(actual.properties()).isEqualTo(expected.properties());
    Assertions.assertThat(actual.properties()).containsEntry("key1", "value1");
  }

  public static class TestMetricsReporter implements MetricsReporter {
    private Map<String, String> properties;

    @Override
    public void initialize(Map<String, String> props) {
      this.properties = props;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public void report(MetricsReport report) {}
  }
}
