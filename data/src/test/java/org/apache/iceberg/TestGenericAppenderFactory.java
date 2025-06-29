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

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.TestAppenderFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.TestTemplate;

public class TestGenericAppenderFactory extends TestAppenderFactory<Record> {

  private final GenericRecord gRecord = GenericRecord.create(SCHEMA);

  @Override
  protected FileAppenderFactory<Record> createAppenderFactory(
      List<Integer> equalityFieldIds, Schema eqDeleteSchema, Schema posDeleteRowSchema) {
    return new GenericAppenderFactory(
        table,
        table.schema(),
        table.spec(),
        Maps.newHashMap(),
        ArrayUtil.toIntArray(equalityFieldIds),
        eqDeleteSchema,
        posDeleteRowSchema);
  }

  @Override
  protected Record createRow(Integer id, String data) {
    return gRecord.copy(ImmutableMap.of("id", id, "data", data));
  }

  @Override
  protected StructLikeSet expectedRowSet(Iterable<Record> records) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    records.forEach(set::add);
    return set;
  }

  @TestTemplate
  void illegalSetConfig() {
    GenericAppenderFactory appenderFactory =
        (GenericAppenderFactory) createAppenderFactory(null, null, null);

    assertThatThrownBy(
            () ->
                appenderFactory.set(
                    TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS,
                    MetricsModes.None.get().toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot set metrics properties, use table properties instead");
  }

  @TestTemplate
  void illegalSetAllConfigs() {
    GenericAppenderFactory appenderFactory =
        (GenericAppenderFactory) createAppenderFactory(null, null, null);

    Map<String, String> properties =
        ImmutableMap.of(
            TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS,
            "10",
            TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id",
            MetricsModes.Full.get().toString());

    assertThatThrownBy(() -> appenderFactory.setAll(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot set metrics properties, use table properties instead");
  }

  @TestTemplate
  void setConfigExcludeMetrics() {
    GenericAppenderFactory appenderFactory =
        (GenericAppenderFactory) createAppenderFactory(null, null, null);
    assertThatNoException().isThrownBy(() -> appenderFactory.set("key1", "value1"));
    assertThatNoException()
        .isThrownBy(() -> appenderFactory.setAll(ImmutableMap.of("key2", "value2")));
  }

  @TestTemplate
  void setConfigContainsMetricsConfig() {
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(SCHEMA);
    assertThatThrownBy(
            () -> appenderFactory.set(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "10"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot set metrics properties, use table properties instead");
    assertThatThrownBy(
            () ->
                appenderFactory.setAll(
                    ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "full")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot set metrics properties, use table properties instead");
  }

  @TestTemplate
  void createFactoryWithConflictConfig() {
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, MetricsModes.Full.get().toString())
        .commit();
    Map<String, String> config =
        ImmutableMap.of(
            TableProperties.DEFAULT_WRITE_METRICS_MODE, MetricsModes.None.get().toString());

    assertThatThrownBy(
            () -> new GenericAppenderFactory(table, SCHEMA, SPEC, config, null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot set metrics properties, use table properties instead");
  }
}
