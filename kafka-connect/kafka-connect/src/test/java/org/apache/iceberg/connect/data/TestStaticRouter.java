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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TableSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class TestStaticRouter {

  private static final String TABLE_NAME = "db.tbl";
  private static final String ROUTE_FIELD = "fld";

  private static SinkRecord recordWithValue(Object value) {
    return new SinkRecord("topic", 0, null, null, null, value, 0);
  }

  @Test
  public void testDefaultRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.tables()).thenReturn(ImmutableList.of(TABLE_NAME));

    StaticRouter router = new StaticRouter();
    router.configure(config);

    List<RouteTarget> targets = router.route(recordWithValue(ImmutableMap.of()));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo(TABLE_NAME);
    assertThat(targets.get(0).ignoreMissingTable()).isFalse();
  }

  @Test
  public void testDefaultNoRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.tables()).thenReturn(ImmutableList.of());

    StaticRouter router = new StaticRouter();
    router.configure(config);

    List<RouteTarget> targets = router.route(recordWithValue(ImmutableMap.of()));
    assertThat(targets).isEmpty();
  }

  @Test
  public void testStaticRoute() {
    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.routeRegex()).thenReturn(Pattern.compile("val"));

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tables()).thenReturn(ImmutableList.of(TABLE_NAME));
    when(config.tableConfig(any())).thenReturn(tableConfig);
    when(config.tablesRouteField()).thenReturn(ROUTE_FIELD);

    StaticRouter router = new StaticRouter();
    router.configure(config);

    List<RouteTarget> targets = router.route(recordWithValue(ImmutableMap.of(ROUTE_FIELD, "val")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo(TABLE_NAME);
  }

  @Test
  public void testStaticNoRoute() {
    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    when(tableConfig.routeRegex()).thenReturn(Pattern.compile("val"));

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tables()).thenReturn(ImmutableList.of(TABLE_NAME));
    when(config.tableConfig(any())).thenReturn(tableConfig);
    when(config.tablesRouteField()).thenReturn(ROUTE_FIELD);

    StaticRouter router = new StaticRouter();
    router.configure(config);

    List<RouteTarget> targets =
        router.route(recordWithValue(ImmutableMap.of(ROUTE_FIELD, "foobar")));
    assertThat(targets).isEmpty();
  }

  @Test
  public void testNullRecordValue() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tables()).thenReturn(ImmutableList.of(TABLE_NAME));
    when(config.tablesRouteField()).thenReturn(ROUTE_FIELD);

    StaticRouter router = new StaticRouter();
    router.configure(config);

    List<RouteTarget> targets = router.route(recordWithValue(null));
    assertThat(targets).isEmpty();
  }
}
