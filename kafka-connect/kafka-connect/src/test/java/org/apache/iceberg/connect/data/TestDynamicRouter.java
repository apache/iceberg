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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class TestDynamicRouter {

  private static final String TABLE_NAME = "db.tbl";
  private static final String ROUTE_FIELD = "fld";

  private static SinkRecord recordWithValue(Object value) {
    return new SinkRecord("topic", 0, null, null, null, value, 0);
  }

  @Test
  public void testDynamicRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tablesRouteField()).thenReturn(ROUTE_FIELD);

    DynamicRouter router = new DynamicRouter();
    router.configure(config);

    List<RouteTarget> targets =
        router.route(recordWithValue(ImmutableMap.of(ROUTE_FIELD, TABLE_NAME)));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo(TABLE_NAME);
    assertThat(targets.get(0).ignoreMissingTable()).isTrue();
  }

  @Test
  public void testDynamicNoRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tablesRouteField()).thenReturn(ROUTE_FIELD);

    DynamicRouter router = new DynamicRouter();
    router.configure(config);

    // field exists but with null value via missing key
    List<RouteTarget> targets =
        router.route(recordWithValue(ImmutableMap.of("other_field", "value")));
    assertThat(targets).isEmpty();
  }

  @Test
  public void testDynamicRouteLowercase() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tablesRouteField()).thenReturn(ROUTE_FIELD);

    DynamicRouter router = new DynamicRouter();
    router.configure(config);

    List<RouteTarget> targets =
        router.route(recordWithValue(ImmutableMap.of(ROUTE_FIELD, "DB.MyTable")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("db.mytable");
  }

  @Test
  public void testDynamicNullValue() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tablesRouteField()).thenReturn(ROUTE_FIELD);

    DynamicRouter router = new DynamicRouter();
    router.configure(config);

    List<RouteTarget> targets = router.route(recordWithValue(null));
    assertThat(targets).isEmpty();
  }
}
