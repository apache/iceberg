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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestRecordRouterFactory {

  @Test
  public void testDefaultCreatesStaticRouter() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tablesRouterClass()).thenReturn(null);
    when(config.dynamicTablesEnabled()).thenReturn(false);
    when(config.tables()).thenReturn(ImmutableList.of("db.tbl"));
    when(config.tablesRouteField()).thenReturn(null);

    RecordRouter router = RecordRouterFactory.create(config);
    assertThat(router).isInstanceOf(StaticRouter.class);
  }

  @Test
  public void testDynamicCreatesDynamicRouter() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tablesRouterClass()).thenReturn(null);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn("route_field");

    RecordRouter router = RecordRouterFactory.create(config);
    assertThat(router).isInstanceOf(DynamicRouter.class);
  }

  @Test
  public void testCustomClassLoadsTopicNameRouter() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tablesRouterClass()).thenReturn("org.apache.iceberg.connect.data.TopicNameRouter");
    when(config.originalProps()).thenReturn(ImmutableMap.of());

    RecordRouter router = RecordRouterFactory.create(config);
    assertThat(router).isInstanceOf(TopicNameRouter.class);
  }

  @Test
  public void testInvalidClassThrows() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tablesRouterClass()).thenReturn("com.example.NonExistentRouter");
    when(config.originalProps()).thenReturn(ImmutableMap.of());

    assertThatThrownBy(() -> RecordRouterFactory.create(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("NonExistentRouter");
  }
}
