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

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class TestHeaderRouter {

  private static SinkRecord recordWithHeaders(ConnectHeaders headers) {
    SinkRecord record =
        new SinkRecord("topic", 0, null, null, null, ImmutableMap.of(), 0, null, null, headers);
    return record;
  }

  private static SinkRecord recordWithNoHeaders() {
    return new SinkRecord("topic", 0, null, null, null, ImmutableMap.of(), 0);
  }

  private static ConnectHeaders headersWithString(String name, String value) {
    ConnectHeaders headers = new ConnectHeaders();
    headers.addBytes(name, value.getBytes(StandardCharsets.UTF_8));
    return headers;
  }

  @Test
  public void testSimpleHeaderAsTable() {
    HeaderRouter router = new HeaderRouter();
    router.configure(
        ImmutableMap.of(HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "iceberg.table"));

    List<RouteTarget> targets =
        router.route(recordWithHeaders(headersWithString("iceberg.table", "db.orders")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("db.orders");
  }

  @Test
  public void testHeaderWithNamespace() {
    HeaderRouter router = new HeaderRouter();
    router.configure(
        ImmutableMap.of(
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "event-type",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.TABLE_NAMESPACE_PROP, "events"));

    List<RouteTarget> targets =
        router.route(recordWithHeaders(headersWithString("event-type", "UserSignup")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("events.usersignup");
  }

  @Test
  public void testHeaderExplicitMap() {
    HeaderRouter router = new HeaderRouter();
    router.configure(
        ImmutableMap.of(
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "route",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.TABLE_MAP_PREFIX + "user", "analytics.users",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.TABLE_MAP_PREFIX + "order",
                "analytics.orders"));

    List<RouteTarget> targets = router.route(recordWithHeaders(headersWithString("route", "user")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("analytics.users");
  }

  @Test
  public void testHeaderExplicitMapBypassesTransforms() {
    HeaderRouter router = new HeaderRouter();
    router.configure(
        ImmutableMap.of(
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "route",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.TABLE_NAMESPACE_PROP, "ns",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.TABLE_MAP_PREFIX + "special",
                "custom.table"));

    List<RouteTarget> targets =
        router.route(recordWithHeaders(headersWithString("route", "special")));
    assertThat(targets).hasSize(1);
    // explicit map should NOT have namespace prepended
    assertThat(targets.get(0).tableName()).isEqualTo("custom.table");
  }

  @Test
  public void testHeaderRegex() {
    HeaderRouter router = new HeaderRouter();
    router.configure(
        ImmutableMap.of(
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "dest",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.REGEX_PROP, ".*\\.(.+)",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.REGEX_REPLACEMENT_PROP, "$1"));

    List<RouteTarget> targets =
        router.route(recordWithHeaders(headersWithString("dest", "prod.clicks")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("clicks");
  }

  @Test
  public void testHeaderRegexNoMatch() {
    HeaderRouter router = new HeaderRouter();
    router.configure(
        ImmutableMap.of(
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "dest",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.REGEX_PROP, "^events\\.(.+)$"));

    List<RouteTarget> targets =
        router.route(recordWithHeaders(headersWithString("dest", "orders")));
    assertThat(targets).isEmpty();
  }

  @Test
  public void testMissingHeaderDrop() {
    HeaderRouter router = new HeaderRouter();
    router.configure(
        ImmutableMap.of(HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "missing"));

    List<RouteTarget> targets = router.route(recordWithNoHeaders());
    assertThat(targets).isEmpty();
  }

  @Test
  public void testMissingHeaderFail() {
    HeaderRouter router = new HeaderRouter();
    router.configure(
        ImmutableMap.of(
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "missing",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.ON_MISSING_HEADER_PROP, "fail"));

    assertThatThrownBy(() -> router.route(recordWithNoHeaders()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing");
  }

  @Test
  public void testMissingHeaderDefault() {
    HeaderRouter router = new HeaderRouter();
    router.configure(
        ImmutableMap.of(
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "missing",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.ON_MISSING_HEADER_PROP,
                "default:fallback.table"));

    List<RouteTarget> targets = router.route(recordWithNoHeaders());
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("fallback.table");
  }

  @Test
  public void testMultiValueFirst() {
    HeaderRouter router = new HeaderRouter();
    router.configure(
        ImmutableMap.of(
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "dest",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.MULTI_VALUE_PROP, "first"));

    ConnectHeaders headers = new ConnectHeaders();
    headers.addBytes("dest", "first_table".getBytes(StandardCharsets.UTF_8));
    headers.addBytes("dest", "second_table".getBytes(StandardCharsets.UTF_8));

    List<RouteTarget> targets = router.route(recordWithHeaders(headers));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("first_table");
  }

  @Test
  public void testMultiValueAll() {
    HeaderRouter router = new HeaderRouter();
    router.configure(
        ImmutableMap.of(
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "dest",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.MULTI_VALUE_PROP, "all",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.LOWERCASE_PROP, "false"));

    ConnectHeaders headers = new ConnectHeaders();
    headers.addBytes("dest", "tableA".getBytes(StandardCharsets.UTF_8));
    headers.addBytes("dest", "tableB".getBytes(StandardCharsets.UTF_8));

    List<RouteTarget> targets = router.route(recordWithHeaders(headers));
    assertThat(targets).hasSize(2);
    assertThat(targets.get(0).tableName()).isEqualTo("tableA");
    assertThat(targets.get(1).tableName()).isEqualTo("tableB");
  }

  @Test
  public void testMultiValueLast() {
    HeaderRouter router = new HeaderRouter();
    router.configure(
        ImmutableMap.of(
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "dest",
            HeaderRouter.CONFIG_PREFIX + HeaderRouter.MULTI_VALUE_PROP, "last"));

    ConnectHeaders headers = new ConnectHeaders();
    headers.addBytes("dest", "first_table".getBytes(StandardCharsets.UTF_8));
    headers.addBytes("dest", "last_table".getBytes(StandardCharsets.UTF_8));

    List<RouteTarget> targets = router.route(recordWithHeaders(headers));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("last_table");
  }

  @Test
  public void testNoHeaderNameConfig() {
    HeaderRouter router = new HeaderRouter();
    assertThatThrownBy(() -> router.configure(ImmutableMap.of()))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("name");
  }

  @Test
  public void testIgnoreMissingTableDefault() {
    HeaderRouter router = new HeaderRouter();
    router.configure(ImmutableMap.of(HeaderRouter.CONFIG_PREFIX + HeaderRouter.NAME_PROP, "dest"));

    List<RouteTarget> targets =
        router.route(recordWithHeaders(headersWithString("dest", "orders")));
    assertThat(targets.get(0).ignoreMissingTable()).isTrue();
  }
}
