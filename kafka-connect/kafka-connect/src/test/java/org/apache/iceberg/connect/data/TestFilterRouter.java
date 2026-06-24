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

public class TestFilterRouter {

  private static SinkRecord recordWithTopicAndValue(String topic, Object value) {
    return new SinkRecord(topic, 0, null, null, null, value, 0);
  }

  private static SinkRecord recordWithTopicValueAndHeaders(
      String topic, Object value, ConnectHeaders headers) {
    return new SinkRecord(topic, 0, null, null, null, value, 0, null, null, headers);
  }

  /**
   * Base config that delegates to TopicNameRouter — the simplest router that doesn't need
   * iceberg.tables config.
   */
  private static ImmutableMap.Builder<String, String> baseConfig() {
    return ImmutableMap.<String, String>builder()
        .put(
            FilterRouter.CONFIG_PREFIX + FilterRouter.DELEGATE_CLASS_PROP,
            "org.apache.iceberg.connect.data.TopicNameRouter");
  }

  @Test
  public void testIncludeTopicRegex() {
    FilterRouter router = new FilterRouter();
    router.configure(
        baseConfig()
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.TOPIC_REGEX_PROP, "events\\..*")
            .buildOrThrow());

    List<RouteTarget> targets =
        router.route(recordWithTopicAndValue("events.clicks", ImmutableMap.of()));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("events.clicks");
  }

  @Test
  public void testIncludeTopicRegexNoMatch() {
    FilterRouter router = new FilterRouter();
    router.configure(
        baseConfig()
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.TOPIC_REGEX_PROP, "events\\..*")
            .buildOrThrow());

    List<RouteTarget> targets =
        router.route(recordWithTopicAndValue("orders.new", ImmutableMap.of()));
    assertThat(targets).isEmpty();
  }

  @Test
  public void testExcludeTopicRegex() {
    FilterRouter router = new FilterRouter();
    router.configure(
        baseConfig()
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.MODE_PROP, "exclude")
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.TOPIC_REGEX_PROP, "test\\..*")
            .buildOrThrow());

    // matching topic should be excluded
    List<RouteTarget> excluded =
        router.route(recordWithTopicAndValue("test.debug", ImmutableMap.of()));
    assertThat(excluded).isEmpty();

    // non-matching topic should pass through
    List<RouteTarget> included =
        router.route(recordWithTopicAndValue("prod.orders", ImmutableMap.of()));
    assertThat(included).hasSize(1);
  }

  @Test
  public void testIncludeFieldRegex() {
    FilterRouter router = new FilterRouter();
    router.configure(
        baseConfig()
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.FIELD_PROP, "env")
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.FIELD_REGEX_PROP, "prod|staging")
            .buildOrThrow());

    List<RouteTarget> targets =
        router.route(recordWithTopicAndValue("topic", ImmutableMap.of("env", "prod")));
    assertThat(targets).hasSize(1);

    List<RouteTarget> dropped =
        router.route(recordWithTopicAndValue("topic", ImmutableMap.of("env", "dev")));
    assertThat(dropped).isEmpty();
  }

  @Test
  public void testExcludeFieldRegex() {
    FilterRouter router = new FilterRouter();
    router.configure(
        baseConfig()
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.MODE_PROP, "exclude")
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.FIELD_PROP, "env")
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.FIELD_REGEX_PROP, "test|dev")
            .buildOrThrow());

    List<RouteTarget> dropped =
        router.route(recordWithTopicAndValue("topic", ImmutableMap.of("env", "test")));
    assertThat(dropped).isEmpty();

    List<RouteTarget> passed =
        router.route(recordWithTopicAndValue("topic", ImmutableMap.of("env", "prod")));
    assertThat(passed).hasSize(1);
  }

  @Test
  public void testFieldExists() {
    FilterRouter router = new FilterRouter();
    router.configure(
        baseConfig()
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.FIELD_EXISTS_PROP, "customer.id")
            .buildOrThrow());

    // field exists → pass
    List<RouteTarget> passed =
        router.route(
            recordWithTopicAndValue(
                "topic", ImmutableMap.of("customer", ImmutableMap.of("id", "123"))));
    assertThat(passed).hasSize(1);

    // field missing → drop
    List<RouteTarget> dropped =
        router.route(
            recordWithTopicAndValue(
                "topic", ImmutableMap.of("customer", ImmutableMap.of("name", "test"))));
    assertThat(dropped).isEmpty();
  }

  @Test
  public void testHeaderFilter() {
    FilterRouter router = new FilterRouter();
    router.configure(
        baseConfig()
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.HEADER_PROP, "processable")
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.HEADER_REGEX_PROP, "true")
            .buildOrThrow());

    ConnectHeaders headers = new ConnectHeaders();
    headers.addBytes("processable", "true".getBytes(StandardCharsets.UTF_8));

    List<RouteTarget> passed =
        router.route(recordWithTopicValueAndHeaders("topic", ImmutableMap.of(), headers));
    assertThat(passed).hasSize(1);

    // no matching header → drop
    List<RouteTarget> dropped = router.route(recordWithTopicAndValue("topic", ImmutableMap.of()));
    assertThat(dropped).isEmpty();
  }

  @Test
  public void testCombinedPredicates() {
    FilterRouter router = new FilterRouter();
    router.configure(
        baseConfig()
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.TOPIC_REGEX_PROP, "events\\..*")
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.FIELD_PROP, "env")
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.FIELD_REGEX_PROP, "prod")
            .buildOrThrow());

    // both predicates match → pass
    List<RouteTarget> passed =
        router.route(recordWithTopicAndValue("events.clicks", ImmutableMap.of("env", "prod")));
    assertThat(passed).hasSize(1);

    // topic matches but field doesn't → drop (AND logic)
    List<RouteTarget> dropped =
        router.route(recordWithTopicAndValue("events.clicks", ImmutableMap.of("env", "dev")));
    assertThat(dropped).isEmpty();

    // field matches but topic doesn't → drop
    List<RouteTarget> dropped2 =
        router.route(recordWithTopicAndValue("orders.new", ImmutableMap.of("env", "prod")));
    assertThat(dropped2).isEmpty();
  }

  @Test
  public void testNoPredicatesConfigured() {
    FilterRouter router = new FilterRouter();
    assertThatThrownBy(() -> router.configure(baseConfig().buildOrThrow()))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("predicate");
  }

  @Test
  public void testNoDelegateConfigured() {
    FilterRouter router = new FilterRouter();
    assertThatThrownBy(
            () ->
                router.configure(
                    ImmutableMap.of(
                        FilterRouter.CONFIG_PREFIX + FilterRouter.TOPIC_REGEX_PROP, ".*")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("delegate");
  }

  @Test
  public void testNullRecordValue() {
    FilterRouter router = new FilterRouter();
    router.configure(
        baseConfig()
            .put(FilterRouter.CONFIG_PREFIX + FilterRouter.FIELD_EXISTS_PROP, "id")
            .buildOrThrow());

    // null record value → field can't exist → drop
    List<RouteTarget> targets = router.route(recordWithTopicAndValue("topic", null));
    assertThat(targets).isEmpty();
  }
}
