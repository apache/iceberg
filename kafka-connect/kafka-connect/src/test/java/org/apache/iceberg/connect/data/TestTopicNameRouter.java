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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class TestTopicNameRouter {

  private static SinkRecord recordWithTopic(String topic) {
    return new SinkRecord(topic, 0, null, null, null, ImmutableMap.of(), 0);
  }

  @Test
  public void testSimpleTopicAsTable() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(ImmutableMap.of());

    List<RouteTarget> targets = router.route(recordWithTopic("orders"));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("orders");
    assertThat(targets.get(0).ignoreMissingTable()).isTrue();
  }

  @Test
  public void testNamespacePrepended() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(
        ImmutableMap.of(
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.TABLE_NAMESPACE_PROP, "analytics"));

    List<RouteTarget> targets = router.route(recordWithTopic("orders"));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("analytics.orders");
  }

  @Test
  public void testExplicitMapping() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(
        ImmutableMap.of(
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.TABLE_MAP_PREFIX + "orders-v2",
            "analytics.orders"));

    List<RouteTarget> targets = router.route(recordWithTopic("orders-v2"));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("analytics.orders");
  }

  @Test
  public void testExplicitMappingBypassesNamespace() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(
        ImmutableMap.of(
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.TABLE_NAMESPACE_PROP, "db",
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.TABLE_MAP_PREFIX + "orders-v2",
                "analytics.orders"));

    List<RouteTarget> targets = router.route(recordWithTopic("orders-v2"));
    assertThat(targets).hasSize(1);
    // explicit map should NOT have namespace prepended
    assertThat(targets.get(0).tableName()).isEqualTo("analytics.orders");
  }

  @Test
  public void testRegexTransform() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(
        ImmutableMap.of(
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.REGEX_PROP, ".*\\.(.+)",
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.REGEX_REPLACEMENT_PROP, "$1"));

    List<RouteTarget> targets = router.route(recordWithTopic("prod.clicks"));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("clicks");
  }

  @Test
  public void testRegexNoMatch() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(
        ImmutableMap.of(
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.REGEX_PROP, "^events\\.(.+)$"));

    List<RouteTarget> targets = router.route(recordWithTopic("orders"));
    assertThat(targets).isEmpty();
  }

  @Test
  public void testLowercaseDefault() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(ImmutableMap.of());

    List<RouteTarget> targets = router.route(recordWithTopic("MyTopic"));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("mytopic");
  }

  @Test
  public void testLowercaseDisabled() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(
        ImmutableMap.of(TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.LOWERCASE_PROP, "false"));

    List<RouteTarget> targets = router.route(recordWithTopic("MyTopic"));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("MyTopic");
  }

  @Test
  public void testIgnoreMissingTableDefault() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(ImmutableMap.of());

    List<RouteTarget> targets = router.route(recordWithTopic("orders"));
    assertThat(targets.get(0).ignoreMissingTable()).isTrue();
  }

  @Test
  public void testIgnoreMissingTableFalse() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(
        ImmutableMap.of(
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.IGNORE_MISSING_TABLE_PROP, "false"));

    List<RouteTarget> targets = router.route(recordWithTopic("orders"));
    assertThat(targets.get(0).ignoreMissingTable()).isFalse();
  }

  @Test
  public void testRegexWithNamespace() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(
        ImmutableMap.of(
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.REGEX_PROP, ".*\\.(.+)",
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.REGEX_REPLACEMENT_PROP, "$1",
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.TABLE_NAMESPACE_PROP, "warehouse"));

    List<RouteTarget> targets = router.route(recordWithTopic("prod.events.clicks"));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("warehouse.clicks");
  }

  @Test
  public void testNullTopic() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(ImmutableMap.of());

    SinkRecord record = new SinkRecord(null, 0, null, null, null, ImmutableMap.of(), 0);
    List<RouteTarget> targets = router.route(record);
    assertThat(targets).isEmpty();
  }

  @Test
  public void testNonMatchedTopicFallsThrough() {
    TopicNameRouter router = new TopicNameRouter();
    router.configure(
        ImmutableMap.of(
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.TABLE_MAP_PREFIX + "special",
            "db.special_table",
            TopicNameRouter.CONFIG_PREFIX + TopicNameRouter.TABLE_NAMESPACE_PROP,
            "default_ns"));

    // topic not in explicit map should fall through to default behavior
    List<RouteTarget> targets = router.route(recordWithTopic("regular"));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("default_ns.regular");
  }
}
