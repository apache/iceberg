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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class TestCompositeFieldRouter {

  private static SinkRecord recordWithValue(Object value) {
    return new SinkRecord("topic", 0, null, null, null, value, 0);
  }

  @Test
  public void testTwoFieldTemplate() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    router.configure(
        ImmutableMap.of(
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP,
            "region,event_type",
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.TABLE_TEMPLATE_PROP,
            "${0}_${1}"));

    List<RouteTarget> targets =
        router.route(recordWithValue(ImmutableMap.of("region", "us_east", "event_type", "clicks")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("us_east_clicks");
  }

  @Test
  public void testThreeFieldTemplate() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    router.configure(
        ImmutableMap.of(
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP,
            "env,service,version",
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.TABLE_TEMPLATE_PROP,
            "${0}.${1}_v${2}"));

    List<RouteTarget> targets =
        router.route(
            recordWithValue(ImmutableMap.of("env", "prod", "service", "payments", "version", "2")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("prod.payments_v2");
  }

  @Test
  public void testDefaultTemplate() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    router.configure(
        ImmutableMap.of(
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP, "a,b"));

    List<RouteTarget> targets =
        router.route(recordWithValue(ImmutableMap.of("a", "foo", "b", "bar")));
    assertThat(targets).hasSize(1);
    // default template is ${0}.${1}
    assertThat(targets.get(0).tableName()).isEqualTo("foo.bar");
  }

  @Test
  public void testNestedFieldExtraction() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    router.configure(
        ImmutableMap.of(
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP,
            "metadata.env,name",
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.TABLE_TEMPLATE_PROP,
            "${0}_${1}"));

    List<RouteTarget> targets =
        router.route(
            recordWithValue(
                ImmutableMap.of("metadata", ImmutableMap.of("env", "staging"), "name", "orders")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("staging_orders");
  }

  @Test
  public void testNamespacePrepended() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    router.configure(
        ImmutableMap.of(
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP, "table",
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.TABLE_NAMESPACE_PROP,
                "warehouse"));

    List<RouteTarget> targets = router.route(recordWithValue(ImmutableMap.of("table", "orders")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("warehouse.orders");
  }

  @Test
  public void testLowercaseDefault() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    router.configure(
        ImmutableMap.of(
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP, "name"));

    List<RouteTarget> targets = router.route(recordWithValue(ImmutableMap.of("name", "MyTable")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("mytable");
  }

  @Test
  public void testLowercaseDisabled() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    router.configure(
        ImmutableMap.of(
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP, "name",
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.LOWERCASE_PROP, "false"));

    List<RouteTarget> targets = router.route(recordWithValue(ImmutableMap.of("name", "MyTable")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("MyTable");
  }

  @Test
  public void testNullFieldDrop() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    router.configure(
        ImmutableMap.of(
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP, "a,b",
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.NULL_HANDLING_PROP, "drop"));

    // field "b" is missing → null → drop
    List<RouteTarget> targets = router.route(recordWithValue(ImmutableMap.of("a", "foo")));
    assertThat(targets).isEmpty();
  }

  @Test
  public void testNullFieldLiteral() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    router.configure(
        ImmutableMap.of(
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP, "a,b",
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.TABLE_TEMPLATE_PROP,
                "${0}_${1}",
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.NULL_HANDLING_PROP,
                "literal"));

    List<RouteTarget> targets = router.route(recordWithValue(ImmutableMap.of("a", "foo")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("foo_null");
  }

  @Test
  public void testNullFieldDefault() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    router.configure(
        ImmutableMap.of(
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP, "a,b",
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.TABLE_TEMPLATE_PROP,
                "${0}_${1}",
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.NULL_HANDLING_PROP,
                "default:unknown"));

    List<RouteTarget> targets = router.route(recordWithValue(ImmutableMap.of("a", "foo")));
    assertThat(targets).hasSize(1);
    assertThat(targets.get(0).tableName()).isEqualTo("foo_unknown");
  }

  @Test
  public void testNullRecordValue() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    router.configure(
        ImmutableMap.of(
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP, "a"));

    List<RouteTarget> targets = router.route(recordWithValue(null));
    assertThat(targets).isEmpty();
  }

  @Test
  public void testTemplateValidationInvalidIndex() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    assertThatThrownBy(
            () ->
                router.configure(
                    ImmutableMap.of(
                        CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP, "a",
                        CompositeFieldRouter.CONFIG_PREFIX
                                + CompositeFieldRouter.TABLE_TEMPLATE_PROP,
                            "${0}_${5}")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("${5}");
  }

  @Test
  public void testEmptyFieldsConfig() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    assertThatThrownBy(() -> router.configure(ImmutableMap.of()))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("fields");
  }

  @Test
  public void testIgnoreMissingTableDefault() {
    CompositeFieldRouter router = new CompositeFieldRouter();
    router.configure(
        ImmutableMap.of(
            CompositeFieldRouter.CONFIG_PREFIX + CompositeFieldRouter.FIELDS_PROP, "name"));

    List<RouteTarget> targets = router.route(recordWithValue(ImmutableMap.of("name", "orders")));
    assertThat(targets.get(0).ignoreMissingTable()).isTrue();
  }
}
