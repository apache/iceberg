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

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.NullOrder.NULLS_LAST;
import static org.apache.iceberg.SortDirection.ASC;
import static org.apache.iceberg.SortDirection.DESC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.transforms.UnknownTransform;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSortOrderParser extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1);
  }

  @TestTemplate
  public void testUnknownTransforms() {
    String jsonString =
        "{\n"
            + "  \"order-id\" : 10,\n"
            + "  \"fields\" : [ {\n"
            + "    \"transform\" : \"custom_transform\",\n"
            + "    \"source-id\" : 2,\n"
            + "    \"direction\" : \"desc\",\n"
            + "    \"null-order\" : \"nulls-first\"\n"
            + "  } ]\n"
            + "}";

    SortOrder order = SortOrderParser.fromJson(table.schema(), jsonString);

    assertThat(order.orderId()).isEqualTo(10);
    assertThat(order.fields()).hasSize(1);
    assertThat(order.fields().get(0).transform())
        .isInstanceOf(UnknownTransform.class)
        .asString()
        .isEqualTo("custom_transform");
    assertThat(order.fields().get(0).sourceId()).isEqualTo(2);
    assertThat(order.fields().get(0).direction()).isEqualTo(DESC);
    assertThat(order.fields().get(0).nullOrder()).isEqualTo(NULLS_FIRST);
  }

  @TestTemplate
  public void testSourceIdsDeserialization() {
    String jsonString =
        "{\n"
            + "  \"order-id\" : 10,\n"
            + "  \"fields\" : [ {\n"
            + "    \"transform\" : \"bucket[16]\",\n"
            + "    \"source-ids\" : [ 1, 2 ],\n"
            + "    \"direction\" : \"asc\",\n"
            + "    \"null-order\" : \"nulls-last\"\n"
            + "  } ]\n"
            + "}";

    UnboundSortOrder order = SortOrderParser.fromJson(jsonString);

    assertThat(order.orderId()).isEqualTo(10);
    assertThat(order.fields()).hasSize(1);
    assertThat(order.fields().get(0).sourceIds()).containsExactly(1, 2);
    assertThat(order.fields().get(0).sourceId()).isEqualTo(1);
    assertThat(order.fields().get(0).direction()).isEqualTo(ASC);
    assertThat(order.fields().get(0).nullOrder()).isEqualTo(NULLS_LAST);
  }

  @TestTemplate
  public void testSourceIdsSerialization() {
    UnboundSortOrder order =
        UnboundSortOrder.builder()
            .withOrderId(10)
            .addSortField("bucket[16]", Arrays.asList(1, 2), ASC, NULLS_LAST)
            .build();

    String json = SortOrderParser.toJson(order, true);
    assertThat(json).contains("\"source-ids\" : [ 1, 2 ]");
    assertThat(json).doesNotContain("\"source-id\"");
  }

  @TestTemplate
  public void testSingleSourceIdSerialization() {
    UnboundSortOrder order =
        UnboundSortOrder.builder()
            .withOrderId(10)
            .addSortField("bucket[16]", 1, ASC, NULLS_LAST)
            .build();

    String json = SortOrderParser.toJson(order, true);
    assertThat(json).contains("\"source-id\" : 1");
    assertThat(json).doesNotContain("\"source-ids\"");
  }

  @TestTemplate
  public void testSourceIdsRoundTrip() {
    UnboundSortOrder order =
        UnboundSortOrder.builder()
            .withOrderId(10)
            .addSortField("bucket[16]", Arrays.asList(1, 2), ASC, NULLS_LAST)
            .addSortField("identity", 3, DESC, NULLS_FIRST)
            .build();

    String json = SortOrderParser.toJson(order, true);
    UnboundSortOrder parsed = SortOrderParser.fromJson(json);

    assertThat(parsed.orderId()).isEqualTo(10);
    assertThat(parsed.fields()).hasSize(2);
    assertThat(parsed.fields().get(0).sourceIds()).containsExactly(1, 2);
    assertThat(parsed.fields().get(0).direction()).isEqualTo(ASC);
    assertThat(parsed.fields().get(1).sourceIds()).containsExactly(3);
    assertThat(parsed.fields().get(1).direction()).isEqualTo(DESC);
  }

  @TestTemplate
  public void testSourceIdsPrecedenceOverSourceId() {
    String jsonString =
        "{\n"
            + "  \"order-id\" : 10,\n"
            + "  \"fields\" : [ {\n"
            + "    \"transform\" : \"bucket[16]\",\n"
            + "    \"source-id\" : 99,\n"
            + "    \"source-ids\" : [ 1, 2 ],\n"
            + "    \"direction\" : \"asc\",\n"
            + "    \"null-order\" : \"nulls-last\"\n"
            + "  } ]\n"
            + "}";

    UnboundSortOrder order = SortOrderParser.fromJson(jsonString);

    assertThat(order.fields()).hasSize(1);
    assertThat(order.fields().get(0).sourceIds()).containsExactly(1, 2);
  }

  @TestTemplate
  public void testEmptySourceIdsArray() {
    String jsonString =
        "{\n"
            + "  \"order-id\" : 10,\n"
            + "  \"fields\" : [ {\n"
            + "    \"transform\" : \"bucket[16]\",\n"
            + "    \"source-ids\" : [ ],\n"
            + "    \"direction\" : \"asc\",\n"
            + "    \"null-order\" : \"nulls-last\"\n"
            + "  } ]\n"
            + "}";

    assertThatThrownBy(() -> SortOrderParser.fromJson(jsonString))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("source-ids must be a non-empty array");
  }

  @TestTemplate
  public void invalidSortDirection() {
    String jsonString =
        "{\n"
            + "  \"order-id\" : 10,\n"
            + "  \"fields\" : [ {\n"
            + "    \"transform\" : \"custom_transform\",\n"
            + "    \"source-id\" : 2,\n"
            + "    \"direction\" : \"invalid\",\n"
            + "    \"null-order\" : \"nulls-first\"\n"
            + "  } ]\n"
            + "}";

    assertThatThrownBy(() -> SortOrderParser.fromJson(table.schema(), jsonString))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid sort direction: invalid");
  }
}
