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
package org.apache.iceberg.rest.requests;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestQueryEventsRequestParser {

  @Test
  public void testRoundTripEmptyRequest() {
    QueryEventsRequest request = ImmutableQueryEventsRequest.builder().build();

    String json = QueryEventsRequestParser.toJson(request);
    QueryEventsRequest parsed = QueryEventsRequestParser.fromJson(json);

    assertThat(parsed.continuationToken()).isNull();
    assertThat(parsed.pageSize()).isNull();
    assertThat(parsed.afterTimestampMs()).isNull();
    assertThat(parsed.operationTypes()).isNull();
    assertThat(parsed.catalogObjectsByName()).isNull();
    assertThat(parsed.catalogObjectsById()).isNull();
    assertThat(parsed.objectTypes()).isNull();
    assertThat(parsed.customFilters()).isNull();
  }

  @Test
  public void testRoundTripWithContinuationToken() {
    QueryEventsRequest request =
        ImmutableQueryEventsRequest.builder()
            .continuationToken("token-abc-123")
            .pageSize(100)
            .build();

    String json = QueryEventsRequestParser.toJson(request);
    QueryEventsRequest parsed = QueryEventsRequestParser.fromJson(json);

    assertThat(parsed.continuationToken()).isEqualTo("token-abc-123");
    assertThat(parsed.pageSize()).isEqualTo(100);
  }

  @Test
  public void testRoundTripWithAllFields() {
    QueryEventsRequest request =
        ImmutableQueryEventsRequest.builder()
            .continuationToken("token-xyz")
            .pageSize(50)
            .afterTimestampMs(1714000000000L)
            .operationTypes(ImmutableList.of("create-table", "drop-table"))
            .catalogObjectsByName(
                ImmutableList.of(
                    ImmutableList.of("accounting", "tax"),
                    ImmutableList.of("marketing")))
            .catalogObjectsById(
                ImmutableList.of(
                    ImmutableCatalogObjectUuid.builder()
                        .uuid("123e4567-e89b-12d3-a456-426614174000")
                        .type("table")
                        .build()))
            .objectTypes(ImmutableList.of("table", "view"))
            .customFilters(ImmutableMap.of("region", "us-east-1"))
            .build();

    String json = QueryEventsRequestParser.toJson(request);
    QueryEventsRequest parsed = QueryEventsRequestParser.fromJson(json);

    assertThat(parsed.continuationToken()).isEqualTo("token-xyz");
    assertThat(parsed.pageSize()).isEqualTo(50);
    assertThat(parsed.afterTimestampMs()).isEqualTo(1714000000000L);
    assertThat(parsed.operationTypes()).containsExactly("create-table", "drop-table");
    assertThat(parsed.catalogObjectsByName()).hasSize(2);
    assertThat(parsed.catalogObjectsByName().get(0)).containsExactly("accounting", "tax");
    assertThat(parsed.catalogObjectsByName().get(1)).containsExactly("marketing");
    assertThat(parsed.catalogObjectsById()).hasSize(1);
    assertThat(parsed.catalogObjectsById().get(0).uuid())
        .isEqualTo("123e4567-e89b-12d3-a456-426614174000");
    assertThat(parsed.catalogObjectsById().get(0).type()).isEqualTo("table");
    assertThat(parsed.objectTypes()).containsExactly("table", "view");
    assertThat(parsed.customFilters()).containsEntry("region", "us-east-1");
  }

  @Test
  public void testRoundTripWithOperationTypesOnly() {
    QueryEventsRequest request =
        ImmutableQueryEventsRequest.builder()
            .operationTypes(ImmutableList.of("create-namespace", "drop-namespace"))
            .build();

    String json = QueryEventsRequestParser.toJson(request);
    QueryEventsRequest parsed = QueryEventsRequestParser.fromJson(json);

    assertThat(parsed.operationTypes()).containsExactly("create-namespace", "drop-namespace");
    assertThat(parsed.continuationToken()).isNull();
  }
}
