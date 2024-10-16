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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

public class TestFetchScanTasksRequest {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> FetchScanTasksRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid request: fetchScanTasks request null");

    assertThatThrownBy(() -> FetchScanTasksRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid request: fetchScanTasks null");
  }

  @Test
  public void roundTripSerdeWithPlanTask() {
    FetchScanTasksRequest request = new FetchScanTasksRequest("somePlanTask");
    String expectedJson = "{\"plan-task\":\"somePlanTask\"}";
    String json = FetchScanTasksRequestParser.toJson(request, false);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison on FetchScanTasksRequest because we don't implement
    // equals/hashcode
    assertThat(
            FetchScanTasksRequestParser.toJson(FetchScanTasksRequestParser.fromJson(json), false))
        .isEqualTo(expectedJson);
  }
}
