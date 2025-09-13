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
package org.apache.iceberg.events.model;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class QueryEventsRequestTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void testSerialization() throws Exception {
    QueryEventsRequest request = new QueryEventsRequest();
    request.setPageSize(50);
    request.setAfterTimestampMs(1690000000000L);
    request.setOperationTypes(Arrays.asList(OperationType.APPEND, OperationType.DELETE));

    String json = mapper.writeValueAsString(request);
    assertNotNull(json);
    assertTrue(json.contains("\"page-size\":50"));
    assertTrue(json.contains("\"after-timestamp-ms\":1690000000000"));
    assertTrue(json.contains("\"operation-types\":[\"APPEND\",\"DELETE\"]"));
  }

  @Test
  void testDeserialization() throws Exception {
    String json =
        "{\n"
            + "  \"page-size\": 100,\n"
            + "  \"after-timestamp-ms\": 1690000001000,\n"
            + "  \"operation-types\": [\"CREATE_TABLE\", \"DROP_TABLE\"]\n"
            + "}";

    QueryEventsRequest request = mapper.readValue(json, QueryEventsRequest.class);
    assertNotNull(request);
    assertEquals(100, request.getPageSize());
    assertEquals(1690000001000L, request.getAfterTimestampMs());
    assertEquals(2, request.getOperationTypes().size());
    assertEquals(OperationType.CREATE_TABLE, request.getOperationTypes().get(0));
    assertEquals(OperationType.DROP_TABLE, request.getOperationTypes().get(1));
  }
}
