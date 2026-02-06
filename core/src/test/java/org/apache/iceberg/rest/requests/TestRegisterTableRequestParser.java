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

public class TestRegisterTableRequestParser {

  @Test
  public void nullCheck() {
    assertThatThrownBy(() -> RegisterTableRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid register table request: null");

    assertThatThrownBy(() -> RegisterTableRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse register table request from null object");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> RegisterTableRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");

    assertThatThrownBy(() -> RegisterTableRequestParser.fromJson("{\"name\" : \"test_tbl\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: metadata-location");

    assertThatThrownBy(
            () ->
                RegisterTableRequestParser.fromJson(
                    "{\"metadata-location\" : \"file://tmp/NS/test_tbl/metadata/00000-d4f60d2f-2ad2-408b-8832-0ed7fbd851ee.metadata.json\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");
  }

  @Test
  public void roundTripSerde() {
    RegisterTableRequest request =
        ImmutableRegisterTableRequest.builder()
            .name("table_1")
            .metadataLocation(
                "file://tmp/NS/test_tbl/metadata/00000-d4f60d2f-2ad2-408b-8832-0ed7fbd851ee.metadata.json")
            .build();

    String expectedJson =
        "{\n"
            + "  \"name\" : \"table_1\",\n"
            + "  \"metadata-location\" : \"file://tmp/NS/test_tbl/metadata/00000-d4f60d2f-2ad2-408b-8832-0ed7fbd851ee.metadata.json\"\n"
            + "}";

    String json = RegisterTableRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);

    assertThat(RegisterTableRequestParser.toJson(RegisterTableRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }
}
