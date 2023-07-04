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
package org.apache.iceberg.rest.responses;

import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOAuthErrorResponseParser {

  @Test
  public void testOAuthErrorResponseFromJson() {
    String error = OAuth2Properties.INVALID_CLIENT_ERROR;
    String description = "Credentials given were invalid";
    String uri = "http://iceberg.apache.org";
    String json =
        String.format(
            "{\"error\":\"%s\",\"error_description\":\"%s\",\"error_uri\":\"%s\"}",
            error, description, uri);
    ErrorResponse expected =
        ErrorResponse.builder().responseCode(400).withType(error).withMessage(description).build();
    assertEquals(expected, OAuthErrorResponseParser.fromJson(400, json));
  }

  @Test
  public void testOAuthErrorResponseFromJsonWithNulls() {
    String error = OAuth2Properties.INVALID_CLIENT_ERROR;
    String json = String.format("{\"error\":\"%s\"}", error);
    ErrorResponse expected = ErrorResponse.builder().responseCode(400).withType(error).build();
    assertEquals(expected, OAuthErrorResponseParser.fromJson(400, json));

    // test with explicitly set nulls
    json = String.format("{\"error\":\"%s\",\"error_description\":null,\"error_uri\":null}", error);
    assertEquals(expected, OAuthErrorResponseParser.fromJson(400, json));
  }

  @Test
  public void testOAuthErrorResponseFromJsonMissingError() {
    String description = "Credentials given were invalid";
    String uri = "http://iceberg.apache.org";
    String json =
        String.format("{\"error_description\":\"%s\",\"error_uri\":\"%s\"}", description, uri);
    Assertions.assertThatThrownBy(() -> OAuthErrorResponseParser.fromJson(400, json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: error");
  }

  public void assertEquals(ErrorResponse expected, ErrorResponse actual) {
    Assertions.assertThat(actual.code()).isEqualTo(expected.code());
    Assertions.assertThat(actual.type()).isEqualTo(expected.type());
    Assertions.assertThat(actual.message()).isEqualTo(expected.message());
  }
}
