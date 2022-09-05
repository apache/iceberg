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
import org.junit.Assert;
import org.junit.Test;

public class TestOAuthErrorResponseParser {

  @Test
  public void testOAuthErrorResponseToJson() {
    String error = OAuth2Properties.INVALID_CLIENT_ERROR;
    String description = "Credentials given were invalid";
    String uri = "http://iceberg.apache.org";
    String json =
        String.format(
            "{\"error\":\"%s\",\"error_description\":\"%s\",\"error_uri\":\"%s\"}",
            error, description, uri);
    OAuthErrorResponse response =
        OAuthErrorResponse.builder()
            .withError(error)
            .withErrorDescription(description)
            .withErrorUri(uri)
            .build();
    Assert.assertEquals(
        "Should be able to serialize an error response as json",
        OAuthErrorResponseParser.toJson(response),
        json);
  }

  @Test
  public void testOAuthErrorResponseToJsonWithNulls() {
    String error = OAuth2Properties.INVALID_CLIENT_ERROR;
    String expected =
        String.format("{\"error\":\"%s\",\"error_description\":null,\"error_uri\":null}", error);
    OAuthErrorResponse response = OAuthErrorResponse.builder().withError(error).build();
    Assert.assertEquals(
        "Should be able to serialize an error response as json",
        OAuthErrorResponseParser.toJson(response),
        expected);
  }

  @Test
  public void testOAuthErrorResponseBuilderMIssingError() {
    Assert.assertThrows(
        "Missing error should throw exception",
        IllegalArgumentException.class,
        () -> OAuthErrorResponse.builder().build());
  }

  @Test
  public void testOAuthErrorResponseFromJson() {
    String error = OAuth2Properties.INVALID_CLIENT_ERROR;
    String description = "Credentials given were invalid";
    String uri = "http://iceberg.apache.org";
    String json =
        String.format(
            "{\"error\":\"%s\",\"error_description\":\"%s\",\"error_uri\":\"%s\"}",
            error, description, uri);
    OAuthErrorResponse expected =
        OAuthErrorResponse.builder()
            .withError(error)
            .withErrorDescription(description)
            .withErrorUri(uri)
            .build();
    assertEquals(expected, OAuthErrorResponseParser.fromJson(json));
  }

  @Test
  public void testOAuthErrorResponseFromJsonWithNulls() {
    String error = OAuth2Properties.INVALID_CLIENT_ERROR;
    String json = String.format("{\"error\":\"%s\"}", error);
    OAuthErrorResponse expected = OAuthErrorResponse.builder().withError(error).build();
    assertEquals(expected, OAuthErrorResponseParser.fromJson(json));

    // test with explicitly set nulls
    json = String.format("{\"error\":\"%s\",\"error_description\":null,\"error_uri\":null}", error);
    assertEquals(expected, OAuthErrorResponseParser.fromJson(json));
  }

  @Test
  public void testOAuthErrorResponseFromJsonMissingError() {
    String description = "Credentials given were invalid";
    String uri = "http://iceberg.apache.org";
    String json =
        String.format("{\"error_description\":\"%s\",\"error_uri\":\"%s\"}", description, uri);
    Assert.assertThrows(
        "Missing error should throw exception",
        IllegalArgumentException.class,
        () -> OAuthErrorResponseParser.fromJson(json));
  }

  public void assertEquals(OAuthErrorResponse expected, OAuthErrorResponse actual) {
    Assert.assertEquals("Error should be equal", expected.error(), actual.error());
    Assert.assertEquals(
        "Description should be equal", expected.errorDescription(), actual.errorDescription());
    Assert.assertEquals("URI should be equal", expected.errorUri(), actual.errorUri());
  }
}
