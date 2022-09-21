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

import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestCatalogErrorResponseParser {

  @Test
  public void testErrorResponseToJson() {
    String message = "The given namespace does not exist";
    String type = "NoSuchNamespaceException";
    Integer code = 404;
    String errorModelJson =
        String.format("{\"message\":\"%s\",\"type\":\"%s\",\"code\":%d}", message, type, code);
    String json = "{\"error\":" + errorModelJson + "}";
    CatalogErrorResponse response =
        CatalogErrorResponse.builder()
            .withMessage(message)
            .withType(type)
            .responseCode(code)
            .build();
    Assert.assertEquals(
        "Should be able to serialize an error response as json",
        CatalogErrorResponseParser.toJson(response),
        json);
  }

  @Test
  public void testErrorResponseToJsonWithStack() {
    String message = "The given namespace does not exist";
    String type = "NoSuchNamespaceException";
    Integer code = 404;
    List<String> stack = Arrays.asList("a", "b");
    String errorModelJson =
        String.format(
            "{\"message\":\"%s\",\"type\":\"%s\",\"code\":%d,\"stack\":[\"a\",\"b\"]}",
            message, type, code);
    String json = "{\"error\":" + errorModelJson + "}";
    CatalogErrorResponse response =
        CatalogErrorResponse.builder()
            .withMessage(message)
            .withType(type)
            .responseCode(code)
            .withStackTrace(stack)
            .build();
    Assert.assertEquals(
        "Should be able to serialize an error response as json",
        json,
        CatalogErrorResponseParser.toJson(response));
  }

  @Test
  public void testErrorResponseFromJson() {
    String message = "The given namespace does not exist";
    String type = "NoSuchNamespaceException";
    Integer code = 404;
    String errorModelJson =
        String.format("{\"message\":\"%s\",\"type\":\"%s\",\"code\":%d}", message, type, code);
    String json = "{\"error\":" + errorModelJson + "}";

    CatalogErrorResponse expected =
        CatalogErrorResponse.builder()
            .withMessage(message)
            .withType(type)
            .responseCode(code)
            .build();
    assertEquals(expected, CatalogErrorResponseParser.fromJson(json));
  }

  @Test
  public void testErrorResponseFromJsonWithStack() {
    String message = "The given namespace does not exist";
    String type = "NoSuchNamespaceException";
    Integer code = 404;
    List<String> stack = Arrays.asList("a", "b");
    String errorModelJson =
        String.format(
            "{\"message\":\"%s\",\"type\":\"%s\",\"code\":%d,\"stack\":[\"a\",\"b\"]}",
            message, type, code);
    String json = "{\"error\":" + errorModelJson + "}";

    CatalogErrorResponse expected =
        CatalogErrorResponse.builder()
            .withMessage(message)
            .withType(type)
            .responseCode(code)
            .withStackTrace(stack)
            .build();
    assertEquals(expected, CatalogErrorResponseParser.fromJson(json));
  }

  @Test
  public void testErrorResponseFromJsonWithExplicitNullStack() {
    String message = "The given namespace does not exist";
    String type = "NoSuchNamespaceException";
    Integer code = 404;
    List<String> stack = null;
    String errorModelJson =
        String.format(
            "{\"message\":\"%s\",\"type\":\"%s\",\"code\":%d,\"stack\":null}", message, type, code);
    String json = "{\"error\":" + errorModelJson + "}";

    CatalogErrorResponse expected =
        CatalogErrorResponse.builder()
            .withMessage(message)
            .withType(type)
            .responseCode(code)
            .withStackTrace(stack)
            .build();
    assertEquals(expected, CatalogErrorResponseParser.fromJson(json));
  }

  public void assertEquals(CatalogErrorResponse expected, CatalogErrorResponse actual) {
    Assertions.assertThat(actual.message()).isEqualTo(expected.message());
    Assertions.assertThat(actual.type()).isEqualTo(expected.type());
    Assertions.assertThat(actual.code()).isEqualTo(expected.code());
    Assertions.assertThat(actual.stack()).isEqualTo(expected.stack());
  }
}
