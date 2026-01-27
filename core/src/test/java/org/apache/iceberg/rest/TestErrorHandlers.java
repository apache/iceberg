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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.Test;

public class TestErrorHandlers {

  @Test
  public void errorHandlerIncludesCodeAndType() {
    ErrorResponse error =
        ErrorResponse.builder()
            .responseCode(422)
            .withType("ValidationException")
            .withMessage("Invalid input")
            .build();

    assertThatThrownBy(() -> ErrorHandlers.defaultErrorHandler().accept(error))
        .isInstanceOf(RESTException.class)
        .hasMessage("Unable to process (code: 422, type: ValidationException): Invalid input");
  }

  @Test
  public void errorHandlerWithCodeOnly() {
    ErrorResponse error = ErrorResponse.builder().responseCode(422).build();

    assertThatThrownBy(() -> ErrorHandlers.defaultErrorHandler().accept(error))
        .isInstanceOf(RESTException.class)
        .hasMessage("Unable to process (code: 422, type: null): null");
  }

  @Test
  public void errorHandlerWithCodeAndMessageOnly() {
    ErrorResponse error =
        ErrorResponse.builder().responseCode(422).withMessage("Invalid input").build();

    assertThatThrownBy(() -> ErrorHandlers.defaultErrorHandler().accept(error))
        .isInstanceOf(RESTException.class)
        .hasMessage("Unable to process (code: 422, type: null): Invalid input");
  }

  @Test
  public void errorHandlerWithCodeAndTypeOnly() {
    ErrorResponse error =
        ErrorResponse.builder().responseCode(422).withType("ValidationException").build();

    assertThatThrownBy(() -> ErrorHandlers.defaultErrorHandler().accept(error))
        .isInstanceOf(RESTException.class)
        .hasMessage("Unable to process (code: 422, type: ValidationException): null");
  }

  @Test
  public void errorHandlerFor405() {
    ErrorResponse error =
        ErrorResponse.builder()
            .responseCode(405)
            .withType("MethodNotAllowedException")
            .withMessage("Method not allowed")
            .build();

    // 405 (Method Not Allowed) should fall through to default RESTException
    assertThatThrownBy(() -> ErrorHandlers.defaultErrorHandler().accept(error))
        .isInstanceOf(RESTException.class)
        .hasMessage(
            "Unable to process (code: 405, type: MethodNotAllowedException): Method not allowed");
  }
}
