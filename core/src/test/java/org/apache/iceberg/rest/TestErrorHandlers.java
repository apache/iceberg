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

import org.apache.iceberg.exceptions.NoSuchWarehouseException;
import org.apache.iceberg.exceptions.OAuth2BadRequestException;
import org.apache.iceberg.exceptions.OAuth2NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
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
  public void testConfigErrorHandler404ThrowsNoSuchWarehouseException() {
    ErrorResponse error =
        ErrorResponse.builder()
            .responseCode(404)
            .withType("NotFoundException")
            .withMessage("Warehouse not found")
            .build();

    assertThatThrownBy(() -> ErrorHandlers.configErrorHandler().accept(error))
        .isInstanceOf(NoSuchWarehouseException.class)
        .hasMessage("Warehouse not found");
  }

  @Test
  public void testConfigErrorHandler404ForMisconfiguredUri() {
    ErrorResponse error =
        ErrorResponse.builder().responseCode(404).withMessage("Not Found").build();

    assertThatThrownBy(() -> ErrorHandlers.configErrorHandler().accept(error))
        .isInstanceOf(RESTException.class)
        .hasMessageContaining("Not Found");
  }

  @Test
  public void testConfigErrorHandlerDelegatesToDefaultForNon404() {
    ErrorResponse error =
        ErrorResponse.builder().responseCode(500).withMessage("Internal server error").build();

    assertThatThrownBy(() -> ErrorHandlers.configErrorHandler().accept(error))
        .isInstanceOf(ServiceFailureException.class)
        .hasMessageContaining("Internal server error");
  }

  @Test
  public void testOAuthErrorHandlerInvalidGrant() {
    ErrorResponse error =
        ErrorResponse.builder()
            .responseCode(400)
            .withType(OAuth2Properties.INVALID_GRANT_ERROR)
            .withMessage("token expired")
            .build();

    assertThatThrownBy(() -> ErrorHandlers.oauthErrorHandler().accept(error))
        .isInstanceOf(OAuth2BadRequestException.class)
        .hasMessage("Malformed request: invalid_grant: token expired")
        .asInstanceOf(InstanceOfAssertFactories.type(OAuth2BadRequestException.class))
        .extracting(OAuth2BadRequestException::errorType)
        .isEqualTo(OAuth2Properties.INVALID_GRANT_ERROR);
  }

  @Test
  public void testOAuthErrorHandlerInvalidClient() {
    ErrorResponse error =
        ErrorResponse.builder()
            .responseCode(401)
            .withType(OAuth2Properties.INVALID_CLIENT_ERROR)
            .withMessage("bad credentials")
            .build();

    assertThatThrownBy(() -> ErrorHandlers.oauthErrorHandler().accept(error))
        .isInstanceOf(OAuth2NotAuthorizedException.class)
        .hasMessage("Not authorized: invalid_client: bad credentials")
        .asInstanceOf(InstanceOfAssertFactories.type(OAuth2NotAuthorizedException.class))
        .extracting(OAuth2NotAuthorizedException::errorType)
        .isEqualTo(OAuth2Properties.INVALID_CLIENT_ERROR);
  }

  @Test
  public void testOAuthErrorHandlerInvalidRequest() {
    ErrorResponse error =
        ErrorResponse.builder()
            .responseCode(400)
            .withType(OAuth2Properties.INVALID_REQUEST_ERROR)
            .withMessage("missing parameter")
            .build();

    assertThatThrownBy(() -> ErrorHandlers.oauthErrorHandler().accept(error))
        .isInstanceOf(OAuth2BadRequestException.class)
        .hasMessage("Malformed request: invalid_request: missing parameter")
        .asInstanceOf(InstanceOfAssertFactories.type(OAuth2BadRequestException.class))
        .extracting(OAuth2BadRequestException::errorType)
        .isEqualTo(OAuth2Properties.INVALID_REQUEST_ERROR);
  }

  @Test
  public void testOAuthErrorHandlerUnauthorizedClient() {
    ErrorResponse error =
        ErrorResponse.builder()
            .responseCode(400)
            .withType(OAuth2Properties.UNAUTHORIZED_CLIENT_ERROR)
            .withMessage("client cannot use this grant")
            .build();

    assertThatThrownBy(() -> ErrorHandlers.oauthErrorHandler().accept(error))
        .isInstanceOf(OAuth2BadRequestException.class)
        .hasMessage("Malformed request: unauthorized_client: client cannot use this grant")
        .asInstanceOf(InstanceOfAssertFactories.type(OAuth2BadRequestException.class))
        .extracting(OAuth2BadRequestException::errorType)
        .isEqualTo(OAuth2Properties.UNAUTHORIZED_CLIENT_ERROR);
  }

  @Test
  public void testOAuthErrorHandlerUnsupportedGrantType() {
    ErrorResponse error =
        ErrorResponse.builder()
            .responseCode(400)
            .withType(OAuth2Properties.UNSUPPORTED_GRANT_TYPE_ERROR)
            .withMessage("unsupported grant")
            .build();

    assertThatThrownBy(() -> ErrorHandlers.oauthErrorHandler().accept(error))
        .isInstanceOf(OAuth2BadRequestException.class)
        .hasMessage("Malformed request: unsupported_grant_type: unsupported grant")
        .asInstanceOf(InstanceOfAssertFactories.type(OAuth2BadRequestException.class))
        .extracting(OAuth2BadRequestException::errorType)
        .isEqualTo(OAuth2Properties.UNSUPPORTED_GRANT_TYPE_ERROR);
  }

  @Test
  public void testOAuthErrorHandlerInvalidScope() {
    ErrorResponse error =
        ErrorResponse.builder()
            .responseCode(400)
            .withType(OAuth2Properties.INVALID_SCOPE_ERROR)
            .withMessage("scope not allowed")
            .build();

    assertThatThrownBy(() -> ErrorHandlers.oauthErrorHandler().accept(error))
        .isInstanceOf(OAuth2BadRequestException.class)
        .hasMessage("Malformed request: invalid_scope: scope not allowed")
        .asInstanceOf(InstanceOfAssertFactories.type(OAuth2BadRequestException.class))
        .extracting(OAuth2BadRequestException::errorType)
        .isEqualTo(OAuth2Properties.INVALID_SCOPE_ERROR);
  }
}
