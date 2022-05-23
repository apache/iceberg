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

package org.apache.iceberg.rest.auth;

public class OAuth2Properties {
  private OAuth2Properties() {
  }

  /**
   * A Bearer token which will be used for interaction with the server.
   */
  public static final String TOKEN = "token";

  /**
   * A credential to exchange for a token in the OAuth2 client credentials flow.
   */
  public static final String CREDENTIAL = "credential";

  /**
   * Scope for OAuth2 flows.
   */
  public static final String CATALOG_SCOPE = "catalog";
}
