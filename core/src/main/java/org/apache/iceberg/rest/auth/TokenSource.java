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

import org.apache.iceberg.exceptions.NotAuthorizedException;

/** A source of the bearer token that an {@link AuthSession} authenticates requests with. */
@FunctionalInterface
public interface TokenSource {

  /**
   * Returns the current token; tokens returned by later calls are issued for the same principal.
   *
   * @throws NotAuthorizedException if there is no token or the token has expired
   */
  IssuedToken currentToken();
}
