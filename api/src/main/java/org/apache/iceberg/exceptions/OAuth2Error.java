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
package org.apache.iceberg.exceptions;

/**
 * Marker interface for exceptions arising from an OAuth2 token-endpoint error response (RFC 6749
 * §5.2). The {@link #errorType()} value is one of the RFC 6749 §5.2 error codes (e.g. {@code
 * "invalid_grant"}, {@code "invalid_client"}).
 */
public interface OAuth2Error {
  /** The OAuth2 error code from RFC 6749 §5.2 (e.g. {@code "invalid_grant"}). */
  String errorType();
}
