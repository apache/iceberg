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

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A scope for authentication. A scope is a set of properties that define how an {@link AuthSession}
 * should be created, what credentials should be used, and how the session should be cached.
 */
public interface AuthScope {

  /** Properties that define the scope. */
  Map<String, String> properties();

  /**
   * The parent {@link AuthSession} for this scope, if any. Parent sessions are generally used to
   * provide default values for properties when the current scope has no value defined for them.
   */
  @Nullable
  AuthSession parent();

  /**
   * Whether the {@link AuthSession} created for this scope should be cached.
   *
   * <p>If this method returns true, the {@link AuthManager} is responsible for the session's
   * lifecycle and should close the session when it is no longer needed. If this method returns
   * false, the session should be closed by the caller when it is no longer needed.
   */
  boolean cacheable();

  /** Returns a new scope identical to this one, but with the given parent session. */
  AuthScope withParent(AuthSession parent);
}
