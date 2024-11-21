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

import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthManagers {

  private static final Logger LOG = LoggerFactory.getLogger(AuthManagers.class);

  private AuthManagers() {}

  public static AuthManager loadAuthManager(String name, Map<String, String> properties) {

    String authType =
        properties.getOrDefault(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_NONE);

    String impl;
    switch (authType.toLowerCase(Locale.ROOT)) {
      case AuthProperties.AUTH_TYPE_NONE:
        impl = AuthProperties.AUTH_MANAGER_IMPL_NONE;
        break;
      case AuthProperties.AUTH_TYPE_BASIC:
        impl = AuthProperties.AUTH_MANAGER_IMPL_BASIC;
        break;
      default:
        impl = authType;
    }

    LOG.info("Loading AuthManager implementation: {}", impl);
    DynConstructors.Ctor<AuthManager> ctor;
    try {
      ctor =
          DynConstructors.builder(AuthManager.class)
              .loader(AuthManagers.class.getClassLoader())
              .impl(impl, String.class) // with name
              .impl(impl) // without name
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize AuthManager implementation %s: %s", impl, e.getMessage()),
          e);
    }

    AuthManager authManager;
    try {
      authManager = ctor.newInstance(name);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize AuthManager, %s does not implement AuthManager", impl),
          e);
    }

    return authManager;
  }
}
