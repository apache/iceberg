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
import org.apache.iceberg.common.DynConstructors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthManagers {

  private static final Logger LOG = LoggerFactory.getLogger(AuthManagers.class);

  public static final String AUTH_MANAGER_IMPL = "auth-manager-impl";

  private AuthManagers() {}

  public static AuthManager loadAuthManager(String name, Map<String, String> properties) {
    String impl = properties.get(AUTH_MANAGER_IMPL);
    if (impl == null) {
      return new OAuth2Manager(name, properties);
    }

    LOG.info("Loading custom AuthManager implementation: {}", impl);
    DynConstructors.Ctor<AuthManager> ctor;
    try {
      ctor =
          DynConstructors.builder(AuthManager.class)
              .loader(AuthManagers.class.getClassLoader())
              .impl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize AuthManager implementation %s: %s", impl, e.getMessage()),
          e);
    }

    AuthManager authManager;
    try {
      authManager = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize AuthManager, %s does not implement AuthManager.", impl),
          e);
    }

    authManager.initialize(name, properties);
    return authManager;
  }
}
