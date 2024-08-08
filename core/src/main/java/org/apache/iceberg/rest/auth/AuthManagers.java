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
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthManagers {

  private static final Logger LOG = LoggerFactory.getLogger(AuthManagers.class);

  /** Old property name for enabling SigV4 authentication. */
  private static final String SIGV4_ENABLED = "rest.sigv4-enabled";

  private AuthManagers() {}

  public static AuthManager loadAuthManager(Map<String, String> properties) {

    String authType;
    if (properties.containsKey(SIGV4_ENABLED)) {
      LOG.warn(
          "The property {} is deprecated and will be removed in a future release. "
              + "Please use the property {}={} instead.",
          SIGV4_ENABLED,
          AuthProperties.AUTH_TYPE,
          AuthProperties.AUTH_TYPE_SIGV4);
    }

    if (PropertyUtil.propertyAsBoolean(properties, SIGV4_ENABLED, false)) {
      authType = AuthProperties.AUTH_TYPE_SIGV4;
    } else {
      boolean hasOAuth2 =
          properties.containsKey(OAuth2Properties.CREDENTIAL)
              || properties.containsKey(OAuth2Properties.TOKEN);
      String defaultAuthType =
          hasOAuth2 ? AuthProperties.AUTH_TYPE_OAUTH2 : AuthProperties.AUTH_TYPE_NONE;
      authType =
          PropertyUtil.propertyAsString(properties, AuthProperties.AUTH_TYPE, defaultAuthType);
    }

    String impl;
    switch (authType.toLowerCase(Locale.ROOT)) {
      case AuthProperties.AUTH_TYPE_NONE:
        impl = AuthProperties.AUTHENTICATION_IMPL_NONE;
        break;
      case AuthProperties.AUTH_TYPE_BASIC:
        impl = AuthProperties.AUTHENTICATION_IMPL_BASIC;
        break;
      case AuthProperties.AUTH_TYPE_SIGV4:
        impl = AuthProperties.AUTHENTICATION_IMPL_SIGV4;
        break;
      case AuthProperties.AUTH_TYPE_OAUTH2:
        impl = AuthProperties.AUTHENTICATION_IMPL_OAUTH2;
        break;
      default:
        impl = authType;
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

    return authManager;
  }
}
