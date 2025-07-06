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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthManagers {

  private static final Logger LOG = LoggerFactory.getLogger(AuthManagers.class);

  /** Old property name for enabling SigV4 authentication. */
  private static final String SIGV4_ENABLED_LEGACY = "rest.sigv4-enabled";

  private AuthManagers() {}

  public static AuthManager loadAuthManager(String name, Map<String, String> properties) {
    if (properties.containsKey(SIGV4_ENABLED_LEGACY)) {
      LOG.warn(
          "The property {} is deprecated and will be removed in a future release. "
              + "Please use the property {}={} instead.",
          SIGV4_ENABLED_LEGACY,
          AuthProperties.AUTH_TYPE,
          AuthProperties.AUTH_TYPE_SIGV4);
    }

    String authType;
    if (PropertyUtil.propertyAsBoolean(properties, SIGV4_ENABLED_LEGACY, false)) {
      authType = AuthProperties.AUTH_TYPE_SIGV4;
    } else {
      authType = properties.get(AuthProperties.AUTH_TYPE);
      if (authType == null) {
        boolean hasCredential = properties.containsKey(OAuth2Properties.CREDENTIAL);
        boolean hasToken = properties.containsKey(OAuth2Properties.TOKEN);
        if (hasCredential || hasToken) {
          LOG.warn(
              "Inferring {}={} since property {} was provided. "
                  + "Please explicitly set {} to avoid this warning.",
              AuthProperties.AUTH_TYPE,
              AuthProperties.AUTH_TYPE_OAUTH2,
              hasCredential ? OAuth2Properties.CREDENTIAL : OAuth2Properties.TOKEN,
              AuthProperties.AUTH_TYPE);
          authType = AuthProperties.AUTH_TYPE_OAUTH2;
        } else {
          authType = AuthProperties.AUTH_TYPE_NONE;
        }
      }
    }

    AuthManager delegate = null;
    if (authType.equals(AuthProperties.AUTH_TYPE_SIGV4)) {
      String delegateAuthType =
          properties.getOrDefault(
              AuthProperties.SIGV4_DELEGATE_AUTH_TYPE,
              AuthProperties.SIGV4_DELEGATE_AUTH_TYPE_DEFAULT);
      Preconditions.checkArgument(
          !AuthProperties.AUTH_TYPE_SIGV4.equals(delegateAuthType),
          "Cannot delegate a SigV4 auth manager to another SigV4 auth manager");
      Map<String, String> newProperties = Maps.newHashMap(properties);
      newProperties.put(AuthProperties.AUTH_TYPE, delegateAuthType);
      newProperties.remove(SIGV4_ENABLED_LEGACY);
      delegate = loadAuthManager(name, newProperties);
    }

    String impl;
    switch (authType.toLowerCase(Locale.ROOT)) {
      case AuthProperties.AUTH_TYPE_NONE:
        impl = AuthProperties.AUTH_MANAGER_IMPL_NONE;
        break;
      case AuthProperties.AUTH_TYPE_BASIC:
        impl = AuthProperties.AUTH_MANAGER_IMPL_BASIC;
        break;
      case AuthProperties.AUTH_TYPE_SIGV4:
        impl = AuthProperties.AUTH_MANAGER_IMPL_SIGV4;
        break;
      case AuthProperties.AUTH_TYPE_GOOGLE:
        impl = AuthProperties.AUTH_MANAGER_IMPL_GOOGLE;
        break;
      case AuthProperties.AUTH_TYPE_OAUTH2:
        impl = AuthProperties.AUTH_MANAGER_IMPL_OAUTH2;
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
              .impl(impl, String.class, AuthManager.class) // with name and delegate
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize AuthManager implementation %s: %s", impl, e.getMessage()),
          e);
    }

    AuthManager authManager;
    try {
      authManager = ctor.newInstance(name, delegate);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize AuthManager, %s does not implement AuthManager", impl),
          e);
    }

    return authManager;
  }
}
