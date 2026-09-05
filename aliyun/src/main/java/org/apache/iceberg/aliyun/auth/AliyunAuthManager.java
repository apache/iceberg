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
package org.apache.iceberg.aliyun.auth;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;

/**
 * An {@link AuthManager} that signs every request using a product-specific signing protocol. The
 * product is determined by the {@code aliyun.auth.signing-name} property.
 */
public final class AliyunAuthManager implements AuthManager {

  private static final String SIGNING_NAME_ODPS = "odps";
  private static final String SIGNER_IMPL_ODPS =
      "org.apache.iceberg.aliyun.odps.auth.OdpsRequestSigner";

  @SuppressWarnings("unused")
  private final String name;

  private Map<String, String> catalogProperties = Map.of();

  public AliyunAuthManager(String name) {
    this.name = name;
  }

  @Override
  public AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
    return createSession(properties);
  }

  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    this.catalogProperties = properties;
    return createSession(properties);
  }

  @Override
  public AuthSession contextualSession(SessionCatalog.SessionContext context, AuthSession parent) {
    Map<String, String> contextProps =
        RESTUtil.merge(
            Optional.ofNullable(context.properties()).orElseGet(Map::of),
            Optional.ofNullable(context.credentials()).orElseGet(Map::of));
    Map<String, String> merged = RESTUtil.merge(catalogProperties, contextProps);
    return createSession(merged);
  }

  @Override
  public AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    Map<String, String> tableProperties = RESTUtil.merge(catalogProperties, properties);
    return createSession(tableProperties);
  }

  private AuthSession createSession(Map<String, String> properties) {
    AliyunProperties aliyunProperties = new AliyunProperties(properties);
    AliyunRequestSigner signer = createSigner(aliyunProperties, properties);
    return new AliyunAuthSession(signer);
  }

  private static AliyunRequestSigner createSigner(
      AliyunProperties aliyunProperties, Map<String, String> properties) {
    String signingName = aliyunProperties.restSigningName();

    String impl;
    switch (signingName.toLowerCase(Locale.ROOT)) {
      case SIGNING_NAME_ODPS:
        impl = SIGNER_IMPL_ODPS;
        break;
      default:
        impl = signingName;
    }

    return loadSigner(impl, aliyunProperties, properties);
  }

  private static AliyunRequestSigner loadSigner(
      String impl, AliyunProperties aliyunProperties, Map<String, String> properties) {
    DynConstructors.Ctor<AliyunRequestSigner> ctor;
    try {
      ctor =
          DynConstructors.builder(AliyunRequestSigner.class)
              .loader(AliyunAuthManager.class.getClassLoader())
              .impl(impl, AliyunProperties.class, Map.class)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot initialize AliyunRequestSigner implementation %s: %s", impl, e.getMessage()),
          e);
    }

    try {
      return ctor.newInstance(aliyunProperties, properties);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot initialize signer, %s does not implement AliyunRequestSigner", impl),
          e);
    }
  }

  @Override
  public void close() {}
}
