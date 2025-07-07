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

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/** A TLS configurer that supports custom keystore and truststore configuration. */
public class BasicTLSConfigurer implements TLSConfigurer {

  public static final String TLS_KEYSTORE_PATH = "rest.client.tls.keystore.path";
  public static final String TLS_KEYSTORE_PASSWORD = "rest.client.tls.keystore.password";
  public static final String TLS_KEYSTORE_TYPE = "rest.client.tls.keystore.type";
  public static final String TLS_TRUSTSTORE_PATH = "rest.client.tls.truststore.path";
  public static final String TLS_TRUSTSTORE_PASSWORD = "rest.client.tls.truststore.password";
  public static final String TLS_TRUSTSTORE_TYPE = "rest.client.tls.truststore.type";

  private static final String DEFAULT_KEYSTORE_TYPE = "JKS";
  private static final String DEFAULT_SSL_PROTOCOL = "TLS";

  private SSLContext sslContext;

  @Override
  public void initialize(Map<String, String> properties) {
    String keystorePath = properties.get(TLS_KEYSTORE_PATH);
    String keystorePassword = properties.get(TLS_KEYSTORE_PASSWORD);
    String keystoreType = properties.getOrDefault(TLS_KEYSTORE_TYPE, DEFAULT_KEYSTORE_TYPE);
    String truststorePath = properties.get(TLS_TRUSTSTORE_PATH);
    String truststorePassword = properties.get(TLS_TRUSTSTORE_PASSWORD);
    String truststoreType = properties.getOrDefault(TLS_TRUSTSTORE_TYPE, DEFAULT_KEYSTORE_TYPE);

    try {
      SSLContext context = SSLContext.getInstance(DEFAULT_SSL_PROTOCOL);

      KeyManager[] keyManagers = null;
      if (keystorePath != null) {
        char[] keystorePasswordChars =
            keystorePassword != null ? keystorePassword.toCharArray() : null;
        KeyStore keyStore = loadKeyStore(keystorePath, keystorePasswordChars, keystoreType);
        KeyManagerFactory keyManagerFactory =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePasswordChars);
        keyManagers = keyManagerFactory.getKeyManagers();
      }

      TrustManager[] trustManagers = null;
      if (truststorePath != null) {
        char[] truststorePasswordChars =
            truststorePassword != null ? truststorePassword.toCharArray() : null;
        KeyStore trustStore = loadKeyStore(truststorePath, truststorePasswordChars, truststoreType);
        TrustManagerFactory trustManagerFactory =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        trustManagers = trustManagerFactory.getTrustManagers();
      }

      context.init(keyManagers, trustManagers, null);

      this.sslContext = context;
    } catch (NoSuchAlgorithmException
        | KeyManagementException
        | KeyStoreException
        | UnrecoverableKeyException e) {
      throw new IllegalStateException("Failed to create SSL context", e);
    }
  }

  @Override
  public SSLContext sslContext() {
    try {
      return sslContext != null ? sslContext : SSLContext.getDefault();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("Failed to get SSL context", e);
    }
  }

  private KeyStore loadKeyStore(String path, char[] password, String type) {
    try (FileInputStream fis = new FileInputStream(path)) {
      KeyStore keyStore = KeyStore.getInstance(type);
      keyStore.load(fis, password);
      return keyStore;
    } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
      throw new IllegalStateException(
          String.format("Failed to load keystore from path: %s", path), e);
    }
  }
}
