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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.base.Strings;

/** A TLS configurer that supports custom keystore and truststore configuration. */
public class BasicTLSConfigurer implements TLSConfigurer {

  public static final String TLS_KEYSTORE_PATH = "rest.client.tls.keystore.path";
  public static final String TLS_KEYSTORE_PASSWORD = "rest.client.tls.keystore.password";
  public static final String TLS_KEYSTORE_TYPE = "rest.client.tls.keystore.type";
  public static final String TLS_TRUSTSTORE_PATH = "rest.client.tls.truststore.path";
  public static final String TLS_TRUSTSTORE_PASSWORD = "rest.client.tls.truststore.password";
  public static final String TLS_TRUSTSTORE_TYPE = "rest.client.tls.truststore.type";
  public static final String TLS_PROTOCOL = "rest.client.tls.protocol";
  public static final String TLS_CIPHER_SUITES = "rest.client.tls.cipher-suites";

  private static final String DEFAULT_KEYSTORE_TYPE = "JKS";
  private static final String DEFAULT_TLS_PROTOCOL = "TLS";
  private static final Splitter CIPHER_SUITE_SPLITTER =
      Splitter.on(',').trimResults().omitEmptyStrings();

  private SSLContext sslContext;
  private String[] supportedProtocols;
  private String[] supportedCipherSuites;

  @Override
  public void initialize(Map<String, String> properties) {
    String keystorePath = properties.get(TLS_KEYSTORE_PATH);
    String keystorePassword = properties.get(TLS_KEYSTORE_PASSWORD);
    String keystoreType = properties.getOrDefault(TLS_KEYSTORE_TYPE, DEFAULT_KEYSTORE_TYPE);
    String truststorePath = properties.get(TLS_TRUSTSTORE_PATH);
    String truststorePassword = properties.get(TLS_TRUSTSTORE_PASSWORD);
    String truststoreType = properties.getOrDefault(TLS_TRUSTSTORE_TYPE, DEFAULT_KEYSTORE_TYPE);
    String protocol = properties.get(TLS_PROTOCOL);
    String cipherSuites = properties.get(TLS_CIPHER_SUITES);

    // An explicitly configured protocol is also pinned as the only enabled protocol. Passing it to
    // SSLContext.getInstance alone is not enough: JSSE treats the context algorithm as a minimum,
    // so SSLContext.getInstance("TLSv1.3") still leaves TLSv1.2 enabled on the socket.
    this.supportedProtocols =
        Strings.isNullOrEmpty(protocol) ? null : new String[] {protocol.trim()};
    this.supportedCipherSuites = parseCipherSuites(cipherSuites);

    try {
      KeyManager[] keyManagers = null;
      if (!Strings.isNullOrEmpty(keystorePath)) {
        char[] keystorePasswordChars =
            keystorePassword != null ? keystorePassword.toCharArray() : null;
        KeyStore keyStore = loadKeyStore(keystorePath, keystorePasswordChars, keystoreType);
        KeyManagerFactory keyManagerFactory =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePasswordChars);
        keyManagers = keyManagerFactory.getKeyManagers();
      }

      TrustManager[] trustManagers = null;
      if (!Strings.isNullOrEmpty(truststorePath)) {
        char[] truststorePasswordChars =
            truststorePassword != null ? truststorePassword.toCharArray() : null;
        KeyStore trustStore = loadKeyStore(truststorePath, truststorePasswordChars, truststoreType);
        TrustManagerFactory trustManagerFactory =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        trustManagers = trustManagerFactory.getTrustManagers();
      }

      if (keyManagers == null && trustManagers == null) {
        this.sslContext = SSLContext.getDefault();
      } else {
        SSLContext context =
            SSLContext.getInstance(
                Strings.isNullOrEmpty(protocol) ? DEFAULT_TLS_PROTOCOL : protocol.trim());
        context.init(keyManagers, trustManagers, null);
        this.sslContext = context;
      }
    } catch (NoSuchAlgorithmException
        | KeyManagementException
        | KeyStoreException
        | UnrecoverableKeyException e) {
      throw new IllegalStateException("Failed to create SSL context", e);
    }
  }

  @Override
  public SSLContext sslContext() {
    Preconditions.checkState(sslContext != null, "TLSConfigurer must be initialized before use");
    return sslContext;
  }

  /** Returns the configured protocol as the only enabled one, or null to use the JSSE defaults. */
  @Override
  public String[] supportedProtocols() {
    return supportedProtocols == null ? null : supportedProtocols.clone();
  }

  /** Returns the configured cipher suites, or null to use the JSSE defaults. */
  @Override
  public String[] supportedCipherSuites() {
    return supportedCipherSuites == null ? null : supportedCipherSuites.clone();
  }

  private static String[] parseCipherSuites(String cipherSuites) {
    if (Strings.isNullOrEmpty(cipherSuites)) {
      return null;
    }

    List<String> suites = CIPHER_SUITE_SPLITTER.splitToList(cipherSuites);
    Preconditions.checkArgument(
        !suites.isEmpty(), "Invalid cipher suites: %s must not be blank", TLS_CIPHER_SUITES);
    return suites.toArray(new String[0]);
  }

  private KeyStore loadKeyStore(String path, char[] password, String type) {
    if (!Files.exists(Paths.get(path))) {
      throw new IllegalStateException(String.format("Keystore file does not exist: %s", path));
    }
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
