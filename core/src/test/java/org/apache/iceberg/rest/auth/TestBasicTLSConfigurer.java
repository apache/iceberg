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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.FileOutputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestBasicTLSConfigurer {

  @TempDir Path tempDir;

  @Test
  public void testBasicTLSConfigurerInitialization() {
    BasicTLSConfigurer configurer = new BasicTLSConfigurer();
    configurer.initialize(ImmutableMap.of());

    // Should return default SSL context when no custom configuration is provided
    SSLContext sslContext = configurer.sslContext();
    assertThat(sslContext).isNotNull();
  }

  @Test
  public void testBasicTLSConfigurerWithBothKeystoreAndTruststore() throws Exception {
    Path keystorePath = createTempKeyStore("keystore.jks", "keypass");
    Path truststorePath = createTempKeyStore("truststore.jks", "trustpass");

    BasicTLSConfigurer configurer = new BasicTLSConfigurer();
    Map<String, String> properties =
        ImmutableMap.of(
            BasicTLSConfigurer.TLS_KEYSTORE_PATH,
            keystorePath.toString(),
            BasicTLSConfigurer.TLS_KEYSTORE_PASSWORD,
            "keypass",
            BasicTLSConfigurer.TLS_TRUSTSTORE_PATH,
            truststorePath.toString(),
            BasicTLSConfigurer.TLS_TRUSTSTORE_PASSWORD,
            "trustpass");

    configurer.initialize(properties);

    SSLContext sslContext = configurer.sslContext();
    assertThat(sslContext).isNotNull();
    assertThat(sslContext.getProtocol()).isEqualTo("TLS");
  }

  @Test
  public void testBasicTLSConfigurerWithInvalidKeystorePath() {
    BasicTLSConfigurer configurer = new BasicTLSConfigurer();
    Map<String, String> properties =
        ImmutableMap.of(
            BasicTLSConfigurer.TLS_KEYSTORE_PATH, "/nonexistent/path/keystore.jks",
            BasicTLSConfigurer.TLS_KEYSTORE_PASSWORD, "password123");

    assertThatThrownBy(() -> configurer.initialize(properties))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to load keystore from path");
  }

  /** Creates a temporary keystore file for testing purposes. */
  private Path createTempKeyStore(String filename, String password) throws Exception {
    Path keystorePath = tempDir.resolve(filename);

    // Create an empty keystore
    KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, password != null ? password.toCharArray() : null);

    // Write the keystore to file
    try (FileOutputStream fos = new FileOutputStream(keystorePath.toFile())) {
      keyStore.store(fos, password != null ? password.toCharArray() : null);
    }

    return keystorePath;
  }
}
