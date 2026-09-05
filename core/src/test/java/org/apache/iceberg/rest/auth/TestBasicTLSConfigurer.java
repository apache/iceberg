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
import java.math.BigInteger;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
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
  public void testBasicTLSConfigurerWithCustomProtocol() throws Exception {
    Path keystorePath = createTempKeyStore("keystore.jks", "keypass");

    BasicTLSConfigurer configurer = new BasicTLSConfigurer();
    Map<String, String> properties =
        ImmutableMap.of(
            BasicTLSConfigurer.TLS_KEYSTORE_PATH, keystorePath.toString(),
            BasicTLSConfigurer.TLS_KEYSTORE_PASSWORD, "keypass",
            BasicTLSConfigurer.TLS_PROTOCOL, "TLSv1.3");

    configurer.initialize(properties);

    assertThat(configurer.sslContext().getProtocol()).isEqualTo("TLSv1.3");
    // the context algorithm alone would still leave TLSv1.2 enabled, so the protocol must also be
    // pinned through supportedProtocols
    assertThat(configurer.sslContext().getDefaultSSLParameters().getProtocols())
        .contains("TLSv1.2");
    assertThat(configurer.supportedProtocols()).containsExactly("TLSv1.3");
  }

  @Test
  public void testBasicTLSConfigurerWithoutProtocolOrCipherSuites() {
    BasicTLSConfigurer configurer = new BasicTLSConfigurer();
    configurer.initialize(ImmutableMap.of());

    // null defers to the JSSE defaults
    assertThat(configurer.supportedProtocols()).isNull();
    assertThat(configurer.supportedCipherSuites()).isNull();
  }

  @Test
  public void testBasicTLSConfigurerWithCipherSuites() {
    BasicTLSConfigurer configurer = new BasicTLSConfigurer();
    configurer.initialize(
        ImmutableMap.of(
            BasicTLSConfigurer.TLS_CIPHER_SUITES,
            "TLS_AES_256_GCM_SHA384, TLS_AES_128_GCM_SHA256"));

    assertThat(configurer.supportedCipherSuites())
        .containsExactly("TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256");
  }

  @Test
  public void testBasicTLSConfigurerWithBlankCipherSuites() {
    BasicTLSConfigurer configurer = new BasicTLSConfigurer();

    assertThatThrownBy(
            () ->
                configurer.initialize(ImmutableMap.of(BasicTLSConfigurer.TLS_CIPHER_SUITES, " ,")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be blank");
  }

  @Test
  public void testBasicTLSConfigurerWithUnsupportedProtocol() throws Exception {
    Path keystorePath = createTempKeyStore("keystore.jks", "keypass");

    BasicTLSConfigurer configurer = new BasicTLSConfigurer();
    Map<String, String> properties =
        ImmutableMap.of(
            BasicTLSConfigurer.TLS_KEYSTORE_PATH, keystorePath.toString(),
            BasicTLSConfigurer.TLS_KEYSTORE_PASSWORD, "keypass",
            BasicTLSConfigurer.TLS_PROTOCOL, "NotATLSProtocol");

    assertThatThrownBy(() -> configurer.initialize(properties))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to create SSL context");
  }

  @Test
  public void testBasicTLSConfigurerWithDistinctKeyPassword() throws Exception {
    Path keystorePath = createKeyStoreWithKeyEntry("keystore.jks", "storepass", "keypass");

    BasicTLSConfigurer configurer = new BasicTLSConfigurer();
    configurer.initialize(
        ImmutableMap.of(
            BasicTLSConfigurer.TLS_KEYSTORE_PATH, keystorePath.toString(),
            BasicTLSConfigurer.TLS_KEYSTORE_PASSWORD, "storepass",
            BasicTLSConfigurer.TLS_KEYSTORE_KEY_PASSWORD, "keypass"));

    assertThat(configurer.sslContext()).isNotNull();
  }

  @Test
  public void testBasicTLSConfigurerWithDistinctKeyPasswordMissing() throws Exception {
    Path keystorePath = createKeyStoreWithKeyEntry("keystore.jks", "storepass", "keypass");

    BasicTLSConfigurer configurer = new BasicTLSConfigurer();
    Map<String, String> properties =
        ImmutableMap.of(
            BasicTLSConfigurer.TLS_KEYSTORE_PATH,
            keystorePath.toString(),
            BasicTLSConfigurer.TLS_KEYSTORE_PASSWORD,
            "storepass");

    // without the key password the keystore password is used, which cannot unlock the private key
    assertThatThrownBy(() -> configurer.initialize(properties))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to create SSL context")
        .hasRootCauseInstanceOf(UnrecoverableKeyException.class);
  }

  @Test
  public void testBasicTLSConfigurerKeyPasswordDefaultsToKeystorePassword() throws Exception {
    Path keystorePath = createKeyStoreWithKeyEntry("keystore.jks", "samepass", "samepass");

    BasicTLSConfigurer configurer = new BasicTLSConfigurer();
    configurer.initialize(
        ImmutableMap.of(
            BasicTLSConfigurer.TLS_KEYSTORE_PATH,
            keystorePath.toString(),
            BasicTLSConfigurer.TLS_KEYSTORE_PASSWORD,
            "samepass"));

    assertThat(configurer.sslContext()).isNotNull();
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
        .hasMessageContaining("Keystore file does not exist");
  }

  /**
   * Creates a keystore holding a self-signed key entry, where the private key is protected by a
   * password that may differ from the keystore password.
   */
  private Path createKeyStoreWithKeyEntry(String filename, String storePassword, String keyPassword)
      throws Exception {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    KeyPair keyPair = keyPairGenerator.generateKeyPair();

    X500Name subject = new X500Name("CN=iceberg-test");
    Instant now = Instant.now();
    X509Certificate certificate =
        new JcaX509CertificateConverter()
            .getCertificate(
                new JcaX509v3CertificateBuilder(
                        subject,
                        BigInteger.ONE,
                        Date.from(now),
                        Date.from(now.plus(1, ChronoUnit.DAYS)),
                        subject,
                        keyPair.getPublic())
                    .build(
                        new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate())));

    KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, storePassword.toCharArray());
    keyStore.setKeyEntry(
        "client", keyPair.getPrivate(), keyPassword.toCharArray(), new Certificate[] {certificate});

    Path keystorePath = tempDir.resolve(filename);
    try (FileOutputStream fos = new FileOutputStream(keystorePath.toFile())) {
      keyStore.store(fos, storePassword.toCharArray());
    }

    return keystorePath;
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
