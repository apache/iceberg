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
package org.apache.iceberg.rest.auth.oauth2.test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import org.apache.commons.io.FileUtils;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

/**
 * Generates test certificates and keystores at runtime. All materials are generated lazily (once
 * per JVM) and stored in a temp directory.
 */
public final class TestCertificates {

  private static final class Holder {
    private static final TestCertificates INSTANCE = new TestCertificates();
  }

  public static TestCertificates instance() {
    return Holder.INSTANCE;
  }

  private static final String KEYSTORE_PASSWORD = "s3cr3t";

  private final Path mockServerP12;

  private TestCertificates() {
    try {
      Path baseDir = Files.createTempDirectory("iceberg-test-certs");
      Runtime.getRuntime()
          .addShutdownHook(new Thread(() -> FileUtils.deleteQuietly(baseDir.toFile())));

      // Generate mock server PKCS#12 keystore from MockServer's CA materials
      mockServerP12 = baseDir.resolve("mockserver.p12");
      writeMockServerKeyStore(mockServerP12);

    } catch (Exception e) {
      throw new RuntimeException("Failed to generate test certificates", e);
    }
  }

  /** PKCS#12 keystore containing MockServer's CA certificate and private key (password: s3cr3t). */
  public Path mockServerKeyStore() {
    return mockServerP12;
  }

  /** The keystore password: {@code s3cr3t}. */
  public String keyStorePassword() {
    return KEYSTORE_PASSWORD;
  }

  private static void writeKeyStore(PrivateKey privateKey, X509Certificate certificate, Path path)
      throws Exception {
    KeyStore ks = KeyStore.getInstance("PKCS12");
    ks.load(null, null);
    ks.setKeyEntry(
        "1", privateKey, KEYSTORE_PASSWORD.toCharArray(), new Certificate[] {certificate});
    try (OutputStream os = Files.newOutputStream(path)) {
      ks.store(os, KEYSTORE_PASSWORD.toCharArray());
    }
  }

  private static void writeMockServerKeyStore(Path path) throws Exception {
    X509Certificate cert;
    PrivateKey key;
    try (InputStream certStream =
        TestCertificates.class.getResourceAsStream(
            "/org/mockserver/socket/CertificateAuthorityCertificate.pem")) {
      if (certStream == null) {
        throw new IllegalStateException(
            "MockServer CA certificate not found on classpath. "
                + "Ensure org.mock-server:mockserver-netty is a dependency.");
      }
      cert = readCertificateFromPem(certStream);
    }
    try (InputStream keyStream =
        TestCertificates.class.getResourceAsStream(
            "/org/mockserver/socket/CertificateAuthorityPrivateKey.pem")) {
      if (keyStream == null) {
        throw new IllegalStateException(
            "MockServer CA private key not found on classpath. "
                + "Ensure org.mock-server:mockserver-netty is a dependency.");
      }
      key = readPrivateKeyFromPem(keyStream);
    }
    writeKeyStore(key, cert, path);
  }

  private static X509Certificate readCertificateFromPem(InputStream is) throws Exception {
    return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(is);
  }

  private static PrivateKey readPrivateKeyFromPem(InputStream is) throws Exception {
    try (Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        PEMParser parser = new PEMParser(reader)) {
      Object pemObject = parser.readObject();
      JcaPEMKeyConverter converter =
          new JcaPEMKeyConverter().setProvider(new BouncyCastleProvider());
      if (pemObject instanceof PEMKeyPair) {
        return converter.getPrivateKey(((PEMKeyPair) pemObject).getPrivateKeyInfo());
      } else if (pemObject instanceof PrivateKeyInfo) {
        return converter.getPrivateKey((PrivateKeyInfo) pemObject);
      }
      throw new IllegalStateException("Unexpected PEM object: " + pemObject.getClass());
    }
  }
}
