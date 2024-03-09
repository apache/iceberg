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
package org.apache.iceberg.encryption;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Enumeration;
import java.util.Map;
import javax.crypto.SecretKey;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * KMS client demo class, based on the Java KeyStore API that reads keys from standard PKCS12
 * keystore files. Not for use in production.
 */
public class KeyStoreKmsClient extends MemoryMockKMS {

  // Path to keystore file. Preferably kept in volatile storage, such as ramdisk. Don't store with
  // data.
  public static final String KEYSTORE_FILE_PATH_PROP = "kms.client.keystore.path";

  // Credentials (such as keystore password) must never be kept in a persistent storage.
  // In this class, the password is passed as a system environment variable.
  public static final String KEYSTORE_PASSWORD_ENV_VAR = "KEYSTORE_PASSWORD";

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    // keytool keeps key names in lower case
    return super.wrapKey(key, wrappingKeyId.toLowerCase());
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    // keytool keeps key names in lower case
    return super.unwrapKey(wrappedKey, wrappingKeyId.toLowerCase());
  }

  @Override
  public void initialize(Map<String, String> properties) {
    String keystorePath = properties.get(KEYSTORE_FILE_PATH_PROP);
    Preconditions.checkNotNull(
        keystorePath, KEYSTORE_FILE_PATH_PROP + " must be set in hadoop or table " + "properties");

    String keystorePassword = System.getenv(KEYSTORE_PASSWORD_ENV_VAR);
    Preconditions.checkNotNull(
        keystorePassword, KEYSTORE_PASSWORD_ENV_VAR + " environment variable " + "must be set");

    KeyStore keyStore;
    try {
      keyStore = KeyStore.getInstance("PKCS12");
    } catch (KeyStoreException e) {
      throw new RuntimeException("Failed to init keystore", e);
    }

    char[] pwdArray = keystorePassword.toCharArray();

    FileInputStream fis;
    try {
      fis = new FileInputStream(keystorePath);
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Failed to find keystore file " + keystorePath, e);
    }

    try {
      keyStore.load(fis, pwdArray);
    } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
      throw new RuntimeException("Failed to load keystore file " + keystorePath, e);
    }

    Enumeration<String> keyAliases;
    try {
      keyAliases = keyStore.aliases();
    } catch (KeyStoreException e) {
      throw new RuntimeException("Failed to get key aliases in keystore file " + keystorePath, e);
    }

    masterKeys = Maps.newHashMap();
    while (keyAliases.hasMoreElements()) {
      String keyAlias = keyAliases.nextElement();
      SecretKey secretKey;
      try {
        secretKey = (SecretKey) keyStore.getKey(keyAlias, pwdArray);
      } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
        throw new RuntimeException("Failed to get key " + keyAlias, e);
      }

      masterKeys.put(keyAlias, secretKey.getEncoded());
    }

    if (masterKeys.isEmpty()) {
      throw new RuntimeException("No keys found in " + keystorePath);
    }
  }
}
